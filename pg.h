#ifndef PG_H
#define PG_H

#include "pg_internal.h"

/**
 * @file pg.h
 * @brief Process Group (PG) Library - Public API for RDMA-based Collective Communication
 *
 * @section overview Overview
 * 
 * The Process Group (PG) library provides high-performance collective communication
 * operations over InfiniBand RDMA networks. It implements a ring-based topology for
 * optimal bandwidth utilization and low latency communication between distributed
 * processes.
 *
 * @section abstraction Process Group Abstraction
 * 
 * A Process Group represents a collection of processes arranged in a logical ring
 * topology, where each process has exactly two neighbors (left and right). This
 * abstraction enables efficient collective operations by leveraging the ring's
 * natural data flow patterns.
 *
 * Key characteristics:
 * - Each process has a unique rank (0 to N-1) within the group
 * - Ring topology: rank 0 connects to rank N-1, forming a closed loop
 * - RDMA queue pairs connect each process to its immediate neighbors
 * - Collective operations use pipelined communication for optimal performance
 *
 * @section lifecycle Process Group Lifecycle
 * 
 * The typical lifecycle of a process group follows this pattern:
 * 
 * 1. **Initialization**: `pg_initialize()` creates the process group
 *    - Parses hostname list and determines process rank
 *    - Establishes TCP connections for bootstrap coordination
 *    - Creates RDMA resources (queue pairs, memory regions, buffers)
 *    - Exchanges RDMA connection information via TCP
 *    - Transitions RDMA queue pairs to Ready-to-Send (RTS) state
 * 
 * 2. **Collective Operations**: Perform communication operations
 *    - `pg_all_reduce()`: Reduce data across all processes, broadcast result
 *    - `pg_reduce_scatter()`: Reduce data, distribute chunks to processes
 *    - `pg_all_gather()`: Gather data from all processes to all processes
 * 
 * 3. **Cleanup**: `pg_cleanup()` releases all resources
 *    - Destroys RDMA queue pairs and memory regions
 *    - Frees communication buffers
 *    - Cleans up RDMA context and device resources
 *
 * @section bootstrap RDMA Bootstrap Process
 * 
 * The RDMA bootstrap process establishes high-performance connections:
 * 
 * 1. **TCP Coordination Phase**:
 *    - Each process creates a TCP server socket on a known port
 *    - Processes connect to their ring neighbors via TCP
 *    - Exchange RDMA queue pair information (QPN, LID, GID)
 * 
 * 2. **RDMA Connection Phase**:
 *    - Create RDMA queue pairs in INIT state
 *    - Transition queue pairs to Ready-to-Receive (RTR) using neighbor info
 *    - Transition queue pairs to Ready-to-Send (RTS) for active communication
 *    - Close TCP connections (no longer needed)
 * 
 * 3. **Communication Phase**:
 *    - RDMA send/receive operations for collective algorithms
 *    - High-bandwidth, low-latency data transfer
 *    - Hardware-offloaded network operations
 *
 * @section features Key Features
 * 
 * - **High Performance**: RDMA-based communication with hardware offload
 * - **Scalable Algorithms**: Ring-based collectives with O(N) complexity
 * - **Multiple Data Types**: Support for integers and double-precision floats
 * - **Flexible Operations**: Sum and product reduction operations
 * - **Clean API**: Opaque handles hide implementation complexity
 * - **Error Handling**: Comprehensive error checking with detailed diagnostics
 *
 * @section thread_safety Thread Safety
 * 
 * This library is **NOT thread-safe**. External synchronization is required
 * if used in multi-threaded environments. Each process group handle should
 * be used by only one thread at a time.
 *
 * @section error_handling Error Handling
 * 
 * All public functions return:
 * - `PG_SUCCESS` (0) on successful completion
 * - `PG_ERROR` (-1) on failure
 * 
 * Detailed error messages are printed to stderr for debugging purposes.
 * Applications should check return values and handle errors appropriately.
 *
 * @author Process Group Library Development Team
 * @version 2.0
 * @date 2024
 */

/*
 * =============================================================================
 * Data Types and Constants
 * =============================================================================
 */

/**
 * @brief Opaque handle for process group instances
 * 
 * This handle encapsulates all internal state and resources associated with
 * a process group. Users should treat this as an opaque pointer and never
 * attempt to access its contents directly.
 */
typedef void* pg_handle_t;

/*
 * =============================================================================
 * Process Group Lifecycle Management
 * =============================================================================
 */

/**
 * @brief Initialize a process group from a space-separated list of hostnames
 * 
 * Creates and initializes a process group by parsing the hostname list,
 * setting up RDMA resources, establishing connections, and preparing for
 * collective communication operations.
 * 
 * This function performs the complete bootstrap sequence:
 * 1. Parses hostname list to determine group size and process rank
 * 2. Initializes RDMA context and creates queue pairs
 * 3. Allocates and registers communication buffers
 * 4. Establishes TCP connections for bootstrap coordination
 * 5. Exchanges RDMA connection information with ring neighbors
 * 6. Transitions RDMA queue pairs to ready-to-send state
 * 
 * @param server_list_string Space-separated string of hostnames (e.g., "host1 host2 host3")
 * @param process_group_handle Output parameter for the initialized process group handle
 * @return PG_SUCCESS (0) on successful initialization, PG_ERROR (-1) on failure
 * 
 * @note The hostname list must contain at least one hostname. The calling process
 *       must be able to resolve its own hostname in the list to determine its rank.
 * 
 * @warning This function may block for several seconds during the bootstrap process
 *          as it waits for network connections and RDMA state transitions.
 */
int pg_initialize(const char *server_list_string, pg_handle_t *process_group_handle);

/**
 * @brief Clean up and destroy a process group
 * 
 * Releases all resources associated with a process group, including RDMA
 * resources, communication buffers, and internal data structures.
 * 
 * This function performs complete cleanup:
 * 1. Destroys RDMA queue pairs and memory regions
 * 2. Frees communication buffers
 * 3. Cleans up RDMA context and device resources
 * 4. Releases hostname list memory
 * 5. Frees the process group structure
 * 
 * @param process_group_handle Handle to the process group to clean up
 * @return PG_SUCCESS (0) on successful cleanup, PG_ERROR (-1) on failure
 * 
 * @note After calling this function, the handle becomes invalid and must not be used.
 *       It is safe to call this function with a NULL handle (no-op).
 * 
 * @warning Calling collective operations on a cleaned-up handle results in undefined behavior.
 */
int pg_cleanup(pg_handle_t process_group_handle);

/*
 * =============================================================================
 * Collective Communication Operations
 * =============================================================================
 */

/**
 * @brief Perform an all-reduce collective operation
 * 
 * All-reduce combines data from all processes using the specified reduction
 * operation and distributes the complete result to all processes. Each process
 * receives the same final result containing the reduction of all input data.
 * 
 * Algorithm: Two-phase approach for optimal performance
 * 1. **Reduce-scatter phase**: Distributes partial reductions across processes
 * 2. **All-gather phase**: Collects and broadcasts complete results to all processes
 * 
 * @param process_group_handle Handle to the initialized process group
 * @param send_buffer Input data buffer (must be same size on all processes)
 * @param receive_buffer Output buffer for the reduced result (can be same as send_buffer)
 * @param element_count Number of elements in the buffers (must be divisible by group size)
 * @param data_type Type of data elements (PG_DATATYPE_INT or PG_DATATYPE_DOUBLE)
 * @param reduction_op Reduction operation (PG_OPERATION_SUM or PG_OPERATION_PRODUCT)
 * @return PG_SUCCESS (0) on successful completion, PG_ERROR (-1) on failure
 * 
 * @note The element_count must be the same on all processes and should be
 *       divisible by the process group size for optimal performance.
 * 
 * @warning Send and receive buffers must not overlap unless they are identical.
 *          All processes must call this function with the same parameters.
 */
int pg_all_reduce(pg_handle_t process_group_handle,
                 void *send_buffer,
                 void *receive_buffer,
                 int element_count,
                 pg_datatype_t data_type,
                 pg_operation_t reduction_op);

/**
 * @brief Perform a reduce-scatter collective operation
 * 
 * Reduce-scatter combines data from all processes using the specified reduction
 * operation and distributes the partial results to each process. Each process
 * receives a portion of the final result.
 * 
 * Algorithm: Ring-based reduce-scatter with (N-1) communication steps
 * - Each process starts with its own data
 * - Data circulates around the ring, performing partial reductions
 * - Final result: each process has its portion of the reduced data
 * 
 * @param process_group_handle Handle to the initialized process group
 * @param send_buffer Input data buffer (must be same size on all processes)
 * @param receive_buffer Output buffer for this process's portion of result
 * @param element_count Total number of elements in the final result (across all processes)
 * @param data_type Type of data elements (PG_DATATYPE_INT or PG_DATATYPE_DOUBLE)
 * @param reduction_op Reduction operation (PG_OPERATION_SUM or PG_OPERATION_PRODUCT)
 * @return PG_SUCCESS (0) on successful completion, PG_ERROR (-1) on failure
 * 
 * @note The receive_buffer will contain this process's portion of the final result.
 *       The element_count must be the same on all processes.
 * 
 * @warning All processes must call this function with the same parameters.
 */
int pg_reduce_scatter(pg_handle_t process_group_handle,
                     void *send_buffer,
                     void *receive_buffer,
                     int element_count,
                     pg_datatype_t data_type,
                     pg_operation_t reduction_op);

/**
 * @brief Perform an all-gather collective operation
 * 
 * All-gather collects data from all processes and distributes the complete
 * concatenated result to all processes. No reduction is performed - this is
 * purely a data gathering and distribution operation.
 * 
 * Algorithm: Ring-based all-gather with (N-1) communication steps
 * - Each process starts with its own data in the correct position
 * - Data circulates around the ring, filling in missing pieces
 * - Final result: all processes have identical copies of the complete dataset
 * 
 * @param process_group_handle Handle to the initialized process group
 * @param send_buffer Input data buffer (chunk size should be element_count/group_size)
 * @param receive_buffer Output buffer for gathered data from all processes (size = element_count)
 * @param element_count Total number of elements in the final result (across all processes)
 * @param data_type Type of data elements (PG_DATATYPE_INT or PG_DATATYPE_DOUBLE)
 * @param unused_operation Unused parameter (maintained for API consistency)
 * @return PG_SUCCESS (0) on successful completion, PG_ERROR (-1) on failure
 * 
 * @note The receive_buffer will contain data from all processes:
 *       - Elements [0 : chunk_size) from process 0
 *       - Elements [chunk_size : 2*chunk_size) from process 1
 *       - And so on...
 * 
 * @warning The receive_buffer must be large enough to hold element_count elements.
 *          All processes must call this function with the same element_count.
 */
int pg_all_gather(pg_handle_t process_group_handle,
                 void *send_buffer,
                 void *receive_buffer,
                 int element_count,
                 pg_datatype_t data_type, 
                 pg_operation_t unused_operation);

/**
 * @section usage_example Complete Usage Example
 * 
 * Below is a comprehensive example showing how to use the Process Group library
 * for collective communication. This example demonstrates the complete workflow
 * from initialization to cleanup.
 * 
 * @code{.c}
 * #include "pg.h"
 * #include "constants.h"
 * #include <stdio.h>
 * #include <stdlib.h>
 * #include <string.h>
 * 
 * int main(int argc, char *argv[]) {
 *     // Step 1: Environment setup (optional - uses defaults if not set)
 *     // These environment variables can be set to tune performance:
 *     // export PG_EAGER_MAX=8192        # Eager protocol threshold
 *     // export PG_CHUNK_BYTES=4096      # Chunk size for pipelining
 *     // export PG_INFLIGHT=4            # Maximum inflight operations
 *     // export PG_PORT=12345            # TCP port for bootstrap
 * 
 *     if (argc < 2) {
 *         fprintf(stderr, "Usage: %s <hostname_list>\n", argv[0]);
 *         fprintf(stderr, "Example: %s \"node1 node2 node3 node4\"\n", argv[0]);
 *         return 1;
 *     }
 * 
 *     // Step 2: Initialize the process group
 *     pg_handle_t process_group;
 *     printf("Initializing process group with hosts: %s\n", argv[1]);
 *     
 *     if (pg_initialize(argv[1], &process_group) != PG_SUCCESS) {
 *         fprintf(stderr, "Failed to initialize process group\n");
 *         return 1;
 *     }
 *     
 *     printf("Process group initialized successfully\n");
 * 
 *     // Step 3: Prepare data for collective operation
 *     const int data_size = 1024;  // Number of double elements
 *     double *send_data = malloc(data_size * sizeof(double));
 *     double *result_data = malloc(data_size * sizeof(double));
 *     
 *     if (!send_data || !result_data) {
 *         fprintf(stderr, "Failed to allocate memory\n");
 *         pg_cleanup(process_group);
 *         return 1;
 *     }
 * 
 *     // Initialize data (each process contributes different values)
 *     for (int i = 0; i < data_size; i++) {
 *         send_data[i] = (double)(i + 1);  // Simple test pattern
 *     }
 * 
 *     // Step 4: Perform all-reduce collective operation
 *     printf("Performing all-reduce operation (sum of doubles)...\n");
 *     
 *     if (pg_all_reduce(process_group,
 *                      send_data,           // Input buffer
 *                      result_data,         // Output buffer
 *                      data_size,           // Number of elements
 *                      PG_DATATYPE_DOUBLE,  // Data type
 *                      PG_OPERATION_SUM)    // Reduction operation
 *         != PG_SUCCESS) {
 *         fprintf(stderr, "All-reduce operation failed\n");
 *         free(send_data);
 *         free(result_data);
 *         pg_cleanup(process_group);
 *         return 1;
 *     }
 * 
 *     printf("All-reduce completed successfully\n");
 * 
 *     // Step 5: Verify results (optional)
 *     printf("First 10 result elements: ");
 *     for (int i = 0; i < 10 && i < data_size; i++) {
 *         printf("%.2f ", result_data[i]);
 *     }
 *     printf("\n");
 * 
 *     // Step 6: Additional collective operations (optional)
 *     // Example: Reduce-scatter operation
 *     double *scatter_result = malloc((data_size / 4) * sizeof(double));  // Assuming 4 processes
 *     if (scatter_result) {
 *         if (pg_reduce_scatter(process_group,
 *                              send_data,
 *                              scatter_result,
 *                              data_size,
 *                              PG_DATATYPE_DOUBLE,
 *                              PG_OPERATION_SUM) == PG_SUCCESS) {
 *             printf("Reduce-scatter completed successfully\n");
 *         }
 *         free(scatter_result);
 *     }
 * 
 *     // Step 7: Clean up resources
 *     printf("Cleaning up resources...\n");
 *     
 *     free(send_data);
 *     free(result_data);
 *     
 *     if (pg_cleanup(process_group) != PG_SUCCESS) {
 *         fprintf(stderr, "Warning: Cleanup encountered errors\n");
 *     } else {
 *         printf("Cleanup completed successfully\n");
 *     }
 * 
 *     return 0;
 * }
 * 
 * // Compilation example:
 * // gcc -o pg_example example.c pg.c RDMA_api.c pg_net.c -libverbs
 * 
 * // Execution example (on each participating node):
 * // ./pg_example "node1 node2 node3 node4"
 * @endcode
 * 
 * @section performance_tuning Performance Tuning
 * 
 * The library behavior can be tuned using environment variables:
 * 
 * - **PG_EAGER_MAX**: Maximum message size for eager protocol (default: 8192 bytes)
 * - **PG_CHUNK_BYTES**: Chunk size for pipelined operations (default: 4096 bytes)
 * - **PG_INFLIGHT**: Maximum number of inflight RDMA operations (default: 4)
 * - **PG_PORT**: TCP port for bootstrap coordination (default: 12345)
 * 
 * @section troubleshooting Troubleshooting
 * 
 * Common issues and solutions:
 * 
 * 1. **"Failed to initialize process group"**:
 *    - Verify all hostnames are resolvable
 *    - Check that InfiniBand devices are available
 *    - Ensure TCP port is not blocked by firewall
 * 
 * 2. **"RDMA operation failed"**:
 *    - Verify InfiniBand fabric connectivity
 *    - Check that all processes are using compatible RDMA devices
 *    - Ensure sufficient memory is available for RDMA buffers
 * 
 * 3. **Performance issues**:
 *    - Tune PG_CHUNK_BYTES for your message sizes
 *    - Adjust PG_INFLIGHT based on network characteristics
 *    - Consider CPU affinity and NUMA topology
 */

#endif /* PG_H */
