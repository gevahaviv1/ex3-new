#ifndef PG_INTERNAL_H
#define PG_INTERNAL_H

#include "RDMA_api.h"
#include "constants.h"

struct pg_handle_internal;
#ifndef PG_HANDLE_INTERNAL_FORWARD_DECL
#define PG_HANDLE_INTERNAL_FORWARD_DECL
typedef struct pg_handle_internal pg_handle_internal_t;
#endif

/**
 * Process Group Internal Implementation Details
 *
 * This header contains private structures and helper functions used
 * internally by the process group implementation. These details are
 * hidden from the public API to maintain clean separation between
 * interface and implementation.
 */

/**
 * Data Types for Collective Operations
 *
 * Supported data types for collective communication operations.
 * Each type corresponds to a specific C data type with known size.
 */
typedef enum {
  PG_DATATYPE_INT = 0,   /* 32-bit signed integer */
  PG_DATATYPE_DOUBLE = 1 /* 64-bit double precision floating point */
} pg_datatype_t;

/**
 * Reduction Operations for Collective Communications
 *
 * Mathematical operations that can be applied during collective
 * reductions such as all-reduce and reduce-scatter.
 */
typedef enum {
  PG_OPERATION_SUM = 0,    /* Element-wise addition */
  PG_OPERATION_PRODUCT = 1 /* Element-wise multiplication */
} pg_operation_t;

/**
 * Internal Process Group Handle Structure
 *
 * Contains all state and resources needed for process group operations.
 * This structure is opaque to users and accessed only through the
 * public API functions.
 */
struct pg_handle_internal {
  /* Process Group Topology Information */
  int process_rank;       /* This process's rank (0-based index) */
  int process_group_size; /* Total number of processes in group */
  char **hostname_list;   /* Array of hostnames for all processes */

  /* RDMA Infrastructure */
  rdma_context_t rdma_context; /* Shared RDMA device context and resources */

  /* Ring Topology Queue Pairs */
  struct ibv_qp *left_neighbor_qp;  /* Queue pair to left neighbor in ring */
  struct ibv_qp *right_neighbor_qp; /* Queue pair to right neighbor in ring */

  /* Communication Buffers for Left Neighbor */
  void *left_send_buffer;         /* Buffer for sending data to left neighbor */
  void *left_receive_buffer;      /* Buffer for receiving data from left neighbor */
  struct ibv_mr *left_send_mr;    /* Memory region for left send buffer */
  struct ibv_mr *left_receive_mr; /* Memory region for left receive buffer */

  /* Communication Buffers for Right Neighbor */
  void *right_send_buffer;         /* Buffer for sending data to right neighbor */
  void *right_receive_buffer;      /* Buffer for receiving data from right neighbor */
  struct ibv_mr *right_send_mr;    /* Memory region for right send buffer */
  struct ibv_mr *right_receive_mr; /* Memory region for right receive buffer */

  /* Buffer Configuration */
  size_t total_buffer_size_bytes; /* Total size of each communication buffer */
  size_t chunk_size_bytes;        /* Size of data chunks for pipelined operations */
  
  /* Pipelining Configuration */
  size_t eager_max;               /* Maximum message size for eager protocol */
  size_t chunk_bytes;             /* Chunk size for pipelined operations */
  int inflight;                   /* Maximum number of inflight operations */

  /* Local final destination for collectives (must be an MR) */
  struct ibv_mr *final_recv_mr;
  uint64_t final_recv_base;  /* (uintptr_t)receive_buffer at call time */
  size_t final_recv_bytes;

  /* Neighbors' exposed buffers (we read from left, we let right read from us) */
  uint64_t left_remote_base;
  uint32_t left_remote_rkey;
  uint64_t right_remote_base;
  uint32_t right_remote_rkey;

  /* Remote Memory Region Information for Zero-Copy Operations */
  uint64_t *remote_buffer_addrs;  /* Array of remote buffer addresses for each process */
  uint32_t *remote_buffer_rkeys;  /* Array of remote buffer rkeys for each process */
  size_t remote_buffer_size;      /* Size of each remote buffer */

  /* Reduce-scatter specific remote buffer exposure */
  uint64_t *rs_remote_buffer_addrs; /* Remote addresses for left_receive_buffer exposures */
  uint32_t *rs_remote_buffer_rkeys; /* Remote rkeys for reduce-scatter writes */
  size_t rs_remote_buffer_size;     /* Size of reduce-scatter remote buffer */
  int rs_remote_info_ready;         /* Flag indicating remote info has been exchanged */
};

/*
 * =============================================================================
 * Inline Helper Functions for Collective Operations
 * =============================================================================
 */

/**
 * Apply reduction operation to data elements
 *
 * Performs element-wise reduction operation between two source arrays
 * and stores the result in the destination array. Supports different
 * data types and operations.
 *
 * @param destination_buffer: Buffer to store operation results
 * @param source_buffer_1: First source buffer for operation
 * @param source_buffer_2: Second source buffer for operation
 * @param element_count: Number of elements to process
 * @param data_type: Type of data elements (int or double)
 * @param operation: Reduction operation to perform (sum or product)
 */
static inline void pg_apply_reduction_operation(void *destination_buffer, void *source_buffer_1, void *source_buffer_2,
                                                int element_count, pg_datatype_t data_type, pg_operation_t operation) {
  if (data_type == PG_DATATYPE_INT) {
    int *dest_array = (int *)destination_buffer;
    int *src1_array = (int *)source_buffer_1;
    int *src2_array = (int *)source_buffer_2;

    for (int i = 0; i < element_count; i++) {
      if (operation == PG_OPERATION_SUM) {
        dest_array[i] = src1_array[i] + src2_array[i];
      } else if (operation == PG_OPERATION_PRODUCT) {
        dest_array[i] = src1_array[i] * src2_array[i];
      }
    }
  } else if (data_type == PG_DATATYPE_DOUBLE) {
    double *dest_array = (double *)destination_buffer;
    double *src1_array = (double *)source_buffer_1;
    double *src2_array = (double *)source_buffer_2;

    for (int i = 0; i < element_count; i++) {
      if (operation == PG_OPERATION_SUM) {
        dest_array[i] = src1_array[i] + src2_array[i];
      } else if (operation == PG_OPERATION_PRODUCT) {
        dest_array[i] = src1_array[i] * src2_array[i];
      }
    }
  }
}

/**
 * Get size in bytes of a single data element
 *
 * Returns the size of one element for the specified data type.
 * Used for buffer size calculations and pointer arithmetic.
 *
 * @param data_type: Data type to get size for
 * @return: Size in bytes of one element
 */
static inline size_t pg_get_datatype_element_size(pg_datatype_t data_type) {
  switch (data_type) {
    case PG_DATATYPE_INT:
      return sizeof(int);
    case PG_DATATYPE_DOUBLE:
      return sizeof(double);
    default:
      return 0; /* Invalid data type */
  }
}

/**
 * Calculate ring topology neighbor ranks
 *
 * Computes the rank of left and right neighbors in a ring topology
 * with proper wraparound handling.
 *
 * @param current_rank: This process's rank
 * @param group_size: Total number of processes
 * @param left_neighbor_rank: Pointer to store left neighbor rank
 * @param right_neighbor_rank: Pointer to store right neighbor rank
 */
static inline void pg_calculate_ring_neighbors(int current_rank, int group_size, int *left_neighbor_rank,
                                               int *right_neighbor_rank) {
  *left_neighbor_rank = (current_rank - 1 + group_size) % group_size;
  *right_neighbor_rank = (current_rank + 1) % group_size;
}

/**
 * Calculate chunk boundaries for data distribution
 *
 * Determines the start offset and size of a data chunk for a specific
 * process in collective operations that distribute data across processes.
 *
 * @param process_rank: Rank of process to calculate chunk for
 * @param group_size: Total number of processes
 * @param total_elements: Total number of data elements to distribute
 * @param element_size: Size in bytes of each element
 * @param chunk_offset: Pointer to store chunk start offset in bytes
 * @param chunk_size: Pointer to store chunk size in bytes
 */
static inline void pg_calculate_data_chunk_boundaries(int process_rank, int group_size, int total_elements,
                                                      size_t element_size, size_t *chunk_offset, size_t *chunk_size) {
  int elements_per_process = total_elements / group_size;
  int remaining_elements = total_elements % group_size;

  /* Distribute remaining elements among first few processes */
  int chunk_element_count = elements_per_process;
  if (process_rank < remaining_elements) {
    chunk_element_count++;
  }

  /* Calculate offset considering uneven distribution */
  int offset_elements = process_rank * elements_per_process;
  if (process_rank < remaining_elements) {
    offset_elements += process_rank;
  } else {
    offset_elements += remaining_elements;
  }

  *chunk_offset = offset_elements * element_size;
  *chunk_size = chunk_element_count * element_size;
}

#endif /* PG_INTERNAL_H */
