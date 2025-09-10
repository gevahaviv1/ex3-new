#ifndef RDMA_API_H
#define RDMA_API_H

#include <infiniband/verbs.h>
#include <stdint.h>
#include <stdio.h>
#include "constants.h"

/**
 * RDMA Context Structure
 * 
 * Encapsulates all RDMA resources needed for InfiniBand communication.
 * This structure maintains the device context, protection domain, and
 * completion queue that are shared across all queue pairs.
 */
typedef struct {
    struct ibv_device *ib_device;          /* InfiniBand device handle */
    struct ibv_context *device_context;    /* Device context for operations */
    struct ibv_pd *protection_domain;      /* Protection domain for memory regions */
    struct ibv_cq *completion_queue;       /* Completion queue for work completions */
    int ib_port_number;                    /* Active InfiniBand port number */
    int gid_index;                         /* GID index for RoCE/GRH addressing */
} rdma_context_t;

/**
 * Queue Pair Bootstrap Information
 * 
 * Contains the essential information needed to establish an RDMA connection
 * between two queue pairs. This data is exchanged during the bootstrap phase
 * via TCP before transitioning queue pairs to Ready-to-Receive (RTR) state.
 */
typedef struct {
    uint32_t queue_pair_number;            /* Local queue pair number */
    uint16_t local_identifier;             /* Local LID for addressing */
    union ibv_gid global_identifier;       /* Global identifier for routing */
    uint32_t packet_sequence_number;       /* Starting packet sequence number */
} rdma_qp_bootstrap_info_t;

/*
 * =============================================================================
 * RDMA Device and Context Management
 * =============================================================================
 */

/**
 * Initialize RDMA context with device resources
 * 
 * Sets up the fundamental RDMA resources including device context, protection
 * domain, and completion queue. If device_name is NULL, uses the first
 * available InfiniBand device.
 * 
 * @param rdma_ctx: Pointer to RDMA context structure to initialize
 * @param device_name: Name of specific IB device to use, or NULL for first available
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
int rdma_initialize_context(rdma_context_t *rdma_ctx, const char *device_name);

/**
 * Clean up and release all RDMA context resources
 * 
 * Properly destroys completion queue, deallocates protection domain, and
 * closes device context. Safe to call multiple times or with partially
 * initialized contexts.
 * 
 * @param rdma_ctx: Pointer to RDMA context to clean up
 */
void rdma_cleanup_context(rdma_context_t *rdma_ctx);

/*
 * =============================================================================
 * Memory Region Management
 * =============================================================================
 */

/**
 * Register memory buffer for RDMA operations
 * 
 * Registers a user buffer with the RDMA device, enabling it to be used
 * for send/receive operations. The buffer must remain valid until
 * deregistration.
 * 
 * @param rdma_ctx: Initialized RDMA context
 * @param buffer_ptr: Pointer to memory buffer to register
 * @param buffer_size: Size of buffer in bytes
 * @return: Memory region handle on success, NULL on failure
 */
struct ibv_mr *rdma_register_memory_buffer(rdma_context_t *rdma_ctx, 
                                          void *buffer_ptr, 
                                          size_t buffer_size);

/**
 * Deregister and release memory region
 * 
 * Safely deregisters a previously registered memory region. After this
 * call, the memory region handle becomes invalid.
 * 
 * @param memory_region: Memory region handle to deregister
 */
void rdma_deregister_memory_buffer(struct ibv_mr *memory_region);

/*
 * =============================================================================
 * Queue Pair Management
 * =============================================================================
 */

/**
 * Create a new reliable connection queue pair
 * 
 * Allocates and initializes a new queue pair for reliable connection (RC)
 * transport. The queue pair is created in RESET state and must be transitioned
 * through INIT -> RTR -> RTS states before use.
 * 
 * @param rdma_ctx: Initialized RDMA context
 * @param queue_pair_ptr: Pointer to store created queue pair handle
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
int rdma_create_queue_pair(rdma_context_t *rdma_ctx, struct ibv_qp **queue_pair_ptr);

/**
 * Transition queue pair from RESET to INIT state
 * 
 * Configures the queue pair for the specified InfiniBand port and sets
 * access permissions. This is the first step in queue pair initialization.
 * 
 * @param queue_pair: Queue pair to transition
 * @param ib_port_num: InfiniBand port number to use
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
int rdma_transition_qp_to_init(struct ibv_qp *queue_pair, int ib_port_num);

/**
 * Transition queue pair from INIT to Ready-to-Receive (RTR) state
 * 
 * Configures the queue pair to receive data from the remote peer. Requires
 * the remote peer's bootstrap information obtained during TCP exchange.
 * 
 * @param queue_pair: Queue pair to transition
 * @param remote_qp_info: Bootstrap info from remote peer
 * @param ib_port_num: Local InfiniBand port number
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
int rdma_transition_qp_to_rtr(struct ibv_qp *queue_pair, 
                             rdma_qp_bootstrap_info_t *remote_qp_info, 
                             int ib_port_num,
                             int gid_index);

/**
 * Transition queue pair from RTR to Ready-to-Send (RTS) state
 * 
 * Enables the queue pair to send data. After this transition, the queue
 * pair is fully operational for bidirectional communication.
 * 
 * @param queue_pair: Queue pair to transition
 * @param local_psn: Local packet sequence number for sends
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
int rdma_transition_qp_to_rts(struct ibv_qp *queue_pair, uint32_t local_psn);

/**
 * Destroy queue pair and release resources
 * 
 * Safely destroys a queue pair and releases associated resources.
 * Safe to call with NULL pointer.
 * 
 * @param queue_pair: Queue pair to destroy
 */
void rdma_destroy_queue_pair(struct ibv_qp *queue_pair);

/*
 * =============================================================================
 * Queue Pair Information Management
 * =============================================================================
 */

/**
 * Extract bootstrap information from local queue pair
 * 
 * Queries the local queue pair and device to populate bootstrap information
 * that will be exchanged with remote peers during connection establishment.
 * 
 * @param rdma_ctx: RDMA context containing device information
 * @param queue_pair: Local queue pair to query
 * @param qp_info: Structure to populate with bootstrap information
 */
void rdma_extract_qp_bootstrap_info(rdma_context_t *rdma_ctx, 
                                   struct ibv_qp *queue_pair, 
                                   rdma_qp_bootstrap_info_t *qp_info);

/*
 * =============================================================================
 * RDMA Communication Operations
 * =============================================================================
 */

/**
 * Post receive work request to queue pair
 * 
 * Prepares the queue pair to receive incoming data into the specified buffer.
 * The buffer must be registered as a memory region before use.
 * 
 * @param queue_pair: Queue pair to post receive on
 * @param buffer_ptr: Registered buffer to receive data into
 * @param buffer_size: Size of receive buffer in bytes
 * @param memory_region: Memory region handle for the buffer
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
int rdma_post_receive_request(struct ibv_qp *queue_pair, 
                             void *buffer_ptr, 
                             size_t buffer_size, 
                             struct ibv_mr *memory_region);

/**
 * Post send work request to queue pair
 * 
 * Initiates sending data from the specified buffer to the remote peer.
 * The operation completes asynchronously and must be polled for completion.
 * 
 * @param queue_pair: Queue pair to send data on
 * @param buffer_ptr: Registered buffer containing data to send
 * @param data_size: Amount of data to send in bytes
 * @param memory_region: Memory region handle for the buffer
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
int rdma_post_send_request(struct ibv_qp *queue_pair, 
                          void *buffer_ptr, 
                          size_t data_size, 
                          struct ibv_mr *memory_region);

/**
 * Poll completion queue for work completion
 * 
 * Blocks until a work request completes (either send or receive) and
 * returns the completion status. This function handles the polling loop
 * internally.
 * 
 * @param completion_queue: Completion queue to poll
 * @param work_completion: Structure to store completion information
 * @return: PG_SUCCESS on successful completion, PG_ERROR on failure
 */
int rdma_poll_for_completion(struct ibv_cq *completion_queue, 
                            struct ibv_wc *work_completion);

/**
 * Poll completion queue for specific work completion by ID
 * 
 * Blocks until a work request with the specified work request ID completes.
 * This allows distinguishing between send and receive completions.
 * 
 * @param completion_queue: Completion queue to poll
 * @param work_completion: Structure to store completion information
 * @param expected_wr_id: Work request ID to wait for
 * @return: PG_SUCCESS on successful completion, PG_ERROR on failure
 */
int rdma_poll_for_specific_completion(struct ibv_cq *completion_queue, 
                                     struct ibv_wc *work_completion,
                                     uint64_t expected_wr_id);

#endif /* RDMA_API_H */
