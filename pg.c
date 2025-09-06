#define _POSIX_C_SOURCE 200809L

#include "pg.h"
#include "pg_internal.h"
#include "pg_net.h"
#include "RDMA_api.h"
#include "constants.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <stdbool.h>

/*
 * =============================================================================
 * Private Helper Functions for Process Group Initialization
 * =============================================================================
 */

/**
 * Allocate and initialize communication buffers
 *
 * @param process_group: Process group handle to initialize buffers for
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
static int allocate_communication_buffers(pg_handle_internal_t *process_group) {
  /* Allocate memory buffers for ring communication */
  process_group->left_send_buffer =
      malloc(process_group->total_buffer_size_bytes);
  process_group->left_receive_buffer =
      malloc(process_group->total_buffer_size_bytes);
  process_group->right_send_buffer =
      malloc(process_group->total_buffer_size_bytes);
  process_group->right_receive_buffer =
      malloc(process_group->total_buffer_size_bytes);

  /* Check if all allocations succeeded */
  if (!process_group->left_send_buffer || !process_group->left_receive_buffer ||
      !process_group->right_send_buffer ||
      !process_group->right_receive_buffer) {
    fprintf(stderr, "Failed to allocate communication buffers\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

/**
 * Register communication buffers with RDMA device
 *
 * @param process_group: Process group handle with allocated buffers
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
static int register_communication_buffers(pg_handle_internal_t *process_group) {
  /* Register all buffers as memory regions */
  process_group->left_send_mr = rdma_register_memory_buffer(
      &process_group->rdma_context, process_group->left_send_buffer,
      process_group->total_buffer_size_bytes);

  process_group->left_receive_mr = rdma_register_memory_buffer(
      &process_group->rdma_context, process_group->left_receive_buffer,
      process_group->total_buffer_size_bytes);

  process_group->right_send_mr = rdma_register_memory_buffer(
      &process_group->rdma_context, process_group->right_send_buffer,
      process_group->total_buffer_size_bytes);

  process_group->right_receive_mr = rdma_register_memory_buffer(
      &process_group->rdma_context, process_group->right_receive_buffer,
      process_group->total_buffer_size_bytes);

  /* Check if all registrations succeeded */
  if (!process_group->left_send_mr || !process_group->left_receive_mr ||
      !process_group->right_send_mr || !process_group->right_receive_mr) {
    fprintf(stderr, "Failed to register communication buffers with RDMA\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

/**
 * Create and initialize queue pairs for ring topology
 *
 * @param process_group: Process group handle to create queue pairs for
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
static int create_ring_topology_queue_pairs(
    pg_handle_internal_t *process_group) {
  /* Create queue pairs for left and right neighbors */
  if (rdma_create_queue_pair(&process_group->rdma_context,
                             &process_group->left_neighbor_qp) != PG_SUCCESS) {
    fprintf(stderr, "Failed to create left neighbor queue pair\n");
    return PG_ERROR;
  }

  if (rdma_create_queue_pair(&process_group->rdma_context,
                             &process_group->right_neighbor_qp) != PG_SUCCESS) {
    fprintf(stderr, "Failed to create right neighbor queue pair\n");
    return PG_ERROR;
  }

  /* Transition queue pairs to INIT state */
  if (rdma_transition_qp_to_init(process_group->left_neighbor_qp,
                                 process_group->rdma_context.ib_port_number) !=
      PG_SUCCESS) {
    fprintf(stderr, "Failed to initialize left neighbor queue pair\n");
    return PG_ERROR;
  }

  if (rdma_transition_qp_to_init(process_group->right_neighbor_qp,
                                 process_group->rdma_context.ib_port_number) !=
      PG_SUCCESS) {
    fprintf(stderr, "Failed to initialize right neighbor queue pair\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

/**
 * Establish RDMA connections with ring topology neighbors
 *
 * @param process_group: Process group handle with initialized queue pairs
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
static int establish_neighbor_connections(pg_handle_internal_t *process_group) {
  /* Calculate neighbor ranks in ring topology */
  int left_neighbor_rank, right_neighbor_rank;
  pg_calculate_ring_neighbors(process_group->process_rank,
                              process_group->process_group_size,
                              &left_neighbor_rank, &right_neighbor_rank);

  /* Skip self-connections for single-process groups */
  if (process_group->process_group_size == 1) {
    return PG_SUCCESS;
  }

  /* Extract local queue pair bootstrap information */
  rdma_qp_bootstrap_info_t left_local_info, left_remote_info;
  rdma_qp_bootstrap_info_t right_local_info, right_remote_info;

  rdma_extract_qp_bootstrap_info(&process_group->rdma_context,
                                 process_group->left_neighbor_qp,
                                 &left_local_info);

  rdma_extract_qp_bootstrap_info(&process_group->rdma_context,
                                 process_group->right_neighbor_qp,
                                 &right_local_info);

  /* Establish TCP connections for bootstrap information exchange */
  int left_tcp_socket = -1, right_tcp_socket = -1;

  /*
   * Fix deadlock: Use rank-based ordering to determine who acts as
   * server/client Lower rank process acts as server, higher rank acts as client
   * This breaks the circular dependency
   */

  /* Handle left neighbor connection */
  if (left_neighbor_rank != process_group->process_rank) {
    if (process_group->process_rank < left_neighbor_rank) {
      /* I have lower rank, act as server */

      left_tcp_socket = pgnet_establish_tcp_connection(
          NULL, PG_DEFAULT_PORT + process_group->process_rank,
          1 /* server mode */
      );
    } else {
      /* I have higher rank, act as client */

      left_tcp_socket = pgnet_establish_tcp_connection(
          process_group->hostname_list[left_neighbor_rank],
          PG_DEFAULT_PORT + left_neighbor_rank, 0 /* client mode */
      );
    }

    if (left_tcp_socket < 0) {
      fprintf(stderr,
              "Failed to establish TCP connection with left neighbor\n");
      return PG_ERROR;
    }
  }

  /* Handle right neighbor connection */
  if (right_neighbor_rank != process_group->process_rank) {
    if (process_group->process_rank < right_neighbor_rank) {
      /* I have lower rank, act as server */

      right_tcp_socket = pgnet_establish_tcp_connection(
          NULL, PG_DEFAULT_PORT + process_group->process_rank,
          1 /* server mode */
      );
    } else {
      /* I have higher rank, act as client */

      right_tcp_socket = pgnet_establish_tcp_connection(
          process_group->hostname_list[right_neighbor_rank],
          PG_DEFAULT_PORT + right_neighbor_rank, 0 /* client mode */
      );
    }

    if (right_tcp_socket < 0) {
      fprintf(stderr,
              "Failed to establish TCP connection with right neighbor\n");
      if (left_tcp_socket >= 0) close(left_tcp_socket);
      return PG_ERROR;
    }
  }

  /* Exchange bootstrap information over TCP */
  if (left_tcp_socket >= 0) {
    /* Determine if we're acting as server or client for left neighbor */
    int left_server_mode =
        (process_group->process_rank < left_neighbor_rank) ? 1 : 0;

    if (pgnet_exchange_rdma_bootstrap_info(left_tcp_socket, &left_local_info,
                                           &left_remote_info,
                                           left_server_mode) != PG_SUCCESS) {
      fprintf(stderr, "Failed to exchange bootstrap info with left neighbor\n");
      close(left_tcp_socket);
      if (right_tcp_socket >= 0) close(right_tcp_socket);
      return PG_ERROR;
    }
    close(left_tcp_socket);
  }

  if (right_tcp_socket >= 0) {
    /* Determine if we're acting as server or client for right neighbor */
    int right_server_mode =
        (process_group->process_rank < right_neighbor_rank) ? 1 : 0;

    if (pgnet_exchange_rdma_bootstrap_info(right_tcp_socket, &right_local_info,
                                           &right_remote_info,
                                           right_server_mode) != PG_SUCCESS) {
      fprintf(stderr,
              "Failed to exchange bootstrap info with right neighbor\n");
      close(right_tcp_socket);
      return PG_ERROR;
    }
    close(right_tcp_socket);
  }

  /* Transition queue pairs to Ready-to-Receive (RTR) state */

  if (left_neighbor_rank != process_group->process_rank) {
    if (rdma_transition_qp_to_rtr(
            process_group->left_neighbor_qp, &left_remote_info,
            process_group->rdma_context.ib_port_number) != PG_SUCCESS) {
      fprintf(stderr, "Failed to transition left QP to RTR state\n");
      return PG_ERROR;
    }
  }

  if (right_neighbor_rank != process_group->process_rank) {
    if (rdma_transition_qp_to_rtr(
            process_group->right_neighbor_qp, &right_remote_info,
            process_group->rdma_context.ib_port_number) != PG_SUCCESS) {
      fprintf(stderr, "Failed to transition right QP to RTR state\n");
      return PG_ERROR;
    }
  }

  /* Transition queue pairs to Ready-to-Send (RTS) state */

  if (left_neighbor_rank != process_group->process_rank) {
    if (rdma_transition_qp_to_rts(process_group->left_neighbor_qp,
                                  left_local_info.packet_sequence_number) !=
        PG_SUCCESS) {
      fprintf(stderr, "Failed to transition left QP to RTS state\n");
      return PG_ERROR;
    }
  }

  if (right_neighbor_rank != process_group->process_rank) {
    if (rdma_transition_qp_to_rts(process_group->right_neighbor_qp,
                                  right_local_info.packet_sequence_number) !=
        PG_SUCCESS) {
      fprintf(stderr, "Failed to transition right QP to RTS state\n");
      return PG_ERROR;
    }
  }

  return PG_SUCCESS;
}

/*
 * =============================================================================
 * Process Group Lifecycle Management Implementation
 * =============================================================================
 */

int pg_initialize(const char *server_list_string,
                  pg_handle_t *process_group_handle) {
  PG_CHECK_NULL(server_list_string, "Server list string is NULL");
  PG_CHECK_NULL(process_group_handle, "Process group handle pointer is NULL");

  /* Allocate and initialize process group structure */
  pg_handle_internal_t *process_group = calloc(1, sizeof(pg_handle_internal_t));
  if (!process_group) {
    fprintf(stderr, "Failed to allocate process group structure\n");
    return PG_ERROR;
  }

  /* Parse hostname list and determine process topology */
  process_group->hostname_list = pgnet_parse_hostname_list(
      server_list_string, &process_group->process_group_size);
  if (!process_group->hostname_list) {
    fprintf(stderr, "Failed to parse hostname list\n");
    free(process_group);
    return PG_ERROR;
  }

  /* Determine this process's rank within the group */
  process_group->process_rank = pgnet_determine_process_rank(
      process_group->hostname_list, process_group->process_group_size);
  if (process_group->process_rank < 0) {
    fprintf(stderr, "Could not determine process rank\n");
    pgnet_free_hostname_list(process_group->hostname_list,
                             process_group->process_group_size);
    free(process_group);
    return PG_ERROR;
  }

  /* Initialize RDMA device context */
  if (rdma_initialize_context(&process_group->rdma_context, NULL) !=
      PG_SUCCESS) {
    fprintf(stderr, "Failed to initialize RDMA context\n");
    pgnet_free_hostname_list(process_group->hostname_list,
                             process_group->process_group_size);
    free(process_group);
    return PG_ERROR;
  }

  /* Configure buffer sizes */
  process_group->total_buffer_size_bytes = PG_BUFFER_SIZE_BYTES;
  process_group->chunk_size_bytes = PG_CHUNK_SIZE_BYTES;

  /* Allocate communication buffers */
  if (allocate_communication_buffers(process_group) != PG_SUCCESS) {
    pg_cleanup(process_group);
    return PG_ERROR;
  }

  /* Register buffers with RDMA device */
  if (register_communication_buffers(process_group) != PG_SUCCESS) {
    pg_cleanup(process_group);
    return PG_ERROR;
  }

  /* Create and initialize queue pairs for ring topology */
  if (create_ring_topology_queue_pairs(process_group) != PG_SUCCESS) {
    pg_cleanup(process_group);
    return PG_ERROR;
  }

  /* Establish RDMA connections with ring neighbors */
  if (establish_neighbor_connections(process_group) != PG_SUCCESS) {
    pg_cleanup(process_group);
    return PG_ERROR;
  }

  /* Return initialized handle to caller */
  *process_group_handle = process_group;
  return PG_SUCCESS;
}

int pg_cleanup(pg_handle_t process_group_handle) {
  pg_handle_internal_t *process_group =
      (pg_handle_internal_t *)process_group_handle;
  if (!process_group) {
    return PG_SUCCESS;
  }

  /* Destroy RDMA queue pairs */
  rdma_destroy_queue_pair(process_group->left_neighbor_qp);
  rdma_destroy_queue_pair(process_group->right_neighbor_qp);

  /* Deregister memory regions */
  rdma_deregister_memory_buffer(process_group->left_send_mr);
  rdma_deregister_memory_buffer(process_group->left_receive_mr);
  rdma_deregister_memory_buffer(process_group->right_send_mr);
  rdma_deregister_memory_buffer(process_group->right_receive_mr);

  /* Clean up RDMA context */
  rdma_cleanup_context(&process_group->rdma_context);

  /* Free communication buffers */
  free(process_group->left_send_buffer);
  free(process_group->left_receive_buffer);
  free(process_group->right_send_buffer);
  free(process_group->right_receive_buffer);

  /* Free hostname list */
  pgnet_free_hostname_list(process_group->hostname_list,
                           process_group->process_group_size);

  /* Free the process group structure itself */
  free(process_group);
  return PG_SUCCESS;
}

/*
 * =============================================================================
 * Private Helper Functions for Collective Operations
 * =============================================================================
 */

/**
 * Perform one communication step in ring-based collective algorithm
 *
 * @param process_group: Process group handle
 * @param send_data: Data to send to right neighbor
 * @param receive_data: Buffer to receive data from left neighbor
 * @param data_size: Size of data to send/receive in bytes
 * @return: PG_SUCCESS on success, PG_ERROR on failure
 */
static int perform_ring_communication_step(pg_handle_internal_t *process_group,
                                           void *send_data, void *receive_data,
                                           size_t data_size) {
  /* Post receive request for incoming data from left neighbor */
  if (rdma_post_receive_request(process_group->left_neighbor_qp, receive_data,
                                data_size,
                                process_group->left_receive_mr) != PG_SUCCESS) {
    fprintf(stderr, "Failed to post receive request\n");
    return PG_ERROR;
  }

  /* Synchronization barrier: ensure all processes have posted receives */

  /* All processes wait the same amount to ensure synchronization */
  usleep(200000); /* 200ms delay for all processes */

  /* Post send request to right neighbor */
  if (rdma_post_send_request(process_group->right_neighbor_qp, send_data,
                             data_size,
                             process_group->left_send_mr) != PG_SUCCESS) {
    fprintf(stderr, "Failed to post send request\n");
    return PG_ERROR;
  }

  /* Wait for both operations to complete */
  struct ibv_wc work_completion;

  /* Wait for receive completion */
  if (rdma_poll_for_completion(process_group->rdma_context.completion_queue,
                               &work_completion) != PG_SUCCESS) {
    fprintf(stderr, "Failed to complete receive operation\n");
    return PG_ERROR;
  }

  /* Wait for send completion */
  if (rdma_poll_for_completion(process_group->rdma_context.completion_queue,
                               &work_completion) != PG_SUCCESS) {
    fprintf(stderr, "Failed to complete send operation\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

/*
 * =============================================================================
 * Collective Communication Operations Implementation
 * =============================================================================
 */

int pg_reduce_scatter(pg_handle_t process_group_handle, void *send_buffer,
                      void *receive_buffer, int element_count,
                      pg_datatype_t data_type, pg_operation_t reduction_op) {
  pg_handle_internal_t *process_group =
      (pg_handle_internal_t *)process_group_handle;

  PG_CHECK_NULL(process_group, "Process group handle is NULL");
  PG_CHECK_NULL(send_buffer, "Send buffer is NULL");
  PG_CHECK_NULL(receive_buffer, "Receive buffer is NULL");

  int group_size = process_group->process_group_size;
  int process_rank = process_group->process_rank;

  /* Global synchronization barrier before starting collective operation */
  usleep(500000); /* 500ms initial barrier for all processes */

  /* Handle single-process case */
  if (group_size == 1) {
    size_t element_size = pg_get_datatype_element_size(data_type);
    memcpy(receive_buffer, send_buffer, element_count * element_size);
    return PG_SUCCESS;
  }

  size_t element_size = pg_get_datatype_element_size(data_type);
  size_t total_data_size = element_count * element_size;
  size_t chunk_size_bytes = (element_count / group_size) * element_size;

  /* Copy input data to working buffer */
  memcpy(process_group->left_send_buffer, send_buffer, total_data_size);

  /* Perform reduce-scatter algorithm with (group_size - 1) steps */
  for (int communication_step = 0; communication_step < group_size - 1;
       communication_step++) {
    /* Determine which chunk to reduce in this step */
    int reduction_chunk_index =
        (process_rank - communication_step + group_size) % group_size;

    /* Perform ring communication step */
    if (perform_ring_communication_step(process_group,
                                        process_group->left_send_buffer,
                                        process_group->left_receive_buffer,
                                        total_data_size) != PG_SUCCESS) {
      return PG_ERROR;
    }

    /* Apply reduction operation to the designated chunk */
    char *local_chunk_ptr = (char *)process_group->left_send_buffer +
                            (reduction_chunk_index * chunk_size_bytes);
    char *remote_chunk_ptr = (char *)process_group->left_receive_buffer +
                             (reduction_chunk_index * chunk_size_bytes);

    int chunk_element_count = chunk_size_bytes / element_size;
    pg_apply_reduction_operation(local_chunk_ptr, local_chunk_ptr,
                                 remote_chunk_ptr, chunk_element_count,
                                 data_type, reduction_op);

    /* Prepare data for next communication step */
    memcpy(process_group->left_send_buffer, process_group->left_receive_buffer,
           total_data_size);
    memcpy((char *)process_group->left_send_buffer +
               (reduction_chunk_index * chunk_size_bytes),
           local_chunk_ptr, chunk_size_bytes);
  }

  /* Extract this process's final result chunk */
  int my_chunk_index = process_rank;
  char *my_result_chunk = (char *)process_group->left_send_buffer +
                          (my_chunk_index * chunk_size_bytes);
  memcpy(receive_buffer, my_result_chunk, chunk_size_bytes);

  return PG_SUCCESS;
}

int pg_all_gather(pg_handle_t process_group_handle, void *send_buffer,
                  void *receive_buffer, int element_count,
                  pg_datatype_t data_type) {
  pg_handle_internal_t *process_group =
      (pg_handle_internal_t *)process_group_handle;

  PG_CHECK_NULL(process_group, "Process group handle is NULL");
  PG_CHECK_NULL(send_buffer, "Send buffer is NULL");
  PG_CHECK_NULL(receive_buffer, "Receive buffer is NULL");

  int group_size = process_group->process_group_size;
  int process_rank = process_group->process_rank;

  /* Handle single-process case */
  if (group_size == 1) {
    size_t element_size = pg_get_datatype_element_size(data_type);
    memcpy(receive_buffer, send_buffer, element_count * element_size);
    return PG_SUCCESS;
  }

  size_t element_size = pg_get_datatype_element_size(data_type);
  size_t total_data_size = element_count * element_size;
  size_t chunk_size_bytes = (element_count / group_size) * element_size;

  /* Initialize receive buffer and place local data in correct position */
  memset(receive_buffer, 0, total_data_size);
  char *my_data_position =
      (char *)receive_buffer + (process_rank * chunk_size_bytes);
  memcpy(my_data_position, send_buffer, chunk_size_bytes);

  /* Copy initialized data to working buffer */
  memcpy(process_group->left_send_buffer, receive_buffer, total_data_size);

  /* Perform all-gather algorithm with (group_size - 1) steps */
  for (int communication_step = 0; communication_step < group_size - 1;
       communication_step++) {
    /* Perform ring communication step */
    if (perform_ring_communication_step(process_group,
                                        process_group->left_send_buffer,
                                        process_group->left_receive_buffer,
                                        total_data_size) != PG_SUCCESS) {
      return PG_ERROR;
    }

    /* Merge received data with local accumulated data */
    for (int chunk_index = 0; chunk_index < group_size; chunk_index++) {
      char *local_chunk_ptr = (char *)process_group->left_send_buffer +
                              (chunk_index * chunk_size_bytes);
      char *remote_chunk_ptr = (char *)process_group->left_receive_buffer +
                               (chunk_index * chunk_size_bytes);

      /* Check if remote chunk contains valid data (non-zero) */
      int remote_has_data = 0;
      for (size_t byte_index = 0; byte_index < chunk_size_bytes; byte_index++) {
        if (remote_chunk_ptr[byte_index] != 0) {
          remote_has_data = 1;
          break;
        }
      }

      /* If remote has data and local doesn't, copy it over */
      if (remote_has_data) {
        int local_has_data = 0;
        for (size_t byte_index = 0; byte_index < chunk_size_bytes;
             byte_index++) {
          if (local_chunk_ptr[byte_index] != 0) {
            local_has_data = 1;
            break;
          }
        }

        if (!local_has_data) {
          memcpy(local_chunk_ptr, remote_chunk_ptr, chunk_size_bytes);
        }
      }
    }

    /* Update working buffer with merged data */
    memcpy(process_group->left_send_buffer, process_group->left_receive_buffer,
           total_data_size);

    /* Restore any data we already had accumulated */
    for (int chunk_index = 0; chunk_index < group_size; chunk_index++) {
      char *working_chunk_ptr = (char *)process_group->left_send_buffer +
                                (chunk_index * chunk_size_bytes);
      char *accumulated_chunk_ptr =
          (char *)receive_buffer + (chunk_index * chunk_size_bytes);

      /* Check if we have accumulated data for this chunk */
      int accumulated_has_data = 0;
      for (size_t byte_index = 0; byte_index < chunk_size_bytes; byte_index++) {
        if (accumulated_chunk_ptr[byte_index] != 0) {
          accumulated_has_data = 1;
          break;
        }
      }

      if (accumulated_has_data) {
        memcpy(working_chunk_ptr, accumulated_chunk_ptr, chunk_size_bytes);
      }
    }
  }

  /* Copy final result to output buffer */
  memcpy(receive_buffer, process_group->left_send_buffer, total_data_size);
  return PG_SUCCESS;
}

int pg_all_reduce(pg_handle_t process_group_handle, void *send_buffer,
                  void *receive_buffer, int element_count,
                  pg_datatype_t data_type, pg_operation_t reduction_op) {
  PG_CHECK_NULL(process_group_handle, "Process group handle is NULL");
  PG_CHECK_NULL(send_buffer, "Send buffer is NULL");
  PG_CHECK_NULL(receive_buffer, "Receive buffer is NULL");

  /*
   * All-reduce is implemented as a two-phase algorithm:
   * 1. Reduce-scatter: Distribute partial reductions across processes
   * 2. All-gather: Collect and broadcast complete results to all processes
   */

  /* Phase 1: Reduce-scatter to get partial results */
  if (pg_reduce_scatter(process_group_handle, send_buffer, receive_buffer,
                        element_count, data_type, reduction_op) != PG_SUCCESS) {
    fprintf(stderr, "All-reduce failed during reduce-scatter phase\n");
    return PG_ERROR;
  }

  /* Phase 2: All-gather to distribute complete results */
  if (pg_all_gather(process_group_handle, receive_buffer, receive_buffer,
                    element_count, data_type) != PG_SUCCESS) {
    fprintf(stderr, "All-reduce failed during all-gather phase\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}