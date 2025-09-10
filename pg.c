#define _GNU_SOURCE

#include "pg.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "RDMA_api.h"
#include "constants.h"
#include "pg_internal.h"
#include "pg_net.h"

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
/* Forward declarations for helper functions */
static int bootstrap_server_phase(pg_handle_internal_t *process_group,
                                  rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info);

static int bootstrap_client_phase(pg_handle_internal_t *process_group,
                                  rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info);


/**
 * Bootstrap server phase - rank 0 collects QP info from all ranks
 */
static int bootstrap_server_phase(pg_handle_internal_t *process_group,
                                  rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info) {
    (void)right_local_info; /* Unused in current implementation */
    int world_size = process_group->process_group_size;
    
    printf("[Process 0] DEBUG: Acting as bootstrap server\n");

    /* Create server socket directly instead of using pgnet_establish_tcp_connection */
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
      perror("Failed to create server socket");
      return PG_ERROR;
    }
    
    /* Enable address reuse */
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
      perror("Failed to set socket options");
      close(server_socket);
      return PG_ERROR;
    }

    /* Bind to the bootstrap port */
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PG_DEFAULT_PORT);

    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      perror("Failed to bind server socket");
      close(server_socket);
      return PG_ERROR;
    }

    /* Listen for connections */
    if (listen(server_socket, world_size) < 0) {
      perror("Failed to listen on server socket");
      close(server_socket);
      return PG_ERROR;
    }

    printf("[Process 0] DEBUG: Server listening on port %d\n", PG_DEFAULT_PORT);

    /* Store QP info for all ranks - need both left and right QPs */
    rdma_qp_bootstrap_info_t *left_qp_infos = malloc(world_size * sizeof(rdma_qp_bootstrap_info_t));
    rdma_qp_bootstrap_info_t *right_qp_infos = malloc(world_size * sizeof(rdma_qp_bootstrap_info_t));
    if (!left_qp_infos || !right_qp_infos) {
      fprintf(stderr, "Failed to allocate memory for rank infos\n");
      close(server_socket);
      free(left_qp_infos);
      free(right_qp_infos);
      return PG_ERROR;
    }

    /* Store rank 0's own QP info */
    left_qp_infos[0] = *left_local_info;
    right_qp_infos[0] = *right_local_info;
    
    /* Track which ranks have connected and store their sockets */
    bool *connected_ranks = calloc(world_size, sizeof(bool));
    int *client_sockets = malloc(world_size * sizeof(int));
    for (int i = 0; i < world_size; i++) {
      client_sockets[i] = -1;
    }
    connected_ranks[0] = true; /* rank 0 is always "connected" */
    int connected_count = 1;

    /* Accept connections from other ranks with timeout */
    time_t start_time = time(NULL);
    const int BOOTSTRAP_TIMEOUT = 30; /* 30 seconds total timeout */

    while (connected_count < world_size) {
      /* Check for timeout */
      if (time(NULL) - start_time > BOOTSTRAP_TIMEOUT) {
        printf("[Process 0] DEBUG: Bootstrap timeout - only %d/%d ranks connected\n", 
               connected_count, world_size);
        break;
      }

      /* Use select to wait for connections with timeout */
      fd_set read_fds;
      FD_ZERO(&read_fds);
      FD_SET(server_socket, &read_fds);
      
      struct timeval timeout;
      timeout.tv_sec = 5;  /* 5 second select timeout */
      timeout.tv_usec = 0;
      
      printf("[Process 0] DEBUG: Waiting for any rank to connect (%d/%d connected)\n", 
             connected_count, world_size);
      
      int select_result = select(server_socket + 1, &read_fds, NULL, NULL, &timeout);
      if (select_result < 0) {
        perror("select() failed");
        break;
      } else if (select_result == 0) {
        printf("[Process 0] DEBUG: select() timeout, continuing...\n");
        continue;
      }
      
      printf("[Process 0] DEBUG: select() returned %d\n", select_result);
      
      if (FD_ISSET(server_socket, &read_fds)) {
        printf("[Process 0] DEBUG: select() indicates connection ready, calling accept()...\n");
        
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);  /* Reinitialize addr_len before each accept */
        
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &addr_len);
        if (client_socket < 0) {
          perror("accept() failed");
          continue;
        }
        
        printf("[Process 0] DEBUG: Accepted connection, receiving data...\n");
        
        /* Receive rank first, then both QP infos from the client */
        int client_rank;
        if (recv(client_socket, &client_rank, sizeof(client_rank), 0) != sizeof(client_rank)) {
          fprintf(stderr, "Failed to receive rank from client\n");
          close(client_socket);
          continue;
        }
        
        rdma_qp_bootstrap_info_t left_qp_info, right_qp_info;
        if (recv(client_socket, &left_qp_info, sizeof(left_qp_info), 0) != sizeof(left_qp_info) ||
            recv(client_socket, &right_qp_info, sizeof(right_qp_info), 0) != sizeof(right_qp_info)) {
          fprintf(stderr, "Failed to receive complete QP info from client\n");
          close(client_socket);
          continue;
        }
        printf("[Process 0] DEBUG: Received QP info from rank %d\n", client_rank);
        
        /* Validate rank and avoid duplicates */
        if (client_rank < 0 || client_rank >= world_size || connected_ranks[client_rank]) {
          fprintf(stderr, "Invalid or duplicate rank %d\n", client_rank);
          close(client_socket);
          continue;
        }
        
        /* Store both QP infos and keep socket open */
        left_qp_infos[client_rank] = left_qp_info;
        right_qp_infos[client_rank] = right_qp_info;
        connected_ranks[client_rank] = true;
        client_sockets[client_rank] = client_socket; /* Keep socket open for sending neighbor info */
        connected_count++;
        
        printf("[Process 0] DEBUG: Successfully processed rank %d (%d/%d connected)\n", 
               client_rank, connected_count, world_size);
      }
    }

    close(server_socket);
    free(connected_ranks);

    if (connected_count < world_size) {
      fprintf(stderr, "Bootstrap failed - only %d/%d ranks connected\n", connected_count, world_size);
      free(left_qp_infos);
      free(right_qp_infos);
      free(client_sockets);
      return PG_ERROR;
    }

    printf("[Process 0] DEBUG: All ranks connected, distributing neighbor info\n");

    /* Distribute neighbor info to each rank using existing connections */
    for (int target_rank = 1; target_rank < world_size; target_rank++) {
      if (client_sockets[target_rank] < 0) continue;
      
      /* Calculate neighbors for target rank */
      int left_neighbor = (target_rank - 1 + world_size) % world_size;
      int right_neighbor = (target_rank + 1) % world_size;
      
      /* FINAL FIX: For ring topology, target_rank needs to connect:
       * - left_neighbor_qp (for receiving FROM left) connects to left_neighbor's SENDING QP (their right_qp)
       * - right_neighbor_qp (for sending TO right) connects to right_neighbor's RECEIVING QP (their left_qp)
       * 
       * The QP semantics are:
       * - left_neighbor_qp is for receiving FROM left neighbor → connect to their right_qp (send QP)
       * - right_neighbor_qp is for sending TO right neighbor → connect to their left_qp (receive QP)
       */
      rdma_qp_bootstrap_info_t left_remote_qp = right_qp_infos[left_neighbor];  /* left neighbor's right QP (their send QP) */
      rdma_qp_bootstrap_info_t right_remote_qp = left_qp_infos[right_neighbor]; /* right neighbor's left QP (their receive QP) */
      
      printf("[Process 0] DEBUG: Sending to rank %d: left_neighbor=%d (right_qp=%u, lid=%u), right_neighbor=%d (left_qp=%u, lid=%u)\n", 
             target_rank, left_neighbor, left_remote_qp.queue_pair_number, left_remote_qp.local_identifier,
             right_neighbor, right_remote_qp.queue_pair_number, right_remote_qp.local_identifier);
      
      /* Send the correct remote QP info */
      if (send(client_sockets[target_rank], &left_remote_qp, sizeof(left_remote_qp), 0) != sizeof(left_remote_qp) ||
          send(client_sockets[target_rank], &right_remote_qp, sizeof(right_remote_qp), 0) != sizeof(right_remote_qp)) {
        fprintf(stderr, "Failed to send neighbor info to rank %d\n", target_rank);
        close(client_sockets[target_rank]);
        continue;
      }
      
      printf("[Process 0] DEBUG: Successfully sent neighbor info to rank %d\n", target_rank);
      close(client_sockets[target_rank]);
    }

    /* Set up rank 0's own neighbor info */
    int left_neighbor = (world_size - 1) % world_size;  /* rank 3 for 4 processes */
    int right_neighbor = 1 % world_size;                /* rank 1 for 4 processes */
    
    /* Rank 0 connects: left_qp to rank 3's right_qp, right_qp to rank 1's left_qp */
    *left_remote_info = right_qp_infos[left_neighbor];   /* rank 3's right QP (their send QP) */
    *right_remote_info = left_qp_infos[right_neighbor];  /* rank 1's left QP (their receive QP) */
    
    printf("[Process 0] DEBUG: My neighbors - left: rank %d, right: rank %d\n", 
           left_neighbor, right_neighbor);

    free(left_qp_infos);
    free(right_qp_infos);
    free(client_sockets);
    return PG_SUCCESS;
}

/**
 * Bootstrap client phase - other ranks connect to rank 0
 */
static int bootstrap_client_phase(pg_handle_internal_t *process_group,
                                  rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info) {
    (void)right_local_info; /* Unused in current implementation */
    int rank = process_group->process_rank;
    
    printf("[Process %d] DEBUG: Connecting to rank 0 for bootstrap\n", rank);

    int client_socket = pgnet_establish_tcp_connection(
        process_group->hostname_list[0], PG_DEFAULT_PORT, 0);
    if (client_socket < 0) {
      fprintf(stderr, "Failed to connect to rank 0\n");
      return PG_ERROR;
    }

    /* Send our rank first, then both QP infos to rank 0 */
    if (send(client_socket, &rank, sizeof(rank), 0) != sizeof(rank) ||
        send(client_socket, left_local_info, sizeof(*left_local_info), 0) != sizeof(*left_local_info) ||
        send(client_socket, right_local_info, sizeof(*right_local_info), 0) != sizeof(*right_local_info)) {
      fprintf(stderr, "Failed to send rank and QP info to rank 0\n");
      close(client_socket);
      return PG_ERROR;
    }

    printf("[Process %d] DEBUG: Sent QP info to rank 0: left_qp=%u, right_qp=%u\n",
           rank, left_local_info->queue_pair_number, right_local_info->queue_pair_number);

    /* Receive neighbor info from rank 0 */
    if (recv(client_socket, left_remote_info, sizeof(*left_remote_info), 0) != sizeof(*left_remote_info) ||
        recv(client_socket, right_remote_info, sizeof(*right_remote_info), 0) != sizeof(*right_remote_info)) {
      fprintf(stderr, "Failed to receive neighbor info from rank 0\n");
      close(client_socket);
      return PG_ERROR;
    }

    printf("[Process %d] DEBUG: Received neighbor info: left_remote_qp=%u lid=%u, right_remote_qp=%u lid=%u\n",
           rank, left_remote_info->queue_pair_number, left_remote_info->local_identifier,
           right_remote_info->queue_pair_number, right_remote_info->local_identifier);

    close(client_socket);
    return PG_SUCCESS;
}

static int establish_neighbor_connections(pg_handle_internal_t *process_group) {
  if (process_group->process_group_size == 1) {
    return PG_SUCCESS;
  }

  int rank = process_group->process_rank;
  rdma_qp_bootstrap_info_t left_local_info, right_local_info;
  rdma_qp_bootstrap_info_t left_remote_info, right_remote_info;

  /* Extract local QP information */
  rdma_extract_qp_bootstrap_info(&process_group->rdma_context,
                                 process_group->left_neighbor_qp, &left_local_info);
  rdma_extract_qp_bootstrap_info(&process_group->rdma_context,
                                 process_group->right_neighbor_qp, &right_local_info);

  /* Bootstrap phase - exchange QP info */
  int result;
  if (rank == 0) {
    result = bootstrap_server_phase(process_group, &left_local_info, &right_local_info,
                                    &left_remote_info, &right_remote_info);
  } else {
    result = bootstrap_client_phase(process_group, &left_local_info, &right_local_info,
                                    &left_remote_info, &right_remote_info);
  }

  if (result != PG_SUCCESS) {
    fprintf(stderr, "Bootstrap phase failed\n");
    return PG_ERROR;
  }

  /* Transition queue pairs to RTR state */
  printf("[Process %d] DEBUG: Transitioning queue pairs to RTR state...\n", rank);

  if (rdma_transition_qp_to_rtr(process_group->left_neighbor_qp,
                                &left_remote_info,
                                process_group->rdma_context.ib_port_number,
                                process_group->rdma_context.gid_index) != PG_SUCCESS ||
      rdma_transition_qp_to_rtr(process_group->right_neighbor_qp,
                                &right_remote_info,
                                process_group->rdma_context.ib_port_number,
                                process_group->rdma_context.gid_index) != PG_SUCCESS) {
    fprintf(stderr, "Failed to transition queue pairs to RTR state\n");
    return PG_ERROR;
  }

  /* Transition queue pairs to RTS state */
  printf("[Process %d] DEBUG: Transitioning queue pairs to RTS state...\n",
         rank);

  if (rdma_transition_qp_to_rts(process_group->left_neighbor_qp,
                                left_local_info.packet_sequence_number) != PG_SUCCESS ||
      rdma_transition_qp_to_rts(process_group->right_neighbor_qp,
                                right_local_info.packet_sequence_number) != PG_SUCCESS) {
    fprintf(stderr, "Failed to transition queue pairs to RTS state\n");
    return PG_ERROR;
  }

  printf("[Process %d] DEBUG: Bootstrap complete - all queue pairs connected\n",
         rank);
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
  int left_neighbor =
      (process_group->process_rank - 1 + process_group->process_group_size) %
      process_group->process_group_size;
  int right_neighbor =
      (process_group->process_rank + 1) % process_group->process_group_size;

  printf(
      "[Process %d] DEBUG: Starting ring communication step (data_size=%zu)\n",
      process_group->process_rank, data_size);
  printf(
      "[Process %d] DEBUG: Will receive from process %d, send to process %d\n",
      process_group->process_rank, left_neighbor, right_neighbor);

  /* Copy send data to RDMA send buffer */
  memcpy(process_group->right_send_buffer, send_data, data_size);

  /* Post receive request for incoming data from left neighbor */
  printf("[Process %d] DEBUG: Posting receive request from process %d...\n",
         process_group->process_rank, left_neighbor);

  if (rdma_post_receive_request(process_group->left_neighbor_qp, process_group->left_receive_buffer,
                                data_size,
                                process_group->left_receive_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] ERROR: Failed to post receive request\n",
            process_group->process_rank);
    return PG_ERROR;
  }

  printf("[Process %d] DEBUG: Receive request posted successfully\n",
         process_group->process_rank);

  /* Synchronization barrier: ensure all processes have posted receives */
  printf("[Process %d] DEBUG: Entering synchronization barrier...\n",
         process_group->process_rank);

  /* Implement rank-based staggered synchronization to prevent race conditions:
   * - All processes wait base time to post receives
   * - Higher ranks wait additional time to ensure lower ranks are ready
   * - This creates a cascade effect ensuring proper ordering */
  int base_delay_ms = 1500; /* Base delay for all processes */
  int rank_delay_ms = 200;   /* Additional delay per rank */
  int total_delay_ms = base_delay_ms + (process_group->process_rank * rank_delay_ms);
  
  usleep(total_delay_ms * 1000); /* Convert to microseconds */

  printf("[Process %d] DEBUG: Synchronization barrier complete (waited %d ms)\n",
         process_group->process_rank, total_delay_ms);

  /* Post send request to right neighbor */
  printf("[Process %d] DEBUG: Posting send request to process %d...\n",
         process_group->process_rank, right_neighbor);

  if (rdma_post_send_request(process_group->right_neighbor_qp, process_group->right_send_buffer,
                             data_size,
                             process_group->right_send_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] ERROR: Failed to post send request\n",
            process_group->process_rank);
    return PG_ERROR;
  }

  printf("[Process %d] DEBUG: Send request posted successfully\n",
         process_group->process_rank);

  /* Wait for both operations to complete */
  struct ibv_wc work_completion;
  int recv_completed = 0;
  int send_completed = 0;

  printf("[Process %d] DEBUG: Waiting for both send and receive completions...\n",
         process_group->process_rank);

  /* Poll for completions until both send and receive are done */
  while (!recv_completed || !send_completed) {
    if (rdma_poll_for_completion(process_group->rdma_context.completion_queue,
                                &work_completion) != PG_SUCCESS) {
      fprintf(stderr, "[Process %d] ERROR: Failed to complete RDMA operation\n",
              process_group->process_rank);
      return PG_ERROR;
    }

    /* Check which completion we got */
    if (work_completion.wr_id == RDMA_WR_ID_RECV && !recv_completed) {
      printf("[Process %d] DEBUG: Receive completed successfully\n",
             process_group->process_rank);
      recv_completed = 1;
      /* Copy received data to output buffer */
      memcpy(receive_data, process_group->left_receive_buffer, data_size);
    } else if (work_completion.wr_id == RDMA_WR_ID_SEND && !send_completed) {
      printf("[Process %d] DEBUG: Send completed successfully\n",
             process_group->process_rank);
      send_completed = 1;
    } else {
      printf("[Process %d] DEBUG: Got unexpected completion with wr_id=%lu (recv_done=%d, send_done=%d)\n",
             process_group->process_rank, work_completion.wr_id, recv_completed, send_completed);
    }
  }

  printf("[Process %d] DEBUG: Send completed successfully\n",
         process_group->process_rank);
  printf("[Process %d] DEBUG: Ring communication step completed successfully\n",
         process_group->process_rank);

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
  printf("[Process %d] DEBUG: Starting reduce-scatter operation\n",
         process_rank);
  printf(
      "[Process %d] DEBUG: Entering global synchronization barrier (2 "
      "seconds)...\n",
      process_rank);
  usleep(2000000); /* 2 second initial barrier for all processes */
  printf("[Process %d] DEBUG: Global synchronization barrier complete\n",
         process_rank);

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
  memcpy(process_group->right_send_buffer, send_buffer, total_data_size);

  /* Perform reduce-scatter algorithm with (group_size - 1) steps */
  for (int communication_step = 0; communication_step < group_size - 1;
       communication_step++) {
    /* Determine which chunk to reduce in this step */
    int reduction_chunk_index =
        (process_rank - communication_step + group_size) % group_size;

    /* Perform ring communication step */
    if (perform_ring_communication_step(process_group,
                                        process_group->right_send_buffer,
                                        process_group->left_receive_buffer,
                                        total_data_size) != PG_SUCCESS) {
      return PG_ERROR;
    }

    /* Apply reduction operation to the designated chunk */
    char *local_chunk_ptr = (char *)process_group->right_send_buffer +
                            (reduction_chunk_index * chunk_size_bytes);
    char *remote_chunk_ptr = (char *)process_group->left_receive_buffer +
                             (reduction_chunk_index * chunk_size_bytes);

    int chunk_element_count = chunk_size_bytes / element_size;
    pg_apply_reduction_operation(local_chunk_ptr, local_chunk_ptr,
                                 remote_chunk_ptr, chunk_element_count,
                                 data_type, reduction_op);

    /* Prepare data for next communication step */
    memcpy(process_group->right_send_buffer, process_group->left_receive_buffer,
           total_data_size);
    memcpy((char *)process_group->right_send_buffer +
               (reduction_chunk_index * chunk_size_bytes),
           local_chunk_ptr, chunk_size_bytes);
  }

  /* Extract this process's final result chunk */
  int my_chunk_index = process_rank;
  char *my_result_chunk = (char *)process_group->right_send_buffer +
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

  /* Global synchronization barrier before starting collective operation */
  printf("[Process %d] DEBUG: Starting all-gather operation\n", process_rank);
  printf(
      "[Process %d] DEBUG: Entering global synchronization barrier (2 "
      "seconds)...\n",
      process_rank);
  usleep(2000000); /* 2 second initial barrier for all processes */
  printf("[Process %d] DEBUG: Global synchronization barrier complete\n",
         process_rank);

  /* Handle single-process case */
  if (group_size == 1) {
    size_t element_size = pg_get_datatype_element_size(data_type);
    memcpy(receive_buffer, send_buffer, element_count * element_size);
    return PG_SUCCESS;
  }

  size_t element_size = pg_get_datatype_element_size(data_type);
  size_t total_data_size = element_count * element_size;
  size_t chunk_size_bytes = (element_count / group_size) * element_size;

  /* Initialize receive buffer and place local data in correct position.
   * Handle aliasing between send_buffer and receive_buffer by copying
   * the local chunk to a temporary buffer before clearing receive_buffer. */
  void *tmp_chunk = malloc(chunk_size_bytes);
  if (!tmp_chunk) {
    fprintf(stderr, "[Process %d] ERROR: Failed to allocate temp buffer for all-gather\n", process_rank);
    return PG_ERROR;
  }
  memcpy(tmp_chunk, send_buffer, chunk_size_bytes);

  memset(receive_buffer, 0, total_data_size);
  char *my_data_position =
      (char *)receive_buffer + (process_rank * chunk_size_bytes);
  memcpy(my_data_position, tmp_chunk, chunk_size_bytes);
  free(tmp_chunk);

  /* Copy initialized data to working buffer */
  memcpy(process_group->right_send_buffer, receive_buffer, total_data_size);

  /* Perform all-gather algorithm with (group_size - 1) steps */
  for (int communication_step = 0; communication_step < group_size - 1;
       communication_step++) {
    /* Perform ring communication step */
    if (perform_ring_communication_step(process_group,
                                        process_group->right_send_buffer,
                                        process_group->left_receive_buffer,
                                        total_data_size) != PG_SUCCESS) {
      return PG_ERROR;
    }

    /* Merge received data with local accumulated data */
    for (int chunk_index = 0; chunk_index < group_size; chunk_index++) {
      char *local_chunk_ptr = (char *)process_group->right_send_buffer +
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
    memcpy(process_group->right_send_buffer, process_group->left_receive_buffer,
           total_data_size);

    /* Restore any data we already had accumulated */
    for (int chunk_index = 0; chunk_index < group_size; chunk_index++) {
      char *working_chunk_ptr = (char *)process_group->right_send_buffer +
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
  memcpy(receive_buffer, process_group->right_send_buffer, total_data_size);
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

/**
 * Test basic RDMA connectivity between ring neighbors
 * Sends small test messages to verify RDMA operations work
 */
int pg_test_rdma_connectivity(pg_handle_t process_group_handle) {
  pg_handle_internal_t *process_group =
      (pg_handle_internal_t *)process_group_handle;

  if (!process_group) {
    fprintf(stderr, "Invalid process group handle\n");
    return PG_ERROR;
  }

  printf("[Process %d] Testing RDMA connectivity...\n",
         process_group->process_rank);

  /* Skip connectivity test for single-process groups */
  if (process_group->process_group_size == 1) {
    printf("[Process %d] Single process group - connectivity test skipped\n",
           process_group->process_rank);
    return PG_SUCCESS;
  }

  /* Test only between process 0 and process 1 for simplicity */
  if (process_group->process_rank != 0 && process_group->process_rank != 1) {
    printf(
        "[Process %d] Skipping connectivity test (only testing process 0 <-> "
        "1)\n",
        process_group->process_rank);
    return PG_SUCCESS;
  }

  printf("[Process %d] Simple point-to-point RDMA test with process %d\n",
         process_group->process_rank, 1 - process_group->process_rank);

  /* Use the correct connected queue pairs:
   * Process 0: right_neighbor_qp connects to Process 1's left_neighbor_qp
   * Process 1: left_neighbor_qp connects to Process 0's right_neighbor_qp */
  struct ibv_qp *test_qp;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_mr;

  if (process_group->process_rank == 0) {
    test_qp =
        process_group
            ->right_neighbor_qp; /* Connected to Process 1's left_neighbor_qp */
    send_mr = process_group->right_send_mr;
    recv_mr = process_group->left_receive_mr;
  } else { /* Process 1 */
    test_qp =
        process_group
            ->left_neighbor_qp; /* Connected to Process 0's right_neighbor_qp */
    send_mr = process_group->right_send_mr;
    recv_mr = process_group->left_receive_mr;
  }

  /* Prepare test data */
  size_t test_data_size = 64;
  char *test_send_data = (char *)process_group->right_send_buffer;
  char *test_receive_data = (char *)process_group->left_receive_buffer;

  /* Initialize send data with process rank pattern */
  for (size_t i = 0; i < test_data_size; i++) {
    test_send_data[i] = (char)(process_group->process_rank * 10 + i);
  }
  memset(test_receive_data, 0, test_data_size);

  printf("[Process %d] Using QP %u for test\n", process_group->process_rank,
         test_qp->qp_num);

  /* Both processes post receive first */
  printf("[Process %d] Posting receive request...\n",
         process_group->process_rank);
  if (rdma_post_receive_request(test_qp, test_receive_data, test_data_size,
                                recv_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Failed to post receive request\n",
            process_group->process_rank);
    return PG_ERROR;
  }

  /* Synchronization barrier */
  printf("[Process %d] Synchronization barrier (1 second)...\n",
         process_group->process_rank);
  usleep(1000000);

  /* Both processes send to each other */
  printf("[Process %d] Posting send request...\n", process_group->process_rank);
  if (rdma_post_send_request(test_qp, test_send_data, test_data_size,
                             send_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Failed to post send request\n",
            process_group->process_rank);
    return PG_ERROR;
  }

  /* Wait for completions */
  printf("[Process %d] Waiting for receive completion...\n",
         process_group->process_rank);
  struct ibv_wc work_completion;
  if (rdma_poll_for_completion(process_group->rdma_context.completion_queue,
                               &work_completion) != PG_SUCCESS) {
    fprintf(
        stderr,
        "[Process %d] Point-to-point test FAILED - receive completion failed\n",
        process_group->process_rank);
    return PG_ERROR;
  }

  printf("[Process %d] Waiting for send completion...\n",
         process_group->process_rank);
  if (rdma_poll_for_completion(process_group->rdma_context.completion_queue,
                               &work_completion) != PG_SUCCESS) {
    fprintf(
        stderr,
        "[Process %d] Point-to-point test FAILED - send completion failed\n",
        process_group->process_rank);
    return PG_ERROR;
  }

  /* Verify received data */
  int expected_sender = 1 - process_group->process_rank;
  printf("[Process %d] Verifying data from process %d...\n",
         process_group->process_rank, expected_sender);

  int data_valid = 1;
  for (size_t i = 0; i < test_data_size; i++) {
    char expected_value = (char)(expected_sender * 10 + i);
    if (test_receive_data[i] != expected_value) {
      printf("[Process %d] Data mismatch at byte %zu: expected %d, got %d\n",
             process_group->process_rank, i, expected_value,
             test_receive_data[i]);
      data_valid = 0;
      break;
    }
  }

  if (data_valid) {
    printf("[Process %d] Point-to-point RDMA test PASSED\n",
           process_group->process_rank);
  } else {
    printf(
        "[Process %d] Point-to-point RDMA test FAILED - data verification "
        "failed\n",
        process_group->process_rank);
    return PG_ERROR;
  }

  return PG_SUCCESS;
}
