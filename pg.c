#define _GNU_SOURCE

#include "pg.h"

#include <arpa/inet.h>
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
  process_group->left_send_buffer = malloc(process_group->total_buffer_size_bytes);
  process_group->left_receive_buffer = malloc(process_group->total_buffer_size_bytes);
  process_group->right_send_buffer = malloc(process_group->total_buffer_size_bytes);
  process_group->right_receive_buffer = malloc(process_group->total_buffer_size_bytes);

  /* Check if all allocations succeeded */
  if (!process_group->left_send_buffer || !process_group->left_receive_buffer || !process_group->right_send_buffer ||
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
      &process_group->rdma_context, process_group->left_send_buffer, process_group->total_buffer_size_bytes);

  process_group->left_receive_mr = rdma_register_memory_buffer(
      &process_group->rdma_context, process_group->left_receive_buffer, process_group->total_buffer_size_bytes);

  process_group->right_send_mr = rdma_register_memory_buffer(
      &process_group->rdma_context, process_group->right_send_buffer, process_group->total_buffer_size_bytes);

  process_group->right_receive_mr = rdma_register_memory_buffer(
      &process_group->rdma_context, process_group->right_receive_buffer, process_group->total_buffer_size_bytes);

  /* Check if all registrations succeeded */
  if (!process_group->left_send_mr || !process_group->left_receive_mr || !process_group->right_send_mr ||
      !process_group->right_receive_mr) {
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
static int create_ring_topology_queue_pairs(pg_handle_internal_t *process_group) {
  /* Create queue pairs for left and right neighbors */
  if (rdma_create_queue_pair(&process_group->rdma_context, &process_group->left_neighbor_qp) != PG_SUCCESS) {
    fprintf(stderr, "Failed to create left neighbor queue pair\n");
    return PG_ERROR;
  }

  if (rdma_create_queue_pair(&process_group->rdma_context, &process_group->right_neighbor_qp) != PG_SUCCESS) {
    fprintf(stderr, "Failed to create right neighbor queue pair\n");
    return PG_ERROR;
  }

  /* Transition queue pairs to INIT state */
  if (rdma_transition_qp_to_init(process_group->left_neighbor_qp, process_group->rdma_context.ib_port_number) !=
      PG_SUCCESS) {
    fprintf(stderr, "Failed to initialize left neighbor queue pair\n");
    return PG_ERROR;
  }

  if (rdma_transition_qp_to_init(process_group->right_neighbor_qp, process_group->rdma_context.ib_port_number) !=
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
static int bootstrap_server_phase(pg_handle_internal_t *process_group, rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info);

static int bootstrap_client_phase(pg_handle_internal_t *process_group, rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info);

/**
 * Bootstrap server phase - rank 0 collects QP info from all ranks
 */
static int bootstrap_server_phase(pg_handle_internal_t *process_group, rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info) {
  (void)right_local_info; /* Unused in current implementation */
  int world_size = process_group->process_group_size;

  /* Acting as bootstrap server (quiet) */

  /* Create server socket directly instead of using
   * pgnet_establish_tcp_connection */
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

  if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
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
      printf("[Process 0] DEBUG: Bootstrap timeout - only %d/%d ranks connected\n", connected_count, world_size);
      break;
    }

    /* Use select to wait for connections with timeout */
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(server_socket, &read_fds);

    struct timeval timeout;
    timeout.tv_sec = 5; /* 5 second select timeout */
    timeout.tv_usec = 0;

    /* Waiting for connections */

    int select_result = select(server_socket + 1, &read_fds, NULL, NULL, &timeout);
    if (select_result < 0) {
      perror("select() failed");
      break;
    } else if (select_result == 0) {
      printf("[Process 0] DEBUG: select() timeout, continuing...\n");
      continue;
    }

    if (FD_ISSET(server_socket, &read_fds)) {
      struct sockaddr_in client_addr;
      socklen_t addr_len = sizeof(client_addr); /* Reinitialize addr_len before each accept */

      int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &addr_len);
      if (client_socket < 0) {
        perror("accept() failed");
        continue;
      }

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

  /* All ranks connected, distributing neighbor info */

  /* Distribute neighbor info to each rank using existing connections */
  for (int target_rank = 1; target_rank < world_size; target_rank++) {
    if (client_sockets[target_rank] < 0) continue;

    /* Calculate neighbors for target rank */
    int left_neighbor = (target_rank - 1 + world_size) % world_size;
    int right_neighbor = (target_rank + 1) % world_size;

    /* FINAL FIX: For ring topology, target_rank needs to connect:
     * - left_neighbor_qp (for receiving FROM left) connects to left_neighbor's
     * SENDING QP (their right_qp)
     * - right_neighbor_qp (for sending TO right) connects to right_neighbor's
     * RECEIVING QP (their left_qp)
     *
     * The QP semantics are:
     * - left_neighbor_qp is for receiving FROM left neighbor → connect to their
     * right_qp (send QP)
     * - right_neighbor_qp is for sending TO right neighbor → connect to their
     * left_qp (receive QP)
     */
    rdma_qp_bootstrap_info_t left_remote_qp = right_qp_infos[left_neighbor];  /* left neighbor's right QP (their send
                                                                                 QP) */
    rdma_qp_bootstrap_info_t right_remote_qp = left_qp_infos[right_neighbor]; /* right neighbor's left QP (their
                                                                                 receive QP) */

    /* Send the correct remote QP info */
    if (send(client_sockets[target_rank], &left_remote_qp, sizeof(left_remote_qp), 0) != sizeof(left_remote_qp) ||
        send(client_sockets[target_rank], &right_remote_qp, sizeof(right_remote_qp), 0) != sizeof(right_remote_qp)) {
      fprintf(stderr, "Failed to send neighbor info to rank %d\n", target_rank);
      close(client_sockets[target_rank]);
      continue;
    }

    close(client_sockets[target_rank]);
  }

  /* Set up rank 0's own neighbor info */
  int left_neighbor = (world_size - 1) % world_size; /* rank 3 for 4 processes */
  int right_neighbor = 1 % world_size;               /* rank 1 for 4 processes */

  /* Rank 0 connects: left_qp to rank 3's right_qp, right_qp to rank 1's left_qp
   */
  *left_remote_info = right_qp_infos[left_neighbor];  /* rank 3's right QP (their send QP) */
  *right_remote_info = left_qp_infos[right_neighbor]; /* rank 1's left QP (their receive QP) */

  free(left_qp_infos);
  free(right_qp_infos);
  free(client_sockets);
  return PG_SUCCESS;
}

/**
 * Bootstrap client phase - other ranks connect to rank 0
 */
static int bootstrap_client_phase(pg_handle_internal_t *process_group, rdma_qp_bootstrap_info_t *left_local_info,
                                  rdma_qp_bootstrap_info_t *right_local_info,
                                  rdma_qp_bootstrap_info_t *left_remote_info,
                                  rdma_qp_bootstrap_info_t *right_remote_info) {
  (void)right_local_info; /* Unused in current implementation */
  int rank = process_group->process_rank;

  /* Connecting to rank 0 for bootstrap (quiet) */

  int client_socket = pgnet_establish_tcp_connection(process_group->hostname_list[0], PG_DEFAULT_PORT, 0);
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

  /* Receive neighbor info from rank 0 */
  if (recv(client_socket, left_remote_info, sizeof(*left_remote_info), 0) != sizeof(*left_remote_info) ||
      recv(client_socket, right_remote_info, sizeof(*right_remote_info), 0) != sizeof(*right_remote_info)) {
    fprintf(stderr, "Failed to receive neighbor info from rank 0\n");
    close(client_socket);
    return PG_ERROR;
  }

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
  rdma_extract_qp_bootstrap_info(&process_group->rdma_context, process_group->left_neighbor_qp, &left_local_info);
  rdma_extract_qp_bootstrap_info(&process_group->rdma_context, process_group->right_neighbor_qp, &right_local_info);

  /* Attach exposed buffer metadata for neighbors */
  left_local_info.exposed_buffer_addr = (uint64_t)(uintptr_t)process_group->left_send_buffer;
  left_local_info.exposed_buffer_rkey = process_group->left_send_mr ? process_group->left_send_mr->rkey : 0;
  left_local_info.exposed_buffer_bytes = process_group->total_buffer_size_bytes;
  left_local_info.reserved = 0;

  right_local_info.exposed_buffer_addr = (uint64_t)(uintptr_t)process_group->right_send_buffer;
  right_local_info.exposed_buffer_rkey = process_group->right_send_mr ? process_group->right_send_mr->rkey : 0;
  right_local_info.exposed_buffer_bytes = process_group->total_buffer_size_bytes;
  right_local_info.reserved = 0;

  /* Bootstrap phase - exchange QP info */
  int result;
  if (rank == 0) {
    result = bootstrap_server_phase(process_group, &left_local_info, &right_local_info, &left_remote_info,
                                    &right_remote_info);
  } else {
    result = bootstrap_client_phase(process_group, &left_local_info, &right_local_info, &left_remote_info,
                                    &right_remote_info);
  }

  if (result != PG_SUCCESS) {
    fprintf(stderr, "Bootstrap phase failed\n");
    return PG_ERROR;
  }

  /* Cache neighbor exposed buffer information */
  process_group->left_remote_base = left_remote_info.exposed_buffer_addr;
  process_group->left_remote_rkey = left_remote_info.exposed_buffer_rkey;
  process_group->right_remote_base = right_remote_info.exposed_buffer_addr;
  process_group->right_remote_rkey = right_remote_info.exposed_buffer_rkey;

  /* Transition queue pairs to RTR state */

  if (rdma_transition_qp_to_rtr(process_group->left_neighbor_qp, &left_remote_info,
                                process_group->rdma_context.ib_port_number,
                                process_group->rdma_context.gid_index) != PG_SUCCESS ||
      rdma_transition_qp_to_rtr(process_group->right_neighbor_qp, &right_remote_info,
                                process_group->rdma_context.ib_port_number,
                                process_group->rdma_context.gid_index) != PG_SUCCESS) {
    fprintf(stderr, "Failed to transition queue pairs to RTR state\n");
    return PG_ERROR;
  }

  /* Transition queue pairs to RTS state */

  if (rdma_transition_qp_to_rts(process_group->left_neighbor_qp, left_local_info.packet_sequence_number) !=
          PG_SUCCESS ||
      rdma_transition_qp_to_rts(process_group->right_neighbor_qp, right_local_info.packet_sequence_number) !=
          PG_SUCCESS) {
    fprintf(stderr, "Failed to transition queue pairs to RTS state\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

/**
 * Initialize remote memory region information for zero-copy operations
 */
static int initialize_remote_memory_info(pg_handle_internal_t *process_group) {
  int group_size = process_group->process_group_size;
  int process_rank = process_group->process_rank;

  /* Allocate arrays for remote memory information */
  process_group->remote_buffer_addrs = calloc(group_size, sizeof(uint64_t));
  process_group->remote_buffer_rkeys = calloc(group_size, sizeof(uint32_t));

  if (!process_group->remote_buffer_addrs || !process_group->remote_buffer_rkeys) {
    fprintf(stderr, "[Process %d] ERROR: Failed to allocate remote memory info arrays\n", process_rank);
    free(process_group->remote_buffer_addrs);
    free(process_group->remote_buffer_rkeys);
    return PG_ERROR;
  }

  /* Set buffer size for remote operations */
  process_group->remote_buffer_size = process_group->total_buffer_size_bytes;
  process_group->final_recv_mr = process_group->right_receive_mr;

  /* Use right_receive_buffer as the target buffer for remote writes */
  uint64_t local_buffer_addr = (uint64_t)(uintptr_t)process_group->right_receive_buffer;
  uint32_t local_buffer_rkey = process_group->right_receive_mr->rkey;

  if (pg_exchange_allgather_mr(process_group, local_buffer_addr, local_buffer_rkey,
                               process_group->remote_buffer_size) != PG_SUCCESS) {
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

/*
 * =============================================================================
 * Process Group Lifecycle Management Implementation
 * =============================================================================
 */

int pg_initialize(const char *server_list_string, pg_handle_t *process_group_handle) {
  PG_CHECK_NULL(server_list_string, "Server list string is NULL");
  PG_CHECK_NULL(process_group_handle, "Process group handle pointer is NULL");

  /* Allocate and initialize process group structure */
  pg_handle_internal_t *process_group = calloc(1, sizeof(pg_handle_internal_t));
  if (!process_group) {
    fprintf(stderr, "Failed to allocate process group structure\n");
    return PG_ERROR;
  }

  /* Parse hostname list and determine process topology */
  process_group->hostname_list = pgnet_parse_hostname_list(server_list_string, &process_group->process_group_size);
  if (!process_group->hostname_list) {
    fprintf(stderr, "Failed to parse hostname list\n");
    free(process_group);
    return PG_ERROR;
  }

  /* Determine this process's rank within the group */
  process_group->process_rank =
      pgnet_determine_process_rank(process_group->hostname_list, process_group->process_group_size);
  if (process_group->process_rank < 0) {
    fprintf(stderr, "Could not determine process rank\n");
    pgnet_free_hostname_list(process_group->hostname_list, process_group->process_group_size);
    free(process_group);
    return PG_ERROR;
  }

  /* Initialize RDMA device context */
  if (rdma_initialize_context(&process_group->rdma_context, NULL) != PG_SUCCESS) {
    fprintf(stderr, "Failed to initialize RDMA context\n");
    pgnet_free_hostname_list(process_group->hostname_list, process_group->process_group_size);
    free(process_group);
    return PG_ERROR;
  }

  /* Configure buffer sizes */
  process_group->total_buffer_size_bytes = PG_BUFFER_SIZE_BYTES;
  process_group->chunk_size_bytes = PG_CHUNK_SIZE_BYTES;

  /* Configure pipelining parameters (can be overridden by environment variables) */
  const char *eager_max_env = getenv("PG_EAGER_MAX");
  const char *chunk_bytes_env = getenv("PG_CHUNK_BYTES");
  const char *inflight_env = getenv("PG_INFLIGHT");

  process_group->eager_max = eager_max_env ? atoi(eager_max_env) : PG_DEFAULT_EAGER_MAX;
  process_group->chunk_bytes = chunk_bytes_env ? atoi(chunk_bytes_env) : PG_DEFAULT_CHUNK_BYTES;
  process_group->inflight = inflight_env ? atoi(inflight_env) : PG_DEFAULT_INFLIGHT;

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

  /* Initialize remote memory region information for zero-copy operations */
  if (initialize_remote_memory_info(process_group) != PG_SUCCESS) {
    pg_cleanup(process_group);
    return PG_ERROR;
  }

  /* Return initialized handle to caller */
  *process_group_handle = process_group;
  return PG_SUCCESS;
}

int pg_cleanup(pg_handle_t process_group_handle) {
  pg_handle_internal_t *process_group = (pg_handle_internal_t *)process_group_handle;
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
  if (process_group->final_recv_mr && process_group->final_recv_mr != process_group->right_receive_mr) {
    rdma_deregister_memory_buffer(process_group->final_recv_mr);
  }
  process_group->final_recv_mr = NULL;

  /* Clean up RDMA context */
  rdma_cleanup_context(&process_group->rdma_context);

  /* Free communication buffers */
  free(process_group->left_send_buffer);
  free(process_group->left_receive_buffer);
  free(process_group->right_send_buffer);
  free(process_group->right_receive_buffer);

  /* Free remote memory region arrays */
  free(process_group->remote_buffer_addrs);
  free(process_group->remote_buffer_rkeys);

  /* Free hostname list */
  pgnet_free_hostname_list(process_group->hostname_list, process_group->process_group_size);

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
static int perform_ring_communication_step(pg_handle_internal_t *process_group, void *send_data, void *receive_data,
                                           size_t data_size) {
  /* Copy send data to RDMA send buffer only when source differs */
  if (send_data != process_group->right_send_buffer) {
    memcpy(process_group->right_send_buffer, send_data, data_size);
  }

  /* Post receive request for incoming data from left neighbor */
  if (rdma_post_receive_request(process_group->left_neighbor_qp, process_group->left_receive_buffer, data_size,
                                process_group->left_receive_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] ERROR: Failed to post receive request\n", process_group->process_rank);
    return PG_ERROR;
  }

  /* Post send request to right neighbor */
  if (rdma_post_send_request(process_group->right_neighbor_qp, process_group->right_send_buffer, data_size,
                             process_group->right_send_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] ERROR: Failed to post send request\n", process_group->process_rank);
    return PG_ERROR;
  }

  /* Wait for both operations to complete */
  struct ibv_wc work_completion;
  int recv_completed = 0;
  int send_completed = 0;

  /* Poll for completions until both send and receive are done */
  while (!recv_completed || !send_completed) {
    if (rdma_poll_for_completion(process_group->rdma_context.completion_queue, &work_completion) != PG_SUCCESS) {
      fprintf(stderr, "[Process %d] ERROR: Failed to complete RDMA operation\n", process_group->process_rank);
      return PG_ERROR;
    }

    /* Check which completion we got */
    if (work_completion.wr_id == 1 && !recv_completed) { /* RECV completion */
      recv_completed = 1;
      /* Copy received data to output buffer */
      if (receive_data != process_group->left_receive_buffer) {
        memcpy(receive_data, process_group->left_receive_buffer, data_size);
      }
    } else if (work_completion.wr_id == 2 && !send_completed) { /* SEND completion */
      send_completed = 1;
    } else {
      /* Ignore unrelated completions */
    }
  }

  return PG_SUCCESS;
}

/*
 * =============================================================================
 * Collective Communication Operations Implementation
 * =============================================================================
 */

typedef struct pg_control_msg {
  uint32_t step;
  uint32_t chunk_index;
} pg_control_msg_t;

static int pg_post_control_receive(pg_handle_internal_t *process_group, pg_control_msg_t *slot, uint32_t step);
static int pg_post_control_send(pg_handle_internal_t *process_group, uint32_t step, uint32_t chunk_index);

/**
 * Zero-copy reduce-scatter using RDMA write with SEND/RECV synchronization
 */
static int pg_reduce_scatter_zero_copy(pg_handle_internal_t *process_group, void *send_buffer, void *receive_buffer,
                                       int element_count, pg_datatype_t data_type, pg_operation_t reduction_op) {
  int group_size = process_group->process_group_size;
  int process_rank = process_group->process_rank;

  if (group_size <= 0) {
    return PG_ERROR;
  }

  if (group_size == 1) {
    size_t element_size = pg_get_datatype_element_size(data_type);
    memcpy(receive_buffer, send_buffer, (size_t)element_count * element_size);
    return PG_SUCCESS;
  }

  if (!process_group->remote_buffer_addrs || !process_group->remote_buffer_rkeys) {
    return PG_ERROR;
  }

  size_t element_size = pg_get_datatype_element_size(data_type);
  size_t total_bytes = (size_t)element_count * element_size;
  if (total_bytes == 0) {
    return PG_SUCCESS;
  }

  size_t chunk_bytes = (size_t)(element_count / group_size) * element_size;
  if (chunk_bytes == 0) {
    return PG_ERROR;
  }

  size_t required_bytes = chunk_bytes * (size_t)group_size;
  if (required_bytes > process_group->remote_buffer_size) {
    return PG_ERROR;
  }

  memcpy(process_group->right_send_buffer, send_buffer, total_bytes);

  char *local_work_buffer = (char *)process_group->right_send_buffer;
  char *incoming_buffer = (char *)process_group->right_receive_buffer;
  pg_control_msg_t *control_slots = (pg_control_msg_t *)process_group->left_receive_buffer;

  const int right_neighbor = (process_rank + 1) % group_size;
  const int steps = group_size - 1;
  int chunk_elements = (int)(chunk_bytes / element_size);

  for (int step = 0; step < steps; ++step) {
    int send_chunk_index = (process_rank - step + group_size) % group_size;
    int recv_chunk_index = (process_rank - step - 1 + group_size) % group_size;

    size_t send_offset = (size_t)send_chunk_index * chunk_bytes;
    size_t recv_offset = (size_t)recv_chunk_index * chunk_bytes;

    pg_control_msg_t *control_slot = &control_slots[step];
    if (pg_post_control_receive(process_group, control_slot, (uint32_t)step) != PG_SUCCESS) {
      return PG_ERROR;
    }

    uint64_t remote_addr = process_group->remote_buffer_addrs[right_neighbor] + send_offset;
    uint32_t remote_rkey = process_group->remote_buffer_rkeys[right_neighbor];

    if (rdma_post_write_request(process_group->right_neighbor_qp, local_work_buffer + send_offset, chunk_bytes,
                                process_group->right_send_mr, remote_addr, remote_rkey, (uint64_t)(0xABC00000 | step),
                                1) != 0) {
      return PG_ERROR;
    }

    if (pg_post_control_send(process_group, (uint32_t)step, (uint32_t)send_chunk_index) != PG_SUCCESS) {
      return PG_ERROR;
    }

    int write_done = 0;
    int send_done = 0;
    int control_done = 0;

    while (!write_done || !send_done || !control_done) {
      struct ibv_wc work_completion;
      if (rdma_poll_for_completion(process_group->rdma_context.completion_queue, &work_completion) != PG_SUCCESS) {
        return PG_ERROR;
      }

      if (work_completion.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[Process %d] RDMA completion error (opcode %d, status %d)\n", process_rank,
                work_completion.opcode, work_completion.status);
        return PG_ERROR;
      }

      if (work_completion.opcode == IBV_WC_RDMA_WRITE) {
        write_done = 1;
      } else if (work_completion.opcode == IBV_WC_SEND) {
        send_done = 1;
      } else if (work_completion.opcode == IBV_WC_RECV) {
        if (WRID_KIND(work_completion.wr_id) == WRK_CTRL && WRID_CTRL_STEP(work_completion.wr_id) == (uint32_t)step) {
          control_done = 1;
        }
      }
    }

    int remote_chunk_index = (int)control_slot->chunk_index;
    size_t remote_offset = (size_t)remote_chunk_index * chunk_bytes;

    char *incoming_chunk = incoming_buffer + remote_offset;
    char *local_chunk = local_work_buffer + recv_offset;

    pg_apply_reduction_operation(local_chunk, local_chunk, incoming_chunk, chunk_elements, data_type, reduction_op);
  }

  char *my_final_chunk = local_work_buffer + ((size_t)process_rank * chunk_bytes);
  memcpy(receive_buffer, my_final_chunk, chunk_bytes);

  return PG_SUCCESS;
}

/**
 * Pipelined reduce-scatter with overlapped communication and computation
 */
static int pg_reduce_scatter_pipelined(pg_handle_internal_t *process_group, void *send_buffer, void *receive_buffer,
                                       int element_count, pg_datatype_t data_type, pg_operation_t reduction_op) {
  int group_size = process_group->process_group_size;
  int process_rank = process_group->process_rank;
  size_t element_size = pg_get_datatype_element_size(data_type);
  size_t total_data_size = element_count * element_size;
  size_t chunk_size_bytes = (element_count / group_size) * element_size;

  if (total_data_size == 0) {
    return PG_SUCCESS;
  }

  /* Calculate pipelining parameters */
  size_t pipeline_candidate = PG_MAX(process_group->chunk_bytes, chunk_size_bytes);
  size_t pipeline_chunk_size = PG_MIN(pipeline_candidate, total_data_size);
  if (pipeline_chunk_size == 0) {
    pipeline_chunk_size = total_data_size;
  }
  int num_pipeline_chunks = (int)((total_data_size + pipeline_chunk_size - 1) / pipeline_chunk_size);
  int max_inflight = PG_MIN(process_group->inflight, num_pipeline_chunks);
  if (max_inflight <= 0) {
    max_inflight = 1;
  }

  /* Copy input data to working buffer */
  memcpy(process_group->right_send_buffer, send_buffer, total_data_size);

  /* Temp buffer to preserve reduced chunk across buffer swaps */
  void *reduced_chunk_tmp = malloc(chunk_size_bytes);
  if (!reduced_chunk_tmp) {
    fprintf(stderr, "[Process %d] ERROR: Failed to allocate temp buffer for reduce-scatter\n", process_rank);
    return PG_ERROR;
  }

  void *original_right_send_buffer = process_group->right_send_buffer;
  void *original_left_receive_buffer = process_group->left_receive_buffer;
  struct ibv_mr *original_right_send_mr = process_group->right_send_mr;
  struct ibv_mr *original_left_receive_mr = process_group->left_receive_mr;

  int status = PG_ERROR;

  /* Perform reduce-scatter algorithm with (group_size - 1) steps */
  for (int communication_step = 0; communication_step < group_size - 1; communication_step++) {
    int reduction_chunk_index = process_rank;

    /* Pipeline the communication for this step */
    for (int pipeline_chunk = 0; pipeline_chunk < num_pipeline_chunks; pipeline_chunk += max_inflight) {
      int chunks_this_batch = PG_MIN(max_inflight, num_pipeline_chunks - pipeline_chunk);

      /* Post receives for this batch */
      for (int i = 0; i < chunks_this_batch; i++) {
        int chunk_idx = pipeline_chunk + i;
        size_t chunk_offset = chunk_idx * pipeline_chunk_size;
        size_t this_chunk_size = PG_MIN(pipeline_chunk_size, total_data_size - chunk_offset);

        if (rdma_post_receive_request(process_group->left_neighbor_qp,
                                      (char *)process_group->left_receive_buffer + chunk_offset, this_chunk_size,
                                      process_group->left_receive_mr) != PG_SUCCESS) {
          goto cleanup;
        }
      }

      /* Post sends for this batch */
      for (int i = 0; i < chunks_this_batch; i++) {
        int chunk_idx = pipeline_chunk + i;
        size_t chunk_offset = chunk_idx * pipeline_chunk_size;
        size_t this_chunk_size = PG_MIN(pipeline_chunk_size, total_data_size - chunk_offset);

        if (rdma_post_send_request(process_group->right_neighbor_qp,
                                   (char *)process_group->right_send_buffer + chunk_offset, this_chunk_size,
                                   process_group->right_send_mr) != PG_SUCCESS) {
          goto cleanup;
        }
      }

      /* Wait for all operations in this batch to complete */
      for (int i = 0; i < chunks_this_batch * 2; i++) { /* 2x for send + recv */
        struct ibv_wc work_completion;
        if (rdma_poll_for_completion(process_group->rdma_context.completion_queue, &work_completion) != PG_SUCCESS) {
          goto cleanup;
        }
      }
    }

    /* Apply reduction operation to the designated chunk */
    char *local_chunk_ptr = (char *)process_group->right_send_buffer + (reduction_chunk_index * chunk_size_bytes);
    char *remote_chunk_ptr = (char *)process_group->left_receive_buffer + (reduction_chunk_index * chunk_size_bytes);
    int chunk_element_count = (int)(chunk_size_bytes / element_size);

    /* Reduce into temp buffer to avoid being clobbered by the buffer swap */
    memcpy(reduced_chunk_tmp, local_chunk_ptr, chunk_size_bytes);
    pg_apply_reduction_operation(reduced_chunk_tmp, reduced_chunk_tmp, remote_chunk_ptr, chunk_element_count, data_type,
                                 reduction_op);

    /* Prepare data for next communication step */
    void *tmp_buffer = process_group->right_send_buffer;
    process_group->right_send_buffer = process_group->left_receive_buffer;
    process_group->left_receive_buffer = tmp_buffer;

    struct ibv_mr *tmp_mr = process_group->right_send_mr;
    process_group->right_send_mr = process_group->left_receive_mr;
    process_group->left_receive_mr = tmp_mr;

    memcpy((char *)process_group->right_send_buffer + (reduction_chunk_index * chunk_size_bytes), reduced_chunk_tmp,
           chunk_size_bytes);
  }

  /* Extract this process's final result chunk */
  int my_chunk_index = process_rank;
  char *my_result_chunk = (char *)process_group->right_send_buffer + (my_chunk_index * chunk_size_bytes);
  memcpy(receive_buffer, my_result_chunk, chunk_size_bytes);

  status = PG_SUCCESS;

cleanup:
  process_group->right_send_buffer = original_right_send_buffer;
  process_group->left_receive_buffer = original_left_receive_buffer;
  process_group->right_send_mr = original_right_send_mr;
  process_group->left_receive_mr = original_left_receive_mr;

  free(reduced_chunk_tmp);
  return status;
}

/**
 * Eager reduce-scatter implementation using SEND/RECV with memcpy for small messages
 */
static int pg_reduce_scatter_eager(pg_handle_internal_t *process_group, void *send_buffer, void *receive_buffer,
                                   int element_count, pg_datatype_t data_type, pg_operation_t reduction_op) {
  int group_size = process_group->process_group_size;
  int process_rank = process_group->process_rank;
  size_t element_size = pg_get_datatype_element_size(data_type);
  size_t total_data_size = element_count * element_size;
  size_t chunk_size_bytes = (element_count / group_size) * element_size;

  if (total_data_size == 0) {
    return PG_SUCCESS;
  }

  /* Copy input data to working buffer */
  memcpy(process_group->right_send_buffer, send_buffer, total_data_size);

  /* Temp buffer to preserve reduced chunk across buffer swaps */
  void *reduced_chunk_tmp = malloc(chunk_size_bytes);
  if (!reduced_chunk_tmp) {
    fprintf(stderr, "[Process %d] ERROR: Failed to allocate temp buffer for reduce-scatter\n", process_rank);
    return PG_ERROR;
  }

  void *original_right_send_buffer = process_group->right_send_buffer;
  void *original_left_receive_buffer = process_group->left_receive_buffer;
  struct ibv_mr *original_right_send_mr = process_group->right_send_mr;
  struct ibv_mr *original_left_receive_mr = process_group->left_receive_mr;

  int status = PG_ERROR;

  /* Perform reduce-scatter algorithm with (group_size - 1) steps */
  for (int communication_step = 0; communication_step < group_size - 1; communication_step++) {
    int reduction_chunk_index = process_rank;

    /* Perform ring communication step */
    if (perform_ring_communication_step(process_group, process_group->right_send_buffer,
                                        process_group->left_receive_buffer, total_data_size) != PG_SUCCESS) {
      goto cleanup;
    }

    /* Apply reduction operation to the designated chunk */
    char *local_chunk_ptr = (char *)process_group->right_send_buffer + (reduction_chunk_index * chunk_size_bytes);
    char *remote_chunk_ptr = (char *)process_group->left_receive_buffer + (reduction_chunk_index * chunk_size_bytes);
    int chunk_element_count = (int)(chunk_size_bytes / element_size);

    /* Reduce into temp buffer to avoid being clobbered by the buffer swap */
    memcpy(reduced_chunk_tmp, local_chunk_ptr, chunk_size_bytes);
    pg_apply_reduction_operation(reduced_chunk_tmp, reduced_chunk_tmp, remote_chunk_ptr, chunk_element_count, data_type,
                                 reduction_op);

    /* Prepare data for next communication step */
    void *tmp_buffer = process_group->right_send_buffer;
    process_group->right_send_buffer = process_group->left_receive_buffer;
    process_group->left_receive_buffer = tmp_buffer;

    struct ibv_mr *tmp_mr = process_group->right_send_mr;
    process_group->right_send_mr = process_group->left_receive_mr;
    process_group->left_receive_mr = tmp_mr;

    memcpy((char *)process_group->right_send_buffer + (reduction_chunk_index * chunk_size_bytes), reduced_chunk_tmp,
           chunk_size_bytes);
  }

  /* Extract this process's final result chunk */
  int my_chunk_index = process_rank;
  char *my_result_chunk = (char *)process_group->right_send_buffer + (my_chunk_index * chunk_size_bytes);
  memcpy(receive_buffer, my_result_chunk, chunk_size_bytes);

  status = PG_SUCCESS;

cleanup:
  process_group->right_send_buffer = original_right_send_buffer;
  process_group->left_receive_buffer = original_left_receive_buffer;
  process_group->right_send_mr = original_right_send_mr;
  process_group->left_receive_mr = original_left_receive_mr;

  free(reduced_chunk_tmp);
  return status;
}

int pg_reduce_scatter(pg_handle_t process_group_handle, void *send_buffer, void *receive_buffer, int element_count,
                      pg_datatype_t data_type, pg_operation_t reduction_op) {
  pg_handle_internal_t *process_group = (pg_handle_internal_t *)process_group_handle;

  PG_CHECK_NULL(process_group, "Process group handle is NULL");
  PG_CHECK_NULL(send_buffer, "Send buffer is NULL");
  PG_CHECK_NULL(receive_buffer, "Receive buffer is NULL");

  int group_size = process_group->process_group_size;

  /* Handle single-process case */
  if (group_size == 1) {
    size_t element_size = pg_get_datatype_element_size(data_type);
    memcpy(receive_buffer, send_buffer, element_count * element_size);
    return PG_SUCCESS;
  }

  size_t element_size = pg_get_datatype_element_size(data_type);
  size_t total_data_size = element_count * element_size;

  /* Dispatch based on message size */
  if (total_data_size <= process_group->eager_max) {
    /* Use eager path for small messages */
    return pg_reduce_scatter_eager(process_group, send_buffer, receive_buffer, element_count, data_type, reduction_op);
  }

  if (pg_reduce_scatter_zero_copy(process_group, send_buffer, receive_buffer, element_count, data_type, reduction_op) ==
      PG_SUCCESS) {
    return PG_SUCCESS;
  }

  /* Fallback to pipelined SEND/RECV implementation */
  return pg_reduce_scatter_pipelined(process_group, send_buffer, receive_buffer, element_count, data_type,
                                     reduction_op);
}

/**
 * Eager all-gather implementation using SEND/RECV with memcpy for small messages
 */
static int pg_all_gather_eager(pg_handle_internal_t *process_group, void *send_buffer, void *receive_buffer,
                               size_t total_bytes, size_t chunk_bytes) {
  int group_size = process_group->process_group_size;
  int process_rank = process_group->process_rank;

  if (chunk_bytes == 0) {
    memset(receive_buffer, 0, total_bytes);
    return PG_SUCCESS;
  }

  /* Initialize the ring buffer with our data at the correct position */
  memset(process_group->right_send_buffer, 0, total_bytes);
  char *my_chunk_position = (char *)process_group->right_send_buffer + (process_rank * chunk_bytes);
  memcpy(my_chunk_position, send_buffer, chunk_bytes);

  void *tmp_chunk = malloc(chunk_bytes);
  if (!tmp_chunk) {
    fprintf(stderr, "[Process %d] ERROR: Failed to allocate temp buffer for all-gather\n", process_rank);
    return PG_ERROR;
  }
  memmove(tmp_chunk, send_buffer, chunk_bytes);

  memset(receive_buffer, 0, total_bytes);
  char *my_data_position = (char *)receive_buffer + ((size_t)process_rank * chunk_bytes);
  memcpy(my_data_position, tmp_chunk, chunk_bytes);
  free(tmp_chunk);

  memcpy(process_group->right_send_buffer, receive_buffer, total_bytes);

  for (int communication_step = 0; communication_step < group_size - 1; communication_step++) {
    if (perform_ring_communication_step(process_group, process_group->right_send_buffer,
                                        process_group->left_receive_buffer, total_bytes) != PG_SUCCESS) {
      return PG_ERROR;
    }

    for (int chunk_index = 0; chunk_index < group_size; chunk_index++) {
      char *local_chunk_ptr = (char *)process_group->right_send_buffer + (chunk_index * chunk_bytes);
      char *remote_chunk_ptr = (char *)process_group->left_receive_buffer + (chunk_index * chunk_bytes);

      int remote_has_data = 0;
      for (size_t byte_index = 0; byte_index < chunk_bytes; byte_index++) {
        if (remote_chunk_ptr[byte_index] != 0) {
          remote_has_data = 1;
          break;
        }
      }

      if (remote_has_data) {
        int local_has_data = 0;
        for (size_t byte_index = 0; byte_index < chunk_bytes; byte_index++) {
          if (local_chunk_ptr[byte_index] != 0) {
            local_has_data = 1;
            break;
          }
        }

        if (!local_has_data) {
          memcpy(local_chunk_ptr, remote_chunk_ptr, chunk_bytes);
        }
      }
    }

    memcpy(process_group->right_send_buffer, process_group->left_receive_buffer, total_bytes);

    for (int chunk_index = 0; chunk_index < group_size; chunk_index++) {
      char *working_chunk_ptr = (char *)process_group->right_send_buffer + (chunk_index * chunk_bytes);
      char *accumulated_chunk_ptr = (char *)receive_buffer + (chunk_index * chunk_bytes);

      int accumulated_has_data = 0;
      for (size_t byte_index = 0; byte_index < chunk_bytes; byte_index++) {
        if (accumulated_chunk_ptr[byte_index] != 0) {
          accumulated_has_data = 1;
          break;
        }
      }

      if (accumulated_has_data) {
        memcpy(working_chunk_ptr, accumulated_chunk_ptr, chunk_bytes);
      }
    }
  }

  memcpy(receive_buffer, process_group->right_send_buffer, total_bytes);
  return PG_SUCCESS;
}

static int pg_post_control_receive(pg_handle_internal_t *process_group, pg_control_msg_t *slot, uint32_t step) {
  struct ibv_sge sge = {.addr = (uintptr_t)slot, .length = sizeof(*slot), .lkey = process_group->left_receive_mr->lkey};

  struct ibv_recv_wr wr = {.wr_id = WRID_CTRL(step), .sg_list = &sge, .num_sge = 1};

  struct ibv_recv_wr *bad_wr = NULL;
  int result = ibv_post_recv(process_group->left_neighbor_qp, &wr, &bad_wr);
  if (result != 0) {
    fprintf(stderr, "Failed to post control receive for step %u\n", step);
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

static int pg_post_control_send(pg_handle_internal_t *process_group, uint32_t step, uint32_t chunk_index) {
  pg_control_msg_t message = {.step = step, .chunk_index = chunk_index};

  return rdma_post_send_inline(process_group->right_neighbor_qp, &message, sizeof(message), WRID_CTRL_SEND(step));
}

static int pg_process_next_completion(pg_handle_internal_t *process_group, uint8_t *control_ready, int total_steps,
                                      size_t *outstanding_reads, size_t *completed_reads) {
  struct ibv_wc work_completion;
  if (rdma_poll_for_completion(process_group->rdma_context.completion_queue, &work_completion) != PG_SUCCESS) {
    return PG_ERROR;
  }

  uint8_t kind = WRID_KIND(work_completion.wr_id);

  if (work_completion.opcode == IBV_WC_RDMA_READ) {
    if (kind != WRK_READ || !outstanding_reads || !completed_reads || *outstanding_reads == 0) {
      fprintf(stderr, "Unexpected RDMA read completion\n");
      return PG_ERROR;
    }
    (*outstanding_reads)--;
    (*completed_reads)++;
    return PG_SUCCESS;
  }

  if (work_completion.opcode == IBV_WC_RECV) {
    if (kind != WRK_CTRL) {
      fprintf(stderr, "Unexpected receive completion\n");
      return PG_ERROR;
    }
    uint32_t step = WRID_CTRL_STEP(work_completion.wr_id);
    if (step < (uint32_t)total_steps) {
      control_ready[step] = 1;
    }
    return PG_SUCCESS;
  }

  if (work_completion.opcode == IBV_WC_SEND) {
    return PG_SUCCESS;
  }

  fprintf(stderr, "Unexpected completion opcode %d\n", work_completion.opcode);
  return PG_ERROR;
}

static int pg_wait_for_control(pg_handle_internal_t *process_group, uint32_t expected_step, uint8_t *control_ready,
                               int total_steps) {
  while (!control_ready[expected_step]) {
    if (pg_process_next_completion(process_group, control_ready, total_steps, NULL, NULL) != PG_SUCCESS) {
      return PG_ERROR;
    }
  }
  return PG_SUCCESS;
}

static int pg_all_gather_zero_copy(pg_handle_internal_t *process_group, void *send_buffer, size_t total_bytes,
                                   size_t chunk_bytes) {
  const int group_size = process_group->process_group_size;
  const int process_rank = process_group->process_rank;

  if (!process_group->final_recv_mr) {
    fprintf(stderr, "Final receive memory region is not registered\n");
    return PG_ERROR;
  }

  if (chunk_bytes == 0) {
    memset((void *)(uintptr_t)process_group->final_recv_base, 0, total_bytes);
    return PG_SUCCESS;
  }

  if (chunk_bytes * (size_t)group_size != total_bytes) {
    fprintf(stderr, "All-gather requires equal chunks across ranks\n");
    return PG_ERROR;
  }

  /* Initialize the final receive buffer with our data at the correct position */
  memset((void *)(uintptr_t)process_group->final_recv_base, 0, total_bytes);
  char *final_base = (char *)(uintptr_t)process_group->final_recv_base;
  char *my_chunk_position = final_base + (process_rank * chunk_bytes);
  memcpy(my_chunk_position, send_buffer, chunk_bytes);

  const int steps = group_size - 1;
  if (steps <= 0) {
    return PG_SUCCESS;
  }

  /* High-performance zero-copy: RDMA Write + SEND/RECV synchronization */
  int right_neighbor = (process_rank + 1) % group_size;
  
  /* Ring algorithm: RDMA Write for data, SEND/RECV for synchronization */
  for (int step = 0; step < steps; ++step) {
    /* Determine which chunk to send in this step */
    int send_chunk_owner = (process_rank - step + group_size) % group_size;
    size_t send_chunk_offset = (size_t)send_chunk_owner * chunk_bytes;
    void *send_chunk_ptr = final_base + send_chunk_offset;
    
    /* Determine where the right neighbor stores this chunk (same layout on every rank) */
    int remote_chunk_index = send_chunk_owner;
    size_t remote_chunk_offset = (size_t)remote_chunk_index * chunk_bytes;
    
    /* RDMA Write: High-performance data transfer */
    uint64_t remote_write_addr = process_group->remote_buffer_addrs[right_neighbor] + remote_chunk_offset;
    uint32_t remote_rkey = process_group->remote_buffer_rkeys[right_neighbor];
    
    if (rdma_post_write_request(process_group->right_neighbor_qp, send_chunk_ptr, chunk_bytes,
                                process_group->final_recv_mr, remote_write_addr, remote_rkey,
                                (uint64_t)(100 + step), 1) != 0) {
      fprintf(stderr, "Failed to post RDMA write for step %d\n", step);
      return PG_ERROR;
    }
    
    /* SEND: Lightweight synchronization notification */
    uint32_t sync_msg = 0xDEADBEEF + step;
    if (rdma_post_send_inline(process_group->right_neighbor_qp, &sync_msg, sizeof(sync_msg),
                              (uint64_t)(200 + step)) != 0) {
      fprintf(stderr, "Failed to post sync send for step %d\n", step);
      return PG_ERROR;
    }
    
    /* RECV: Wait for synchronization from left neighbor */
    if (rdma_post_receive_request(process_group->left_neighbor_qp, process_group->left_receive_buffer,
                                  sizeof(sync_msg), process_group->left_receive_mr) != PG_SUCCESS) {
      fprintf(stderr, "Failed to post sync receive for step %d\n", step);
      return PG_ERROR;
    }
    
    /* Wait for all operations: RDMA Write + SEND + RECV */
    int write_done = 0, send_done = 0, recv_done = 0;
    while (!write_done || !send_done || !recv_done) {
      struct ibv_wc work_completion;
      if (rdma_poll_for_completion(process_group->rdma_context.completion_queue, &work_completion) != PG_SUCCESS) {
        fprintf(stderr, "Failed to poll completion for step %d\n", step);
        return PG_ERROR;
      }
      
      if (work_completion.opcode == IBV_WC_RDMA_WRITE && !write_done) {
        write_done = 1;
      } else if (work_completion.opcode == IBV_WC_SEND && !send_done) {
        send_done = 1;
      } else if (work_completion.opcode == IBV_WC_RECV && !recv_done) {
        recv_done = 1;
        /* Synchronization complete - data is guaranteed to be available */
      }
    }
  }

  return PG_SUCCESS;
}

static int pg_all_gather_zero_copy_with_receive_buffer(pg_handle_internal_t *process_group, void *send_buffer,
                                                       void *receive_buffer, size_t total_bytes, size_t chunk_bytes) {
  /* First call the original zero-copy function */
  int result = pg_all_gather_zero_copy(process_group, send_buffer, total_bytes, chunk_bytes);
  if (result != PG_SUCCESS) {
    return result;
  }

  /* Copy the final result from final_recv_base to receive_buffer if they're different */
  if ((void *)(uintptr_t)process_group->final_recv_base != receive_buffer) {
    memcpy(receive_buffer, (void *)(uintptr_t)process_group->final_recv_base, total_bytes);
  }

  return PG_SUCCESS;
}

int pg_all_gather(pg_handle_t process_group_handle, void *send_buffer, void *receive_buffer, int element_count,
                  pg_datatype_t data_type) {
  pg_handle_internal_t *process_group = (pg_handle_internal_t *)process_group_handle;

  PG_CHECK_NULL(process_group, "Process group handle is NULL");
  PG_CHECK_NULL(send_buffer, "Send buffer is NULL");
  PG_CHECK_NULL(receive_buffer, "Receive buffer is NULL");

  int group_size = process_group->process_group_size;

  /* Handle single-process case */
  if (group_size == 1) {
    size_t element_size = pg_get_datatype_element_size(data_type);
    memmove(receive_buffer, send_buffer, (size_t)element_count * element_size);
    return PG_SUCCESS;
  }

  size_t elem_sz = pg_get_datatype_element_size(data_type);
  size_t chunk_bytes = (size_t)element_count * elem_sz;
  size_t total_bytes = chunk_bytes * (size_t)group_size;

  if (chunk_bytes == 0) {
    return PG_SUCCESS;
  }

  uint64_t recv_base = (uint64_t)(uintptr_t)receive_buffer;
  int needs_registration = 0;

  if (!process_group->final_recv_mr) {
    needs_registration = 1;
  } else if (process_group->final_recv_base != recv_base || process_group->final_recv_bytes < total_bytes) {
    needs_registration = 1;
  }

  if (needs_registration) {
    if (process_group->final_recv_mr && process_group->final_recv_mr != process_group->right_receive_mr) {
      rdma_deregister_memory_buffer(process_group->final_recv_mr);
    }

    struct ibv_mr *new_mr = rdma_register_memory_buffer(&process_group->rdma_context, receive_buffer, total_bytes);
    if (!new_mr) {
      fprintf(stderr, "Failed to register final receive buffer for all-gather\n");
      return PG_ERROR;
    }
    process_group->final_recv_mr = new_mr;
  }

  process_group->final_recv_base = recv_base;
  process_group->final_recv_bytes = total_bytes;

  if (pg_exchange_allgather_mr(process_group, process_group->final_recv_base, process_group->final_recv_mr->rkey,
                              total_bytes) != PG_SUCCESS) {
    return PG_ERROR;
  }

  if (total_bytes <= process_group->eager_max) {
    return pg_all_gather_eager(process_group, send_buffer, receive_buffer, total_bytes, chunk_bytes);
  }

  return pg_all_gather_zero_copy_with_receive_buffer(process_group, send_buffer, receive_buffer, total_bytes, chunk_bytes);
}

int pg_all_reduce(pg_handle_t process_group_handle, void *send_buffer, void *receive_buffer, int element_count,
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
  if (pg_reduce_scatter(process_group_handle, send_buffer, receive_buffer, element_count, data_type, reduction_op) !=
      PG_SUCCESS) {
    fprintf(stderr, "All-reduce failed during reduce-scatter phase\n");
    return PG_ERROR;
  }

  /* Phase 2: All-gather to distribute complete results */
  pg_handle_internal_t *process_group = (pg_handle_internal_t *)process_group_handle;
  size_t element_size = pg_get_datatype_element_size(data_type);
  int chunk_size = element_count / process_group->process_group_size;

  size_t chunk_bytes = (size_t)chunk_size * element_size;
  void *gather_send_buffer = receive_buffer;

  if (chunk_bytes > 0) {
    gather_send_buffer = malloc(chunk_bytes);
    if (!gather_send_buffer) {
      fprintf(stderr, "All-reduce failed to allocate gather send buffer\n");
      return PG_ERROR;
    }
    memcpy(gather_send_buffer, receive_buffer, chunk_bytes);
  }

  if (pg_all_gather(process_group_handle, gather_send_buffer, receive_buffer, chunk_size, data_type) != PG_SUCCESS) {
    fprintf(stderr, "All-reduce failed during all-gather phase\n");
    if (chunk_bytes > 0) {
      free(gather_send_buffer);
    }
    return PG_ERROR;
  }

  if (chunk_bytes > 0) {
    free(gather_send_buffer);
  }

  return PG_SUCCESS;
}

/**
 * Test basic RDMA connectivity between ring neighbors
 * Sends small test messages to verify RDMA operations work
 */
int pg_test_rdma_connectivity(pg_handle_t process_group_handle) {
  pg_handle_internal_t *process_group = (pg_handle_internal_t *)process_group_handle;

  if (!process_group) {
    fprintf(stderr, "Invalid process group handle\n");
    return PG_ERROR;
  }

  /* Connectivity test start (quiet) */

  /* Skip connectivity test for single-process groups */
  if (process_group->process_group_size == 1) {
    printf("[Process %d] Single process group - connectivity test skipped\n", process_group->process_rank);
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

  /* Use the correct connected queue pairs:
   * Process 0: right_neighbor_qp connects to Process 1's left_neighbor_qp
   * Process 1: left_neighbor_qp connects to Process 0's right_neighbor_qp */
  struct ibv_qp *test_qp;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_mr;

  if (process_group->process_rank == 0) {
    test_qp = process_group->right_neighbor_qp; /* Connected to Process 1's left_neighbor_qp */
    send_mr = process_group->right_send_mr;
    recv_mr = process_group->left_receive_mr;
  } else {                                     /* Process 1 */
    test_qp = process_group->left_neighbor_qp; /* Connected to Process 0's right_neighbor_qp */
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

  /* Both processes post receive first */
  if (rdma_post_receive_request(test_qp, test_receive_data, test_data_size, recv_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Failed to post receive request\n", process_group->process_rank);
    return PG_ERROR;
  }

  /* Brief pause to ensure peer has its receive ready */
  usleep(1000);

  /* Both processes send to each other */
  if (rdma_post_send_request(test_qp, test_send_data, test_data_size, send_mr) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Failed to post send request\n", process_group->process_rank);
    return PG_ERROR;
  }

  /* Wait for completions */
  struct ibv_wc work_completion;
  if (rdma_poll_for_completion(process_group->rdma_context.completion_queue, &work_completion) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Point-to-point test FAILED - receive completion failed\n",
            process_group->process_rank);
    return PG_ERROR;
  }

  if (rdma_poll_for_completion(process_group->rdma_context.completion_queue, &work_completion) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Point-to-point test FAILED - send completion failed\n", process_group->process_rank);
    return PG_ERROR;
  }

  /* Verify received data */
  int expected_sender = 1 - process_group->process_rank;

  int data_valid = 1;
  for (size_t i = 0; i < test_data_size; i++) {
    char expected_value = (char)(expected_sender * 10 + i);
    if (test_receive_data[i] != expected_value) {
      printf("[Process %d] Data mismatch at byte %zu: expected %d, got %d\n", process_group->process_rank, i,
             expected_value, test_receive_data[i]);
      data_valid = 0;
      break;
    }
  }

  if (!data_valid) {
    printf(
        "[Process %d] Point-to-point RDMA test FAILED - data verification "
        "failed\n",
        process_group->process_rank);
    return PG_ERROR;
  }

  return PG_SUCCESS;
}
