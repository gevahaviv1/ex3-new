#define _DEFAULT_SOURCE
#include "RDMA_api.h"

#include <errno.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "constants.h"

/*
 * =============================================================================
 * Private Helper Functions
 * =============================================================================
 */

/**
 * Find InfiniBand device by name or return first available
 *
 * @param device_name: Name of device to find, or NULL for first available
 * @param device_list: Array of available devices
 * @return: Pointer to device or NULL if not found
 */
static struct ibv_device *find_ib_device(const char *device_name, struct ibv_device **device_list) {
  if (!device_list || !device_list[0]) {
    return NULL;
  }

  /* If no specific device requested, use first available */
  if (!device_name) {
    return device_list[0];
  }

  /* Search for named device */
  for (int i = 0; device_list[i]; i++) {
    if (strcmp(ibv_get_device_name(device_list[i]), device_name) == 0) {
      return device_list[i];
    }
  }

  return NULL;
}

/**
 * Generate random packet sequence number for QP initialization
 *
 * @return: Random 24-bit PSN value
 */
static uint32_t generate_random_psn(void) { return rand() & RDMA_PSN_MASK; }

/*
 * =============================================================================
 * RDMA Device and Context Management Implementation
 * =============================================================================
 */

int rdma_initialize_context(rdma_context_t *rdma_ctx, const char *device_name) {
  PG_CHECK_NULL(rdma_ctx, "RDMA context pointer is NULL");

  /* Initialize context to zero state */
  memset(rdma_ctx, 0, sizeof(*rdma_ctx));

  /* Allow environment to select device explicitly */
  const char *env_dev = getenv("PG_IB_DEVICE");
  if (device_name == NULL && env_dev && *env_dev) {
    device_name = env_dev;
  }

  /* Get list of available InfiniBand devices */
  struct ibv_device **device_list = ibv_get_device_list(NULL);
  if (!device_list) {
    fprintf(stderr, "Failed to get InfiniBand device list\n");
    return PG_ERROR;
  }

  /* Find the requested device or use first available */
  rdma_ctx->ib_device = find_ib_device(device_name, device_list);
  if (!rdma_ctx->ib_device) {
    fprintf(stderr, "No suitable InfiniBand device found\n");
    ibv_free_device_list(device_list);
    return PG_ERROR;
  }

  /* Open device context */
  rdma_ctx->device_context = ibv_open_device(rdma_ctx->ib_device);
  if (!rdma_ctx->device_context) {
    fprintf(stderr, "Failed to open InfiniBand device context\n");
    ibv_free_device_list(device_list);
    return PG_ERROR;
  }

  /* Allocate protection domain */
  rdma_ctx->protection_domain = ibv_alloc_pd(rdma_ctx->device_context);
  if (!rdma_ctx->protection_domain) {
    fprintf(stderr, "Failed to allocate protection domain\n");
    ibv_close_device(rdma_ctx->device_context);
    ibv_free_device_list(device_list);
    return PG_ERROR;
  }

  /* Create completion queue */
  rdma_ctx->completion_queue = ibv_create_cq(rdma_ctx->device_context, RDMA_DEFAULT_CQ_ENTRIES, NULL, NULL, 0);
  if (!rdma_ctx->completion_queue) {
    fprintf(stderr, "Failed to create completion queue\n");
    ibv_dealloc_pd(rdma_ctx->protection_domain);
    ibv_close_device(rdma_ctx->device_context);
    ibv_free_device_list(device_list);
    return PG_ERROR;
  }

  /* Select port (env override) and ensure it's ACTIVE */
  rdma_ctx->ib_port_number = RDMA_DEFAULT_IB_PORT;
  const char *env_port = getenv("PG_IB_PORT");
  if (env_port && *env_port) {
    int p = atoi(env_port);
    if (p > 0 && p < 255) rdma_ctx->ib_port_number = p;
  }

  /* Try to find an active port if possible */
  {
    struct ibv_device_attr dev_attr;
    if (ibv_query_device(rdma_ctx->device_context, &dev_attr) == 0) {
      /* If env specified a port, prefer it if ACTIVE, else pick first ACTIVE */
      int chosen = 0;
      if (rdma_ctx->ib_port_number >= 1 && rdma_ctx->ib_port_number <= dev_attr.phys_port_cnt) {
        struct ibv_port_attr pa;
        if (ibv_query_port(rdma_ctx->device_context, rdma_ctx->ib_port_number, &pa) == 0 &&
            pa.state == IBV_PORT_ACTIVE) {
          chosen = 1;
        }
      }
      if (!chosen) {
        for (uint8_t p = 1; p <= dev_attr.phys_port_cnt; ++p) {
          struct ibv_port_attr port_attr;
          if (ibv_query_port(rdma_ctx->device_context, p, &port_attr) == 0) {
            if (port_attr.state == IBV_PORT_ACTIVE) {
              rdma_ctx->ib_port_number = p;
              chosen = 1;
              break;
            }
          }
        }
      }
    }
  }

  /* Set GID index (env override) */
  rdma_ctx->gid_index = RDMA_DEFAULT_GID_INDEX;
  const char *env_gid = getenv("PG_GID_INDEX");
  if (env_gid && *env_gid) {
    int idx = atoi(env_gid);
    if (idx >= 0 && idx < 128) rdma_ctx->gid_index = idx;
  }

  /* Diagnostics intentionally quiet by default; errors are still reported */

  ibv_free_device_list(device_list);
  return PG_SUCCESS;
}

void rdma_cleanup_context(rdma_context_t *rdma_ctx) {
  if (!rdma_ctx) {
    return;
  }

  /* Destroy completion queue */
  if (rdma_ctx->completion_queue) {
    if (ibv_destroy_cq(rdma_ctx->completion_queue)) {
      fprintf(stderr, "Warning: Failed to destroy completion queue\n");
    }
    rdma_ctx->completion_queue = NULL;
  }

  /* Deallocate protection domain */
  if (rdma_ctx->protection_domain) {
    if (ibv_dealloc_pd(rdma_ctx->protection_domain)) {
      fprintf(stderr, "Warning: Failed to deallocate protection domain\n");
    }
    rdma_ctx->protection_domain = NULL;
  }

  /* Close device context */
  if (rdma_ctx->device_context) {
    if (ibv_close_device(rdma_ctx->device_context)) {
      fprintf(stderr, "Warning: Failed to close device context\n");
    }
    rdma_ctx->device_context = NULL;
  }

  /* Clear the entire structure */
  memset(rdma_ctx, 0, sizeof(*rdma_ctx));
}

/*
 * =============================================================================
 * Memory Region Management Implementation
 * =============================================================================
 */

struct ibv_mr *rdma_register_memory_buffer(rdma_context_t *rdma_ctx, void *buffer_ptr, size_t buffer_size) {
  PG_CHECK_NULL_PTR(rdma_ctx, "RDMA context is NULL");
  PG_CHECK_NULL_PTR(rdma_ctx->protection_domain, "Protection domain is NULL");
  PG_CHECK_NULL_PTR(buffer_ptr, "Buffer pointer is NULL");

  if (buffer_size == 0) {
    fprintf(stderr, "Buffer size cannot be zero\n");
    return NULL;
  }

  /* Register memory with appropriate access flags */
  struct ibv_mr *memory_region = ibv_reg_mr(rdma_ctx->protection_domain, buffer_ptr, buffer_size,
                                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

  if (!memory_region) {
    fprintf(stderr, "Failed to register memory region of size %zu\n", buffer_size);
    return NULL;
  }

  return memory_region;
}

void rdma_deregister_memory_buffer(struct ibv_mr *memory_region) {
  if (!memory_region) {
    return;
  }

  if (ibv_dereg_mr(memory_region)) {
    fprintf(stderr, "Warning: Failed to deregister memory region\n");
  }
}

/*
 * =============================================================================
 * Queue Pair Management Implementation
 * =============================================================================
 */

int rdma_create_queue_pair(rdma_context_t *rdma_ctx, struct ibv_qp **queue_pair_ptr) {
  PG_CHECK_NULL(rdma_ctx, "RDMA context is NULL");
  PG_CHECK_NULL(rdma_ctx->protection_domain, "Protection domain is NULL");
  PG_CHECK_NULL(rdma_ctx->completion_queue, "Completion queue is NULL");
  PG_CHECK_NULL(queue_pair_ptr, "Queue pair pointer is NULL");

  /* Configure queue pair initialization attributes */
  struct ibv_qp_init_attr qp_init_attributes = {
      .send_cq = rdma_ctx->completion_queue,
      .recv_cq = rdma_ctx->completion_queue,
      .cap = {.max_send_wr = RDMA_DEFAULT_QP_SEND_WR,
              .max_recv_wr = RDMA_DEFAULT_QP_RECV_WR,
              .max_send_sge = RDMA_DEFAULT_QP_SGE_COUNT,
              .max_recv_sge = RDMA_DEFAULT_QP_SGE_COUNT},
      .qp_type = IBV_QPT_RC /* Reliable Connection */
  };

  /* Create the queue pair */
  *queue_pair_ptr = ibv_create_qp(rdma_ctx->protection_domain, &qp_init_attributes);
  if (!*queue_pair_ptr) {
    fprintf(stderr, "Failed to create queue pair\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

int rdma_transition_qp_to_init(struct ibv_qp *queue_pair, int ib_port_num) {
  PG_CHECK_NULL(queue_pair, "Queue pair is NULL");

  /* Configure attributes for INIT state */
  struct ibv_qp_attr qp_attributes = {
      .qp_state = IBV_QPS_INIT,
      .pkey_index = 0,
      .port_num = ib_port_num,
      .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE};

  /* Specify which attributes are being modified */
  int attribute_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

  /* Perform the state transition */
  int result = ibv_modify_qp(queue_pair, &qp_attributes, attribute_mask);
  if (result != 0) {
    fprintf(stderr, "Failed to transition queue pair to INIT state\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

int rdma_transition_qp_to_rtr(struct ibv_qp *queue_pair, rdma_qp_bootstrap_info_t *remote_qp_info, int ib_port_num,
                              int gid_index) {
  PG_CHECK_NULL(queue_pair, "Queue pair is NULL");
  PG_CHECK_NULL(remote_qp_info, "Remote QP info is NULL");

  /* Determine addressing mode: prefer LID if available, otherwise use GID */
  int use_global = (remote_qp_info->local_identifier == 0);

  /* Configure attributes for RTR (Ready-to-Receive) state */
  struct ibv_qp_attr qp_attributes;
  memset(&qp_attributes, 0, sizeof(qp_attributes));
  qp_attributes.qp_state = IBV_QPS_RTR;
  qp_attributes.path_mtu = RDMA_DEFAULT_MTU;
  qp_attributes.dest_qp_num = remote_qp_info->queue_pair_number;
  qp_attributes.rq_psn = remote_qp_info->packet_sequence_number;
  qp_attributes.max_dest_rd_atomic = RDMA_DEFAULT_MAX_RD_ATOMIC;
  qp_attributes.min_rnr_timer = RDMA_DEFAULT_MIN_RNR_TIMER;
  qp_attributes.ah_attr.is_global = use_global ? 1 : 0;
  qp_attributes.ah_attr.dlid = remote_qp_info->local_identifier;
  qp_attributes.ah_attr.sl = 0;
  qp_attributes.ah_attr.src_path_bits = 0;
  qp_attributes.ah_attr.port_num = ib_port_num;
  if (use_global) {
    qp_attributes.ah_attr.grh.dgid = remote_qp_info->global_identifier;
    qp_attributes.ah_attr.grh.flow_label = 0;
    qp_attributes.ah_attr.grh.sgid_index = gid_index;
    qp_attributes.ah_attr.grh.hop_limit = 0xFF;
    qp_attributes.ah_attr.grh.traffic_class = 0;
  }

  /* Specify which attributes are being modified */
  int attribute_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                       IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

  /* Perform the state transition */
  int result = ibv_modify_qp(queue_pair, &qp_attributes, attribute_mask);
  if (result != 0) {
    fprintf(stderr, "Failed to transition queue pair to RTR state\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

int rdma_transition_qp_to_rts(struct ibv_qp *queue_pair, uint32_t local_psn) {
  PG_CHECK_NULL(queue_pair, "Queue pair is NULL");

  /* Configure attributes for RTS (Ready-to-Send) state */
  struct ibv_qp_attr qp_attributes = {.qp_state = IBV_QPS_RTS,
                                      .timeout = RDMA_DEFAULT_TIMEOUT,
                                      .retry_cnt = RDMA_DEFAULT_RETRY_COUNT,
                                      .rnr_retry = RDMA_DEFAULT_RNR_RETRY,
                                      .sq_psn = local_psn,
                                      .max_rd_atomic = RDMA_DEFAULT_MAX_RD_ATOMIC};

  /* Specify which attributes are being modified */
  int attribute_mask =
      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

  /* Perform the state transition */
  int result = ibv_modify_qp(queue_pair, &qp_attributes, attribute_mask);
  if (result != 0) {
    fprintf(stderr, "Failed to transition queue pair to RTS state\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

void rdma_destroy_queue_pair(struct ibv_qp *queue_pair) {
  if (!queue_pair) {
    return;
  }

  if (ibv_destroy_qp(queue_pair)) {
    fprintf(stderr, "Warning: Failed to destroy queue pair\n");
  }
}

/*
 * =============================================================================
 * Queue Pair Information Management Implementation
 * =============================================================================
 */

void rdma_extract_qp_bootstrap_info(rdma_context_t *rdma_ctx, struct ibv_qp *queue_pair,
                                    rdma_qp_bootstrap_info_t *qp_info) {
  if (!rdma_ctx || !queue_pair || !qp_info) {
    return;
  }

  /* Query port attributes to get LID */
  struct ibv_port_attr port_attributes;
  int query_result = ibv_query_port(rdma_ctx->device_context, rdma_ctx->ib_port_number, &port_attributes);

  /* Fill in the bootstrap information */
  qp_info->queue_pair_number = queue_pair->qp_num;
  qp_info->local_identifier = (query_result == 0) ? port_attributes.lid : 0;
  qp_info->packet_sequence_number = generate_random_psn();
  qp_info->exposed_buffer_addr = 0;
  qp_info->exposed_buffer_rkey = 0;
  qp_info->reserved = 0;
  qp_info->exposed_buffer_bytes = 0;

  /* Query GID (Global Identifier) */
  int gid_result = ibv_query_gid(rdma_ctx->device_context, rdma_ctx->ib_port_number, rdma_ctx->gid_index,
                                 &qp_info->global_identifier);

  if (gid_result != 0) {
    /* Clear GID on failure */
    memset(&qp_info->global_identifier, 0, sizeof(qp_info->global_identifier));
  }

  /* Quiet: omit verbose local QP diagnostics; keep errors elsewhere */
}

/*
 * =============================================================================
 * RDMA Communication Operations Implementation
 * =============================================================================
 */

int rdma_post_receive_request(struct ibv_qp *queue_pair, void *buffer_ptr, size_t buffer_size,
                              struct ibv_mr *memory_region) {
  PG_CHECK_NULL(queue_pair, "Queue pair is NULL");
  PG_CHECK_NULL(buffer_ptr, "Buffer pointer is NULL");
  PG_CHECK_NULL(memory_region, "Memory region is NULL");

  /* Configure scatter-gather element */
  struct ibv_sge scatter_gather_element = {
      .addr = (uint64_t)buffer_ptr, .length = buffer_size, .lkey = memory_region->lkey};

  /* Configure receive work request */
  struct ibv_recv_wr receive_work_request = {
      .wr_id = 1, .sg_list = &scatter_gather_element, .num_sge = 1}; /* RECV ID */

  /* Post the receive request */
  struct ibv_recv_wr *bad_work_request;
  int result = ibv_post_recv(queue_pair, &receive_work_request, &bad_work_request);

  if (result != 0) {
    fprintf(stderr, "Failed to post receive work request\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

int rdma_post_send_request(struct ibv_qp *queue_pair, void *buffer_ptr, size_t data_size,
                           struct ibv_mr *memory_region) {
  PG_CHECK_NULL(queue_pair, "Queue pair is NULL");
  PG_CHECK_NULL(buffer_ptr, "Buffer pointer is NULL");
  PG_CHECK_NULL(memory_region, "Memory region is NULL");

  /* Configure scatter-gather element */
  struct ibv_sge scatter_gather_element = {
      .addr = (uint64_t)buffer_ptr, .length = data_size, .lkey = memory_region->lkey};

  /* Configure send work request */
  struct ibv_send_wr send_work_request = {
      .wr_id = 2, /* SEND ID */
      .sg_list = &scatter_gather_element,
      .num_sge = 1,
      .opcode = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED /* Request completion notification */
  };

  /* Post the send request */
  struct ibv_send_wr *bad_work_request;
  int result = ibv_post_send(queue_pair, &send_work_request, &bad_work_request);

  if (result != 0) {
    fprintf(stderr, "Failed to post send work request\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

int rdma_poll_for_completion(struct ibv_cq *completion_queue, struct ibv_wc *work_completion) {
  PG_CHECK_NULL(completion_queue, "Completion queue is NULL");
  PG_CHECK_NULL(work_completion, "Work completion pointer is NULL");

  /* Initialize work completion structure */
  memset(work_completion, 0, sizeof(struct ibv_wc));

  /* Poll until we get a completion */
  while (1) {
    int num_completions = ibv_poll_cq(completion_queue, 1, work_completion);

    if (num_completions > 0) {
      /* Got a completion - check status */
      if (work_completion->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Work completion failed with status: %s\n", ibv_wc_status_str(work_completion->status));
        return PG_ERROR;
      }
      return PG_SUCCESS;
    } else if (num_completions < 0) {
      fprintf(stderr, "Error polling completion queue: %d\n", num_completions);
      return PG_ERROR;
    }

    /* num_completions == 0: no completions available, keep polling */
    /* Small yield to avoid excessive CPU usage */
    usleep(1);
  }
}

/* Simple completion storage for out-of-order completions */
#define MAX_STORED_COMPLETIONS 32
static struct ibv_wc stored_completions[MAX_STORED_COMPLETIONS];
static int stored_count = 0;

int rdma_poll_for_specific_completion(struct ibv_cq *completion_queue, struct ibv_wc *work_completion,
                                      uint64_t expected_wr_id) {
  PG_CHECK_NULL(completion_queue, "Completion queue is NULL");
  PG_CHECK_NULL(work_completion, "Work completion pointer is NULL");

  /* First check if we already have this completion stored */
  for (int i = 0; i < stored_count; i++) {
    if (stored_completions[i].wr_id == expected_wr_id) {
      /* Found it! Copy to output and remove from storage */
      *work_completion = stored_completions[i];

      /* Shift remaining completions down */
      for (int j = i; j < stored_count - 1; j++) {
        stored_completions[j] = stored_completions[j + 1];
      }
      stored_count--;

      /* Check completion status */
      if (work_completion->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Work completion failed with status: %s (wr_id=%lu)\n",
                ibv_wc_status_str(work_completion->status), work_completion->wr_id);
        return PG_ERROR;
      }
      return PG_SUCCESS;
    }
  }

  /* Not found in storage, poll for new completions */
  while (1) {
    /* Initialize work completion structure */
    memset(work_completion, 0, sizeof(struct ibv_wc));

    int num_completions = ibv_poll_cq(completion_queue, 1, work_completion);

    if (num_completions > 0) {
      /* Check if this is the completion we're looking for */
      if (work_completion->wr_id == expected_wr_id) {
        /* Got the right completion - check status */
        if (work_completion->status != IBV_WC_SUCCESS) {
          fprintf(stderr, "Work completion failed with status: %s (wr_id=%lu)\n",
                  ibv_wc_status_str(work_completion->status), work_completion->wr_id);
          return PG_ERROR;
        }
        return PG_SUCCESS;
      } else {
        /* Store this completion for later if we have space */
        if (stored_count < MAX_STORED_COMPLETIONS) {
          stored_completions[stored_count] = *work_completion;
          stored_count++;
        } else {
          fprintf(stderr,
                  "Warning: Completion storage full, dropping completion with "
                  "wr_id=%lu\n",
                  work_completion->wr_id);
        }
      }
    } else if (num_completions < 0) {
      fprintf(stderr, "Error polling completion queue: %d\n", num_completions);
      return PG_ERROR;
    }

    /* num_completions == 0: no completions available, keep polling */
    usleep(1);
  }
}

/*
 * =============================================================================
 * RDMA One-Sided Operations Implementation
 * =============================================================================
 */

int rdma_post_send_inline(struct ibv_qp *queue_pair, void *data_ptr, size_t data_size, uint64_t wr_id) {
  PG_CHECK_NULL(queue_pair, "Queue pair is NULL");
  PG_CHECK_NULL(data_ptr, "Data pointer is NULL");

  if (data_size == 0) {
    fprintf(stderr, "Data size cannot be zero\n");
    return PG_ERROR;
  }

  if (data_size > RDMA_DEFAULT_INLINE_DATA_SIZE) {
    fprintf(stderr, "Data size %zu exceeds maximum inline size %d\n", data_size, RDMA_DEFAULT_INLINE_DATA_SIZE);
    return PG_ERROR;
  }

  /* Configure scatter-gather element for inline data */
  struct ibv_sge scatter_gather_element = {
      .addr = (uintptr_t)data_ptr, .length = data_size, .lkey = 0 /* Not needed for inline data */
  };

  /* Configure send work request with inline data */
  struct ibv_send_wr send_work_request = {.wr_id = wr_id,
                                          .next = NULL,
                                          .sg_list = &scatter_gather_element,
                                          .num_sge = 1,
                                          .opcode = IBV_WR_SEND,
                                          .send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE};

  /* Post the send request */
  struct ibv_send_wr *bad_work_request;
  int result = ibv_post_send(queue_pair, &send_work_request, &bad_work_request);

  if (result != 0) {
    fprintf(stderr, "Failed to post inline send work request\n");
    return PG_ERROR;
  }

  return PG_SUCCESS;
}

/*
 * =============================================================================
 * Simplified RDMA Operations for Large-Message Zero-Copy
 * =============================================================================
 */

int rdma_post_write_request(struct ibv_qp *qp, void *local_buf, size_t size, struct ibv_mr *local_mr,
                            uint64_t remote_addr, uint32_t rkey, uint64_t wr_id, int signaled) {
  if (!qp || !local_buf || !local_mr || size == 0) {
    errno = EINVAL;
    perror("rdma_post_write_request");
    return -1;
  }

  struct ibv_sge sge = {.addr = (uintptr_t)local_buf, .length = size, .lkey = local_mr->lkey};

  struct ibv_send_wr wr = {.wr_id = wr_id,
                           .next = NULL,
                           .sg_list = &sge,
                           .num_sge = 1,
                           .opcode = IBV_WR_RDMA_WRITE,
                           .send_flags = 0,
                           .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}}};

  if (signaled) {
    wr.send_flags |= IBV_SEND_SIGNALED;
  }

  struct ibv_send_wr *bad_wr = NULL;
  int result = ibv_post_send(qp, &wr, &bad_wr);

  if (result != 0) {
    errno = result;
    perror("ibv_post_send (write)");
    return -1;
  }

  return 0;
}

int rdma_post_read_request(struct ibv_qp *qp, void *local_buf, size_t size, struct ibv_mr *local_mr,
                           uint64_t remote_addr, uint32_t rkey, uint64_t wr_id, int signaled) {
  if (!qp || !local_buf || !local_mr || size == 0) {
    errno = EINVAL;
    perror("rdma_post_read_request");
    return -1;
  }

  struct ibv_sge sge = {.addr = (uintptr_t)local_buf, .length = size, .lkey = local_mr->lkey};

  struct ibv_send_wr wr = {.wr_id = wr_id,
                           .next = NULL,
                           .sg_list = &sge,
                           .num_sge = 1,
                           .opcode = IBV_WR_RDMA_READ,
                           .send_flags = 0,
                           .wr = {.rdma = {.remote_addr = remote_addr, .rkey = rkey}}};

  if (signaled) {
    wr.send_flags |= IBV_SEND_SIGNALED;
  }

  struct ibv_send_wr *bad_wr = NULL;
  int result = ibv_post_send(qp, &wr, &bad_wr);

  if (result != 0) {
    errno = result;
    perror("ibv_post_send (read)");
    return -1;
  }

  return 0;
}
