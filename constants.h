#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <infiniband/verbs.h>
#include <stdint.h>

/*
 * Process Group Network Configuration
 * These constants control TCP bootstrap and hostname resolution
 */
#define PG_DEFAULT_PORT        18515 /* Base port for TCP bootstrap connections */
#define PG_MAX_PROCESS_COUNT   64    /* Maximum number of processes in a group */
#define PG_HOSTNAME_MAX_LENGTH 256   /* Maximum hostname string length */
#define PG_TCP_BACKLOG         1     /* TCP listen backlog for bootstrap */

/*
 * RDMA/InfiniBand Configuration
 * Default values for RDMA device initialization and QP setup
 */
#define RDMA_DEFAULT_IB_PORT          1  /* Default InfiniBand port number */
#define RDMA_DEFAULT_GID_INDEX        0  /* Default GID table index */
#define RDMA_DEFAULT_INLINE_DATA_SIZE 64 /* Maximum inline data size in bytes */

/* Completion Queue Configuration */
#define RDMA_DEFAULT_CQ_ENTRIES 64 /* Number of completion queue entries */

/* Queue Pair Configuration */
#define RDMA_DEFAULT_QP_SEND_WR   32           /* Maximum outstanding send work requests */
#define RDMA_DEFAULT_QP_RECV_WR   32           /* Maximum outstanding receive work requests */
#define RDMA_DEFAULT_QP_SGE_COUNT 1            /* Number of scatter-gather elements per WR */
#define RDMA_DEFAULT_MTU          IBV_MTU_2048 /* Maximum transmission unit */

/* QP State Transition Parameters */
#define RDMA_DEFAULT_TIMEOUT     14 /* Local ACK timeout (4.096us * 2^timeout) */
#define RDMA_DEFAULT_RETRY_COUNT 7  /* Number of retries for timeout errors */
#define RDMA_DEFAULT_RNR_RETRY   7  /* Number of RNR (Receiver Not Ready) retries */

#define RDMA_DEFAULT_MIN_RNR_TIMER 12 /* Minimum RNR NAK timer */
#define RDMA_DEFAULT_MAX_RD_ATOMIC 4  /* Maximum outstanding RDMA read/atomic ops */

/*
 * Buffer Management
 * Memory allocation and data transfer configuration
 */
#define PG_BUFFER_SIZE_BYTES (128 * 1024 * 1024) /* 64MB buffer for collective operations */
#define PG_CHUNK_SIZE_BYTES  (1024 * 1024)       /* 1MB chunks for pipelined transfers */

/*
 * Pipelining Configuration
 * Parameters for overlapping communication and computation
 */
#define PG_DEFAULT_EAGER_MAX   (256 * 1024 * 1024) /* Maximum message size for eager protocol */
#define PG_DEFAULT_CHUNK_BYTES (1024 * 1024)      /* Default chunk size for pipelining */
#define PG_DEFAULT_INFLIGHT    24                 /* Default maximum inflight operations */

/*
 * Packet Sequence Number Generation
 * Used for RDMA connection establishment
 */
#define RDMA_PSN_MASK 0xffffff /* 24-bit mask for packet sequence numbers */

/*
 * Error Handling Macros
 * Consistent error checking and reporting patterns
 */
#define PG_SUCCESS 0  /* Success return code */
#define PG_ERROR   -1 /* Generic error return code */

/* Check condition and return error if false */
#define PG_CHECK_ERROR(condition, msg)     \
  do {                                     \
    if (!(condition)) {                    \
      fprintf(stderr, "Error: %s\n", msg); \
      return PG_ERROR;                     \
    }                                      \
  } while (0)

/* Check pointer and return error if NULL */
#define PG_CHECK_NULL(ptr, msg) PG_CHECK_ERROR((ptr) != NULL, msg)

/* Check pointer and return NULL if condition fails (for pointer-returning
 * functions) */
#define PG_CHECK_NULL_PTR(ptr, msg)        \
  do {                                     \
    if (!(ptr)) {                          \
      fprintf(stderr, "Error: %s\n", msg); \
      return NULL;                         \
    }                                      \
  } while (0)

/* Check condition and return NULL if false (for pointer-returning functions) */
#define PG_CHECK_ERROR_PTR(condition, msg) \
  do {                                     \
    if (!(condition)) {                    \
      fprintf(stderr, "Error: %s\n", msg); \
      return NULL;                         \
    }                                      \
  } while (0)

/* Check RDMA operation result and return error if failed */
#define PG_CHECK_RDMA(result, msg) PG_CHECK_ERROR((result) == 0, msg)

/*
 * Utility Macros
 * Common operations used throughout the codebase
 */
#define PG_MIN(a, b)                  ((a) < (b) ? (a) : (b))
#define PG_MAX(a, b)                  ((a) > (b) ? (a) : (b))
#define PG_ALIGN_UP(value, alignment) (((value) + (alignment) - 1) & ~((alignment) - 1))

/* Work Request identification helpers */
#define WRID_KIND_SHIFT 56
#define WRK_READ        1u
#define WRK_CTRL        2u
#define WRK_CTRL_SEND   3u

#define WRID_KIND(wrid) ((uint8_t)(((uint64_t)(wrid)) >> WRID_KIND_SHIFT))

#define WRID_READ(idx, off)                                                                  \
  ((((uint64_t)WRK_READ) << WRID_KIND_SHIFT) | ((((uint64_t)(idx)) & 0x00FFFFFFULL) << 32) | \
   (((uint64_t)(off)) & 0xFFFFFFFFULL))
#define WRID_READ_INDEX(wrid)  ((uint32_t)((((uint64_t)(wrid)) >> 32) & 0x00FFFFFFULL))
#define WRID_READ_OFFSET(wrid) ((uint32_t)((uint64_t)(wrid) & 0xFFFFFFFFULL))

#define WRID_CTRL(step)      ((((uint64_t)WRK_CTRL) << WRID_KIND_SHIFT) | ((uint64_t)(step) & 0x00FFFFFFFFFFFFFFULL))
#define WRID_CTRL_STEP(wrid) ((uint32_t)((uint64_t)(wrid) & 0xFFFFFFFFULL))

#define WRID_CTRL_SEND(step) \
  ((((uint64_t)WRK_CTRL_SEND) << WRID_KIND_SHIFT) | ((uint64_t)(step) & 0x00FFFFFFFFFFFFFFULL))

#endif /* CONSTANTS_H */
