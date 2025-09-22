/*
 * =============================================================================
 * Process Group Test Program
 * =============================================================================
 *
 * This program tests the RDMA-based process group library by running collective
 * communication operations across multiple processes. It accepts command-line
 * arguments to specify the process index and the list of participating hosts.
 *
 * Usage:
 *   ./test_connect -myindex <index> -list <host1> <host2> <host3> ...
 *
 * Example:
 *   ./test_connect -myindex 2 -list mlx-stud-01 mlx-stud-02 mlx-stud-03
 * mlx-stud-04
 *
 * This example runs the test as process 2 in a 4-process group.
 */

#define _DEFAULT_SOURCE
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "pg.h"

/*
 * =============================================================================
 * Test Configuration and Utilities
 * =============================================================================
 */

#define MAX_HOSTS        64
#define MAX_HOSTNAME_LEN 256
#define TEST_DATA_SIZE   (1024 * 1024) /* Number of doubles per collective test (≈8 MiB) */

/**
 * Print usage information for the test program
 */
static void print_usage(const char *program_name) {
  printf("Usage: %s -myindex <index> -list <host1> <host2> <host3> ...\n", program_name);
  printf("\n");
  printf("Parameters:\n");
  printf("  -myindex <index>  : Index of this process (0-based)\n");
  printf("  -list <hosts>     : Space-separated list of hostnames\n");
  printf("\n");
  printf("Example:\n");
  printf("  %s -myindex 2 -list mlx-stud-01 mlx-stud-02 mlx-stud-03 mlx-stud-04\n", program_name);
  printf("\n");
  printf("This runs the test as process 2 in a 4-process group.\n");
  printf("Each process must run this command with its own -myindex value.\n");
}

/**
 * Parse command-line arguments and extract process index and host list
 */
static int parse_arguments(int argc, char *argv[], int *my_index, char *host_list, size_t host_list_size) {
  int index_found = 0;
  int list_found = 0;

  /* Initialize my_index to avoid uninitialized warning */
  *my_index = -1;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-myindex") == 0) {
      if (i + 1 >= argc) {
        fprintf(stderr, "Error: -myindex requires an argument\n");
        return -1;
      }
      *my_index = atoi(argv[i + 1]);
      index_found = 1;
      i++;  // Skip the next argument
    } else if (strcmp(argv[i], "-list") == 0) {
      if (i + 1 >= argc) {
        fprintf(stderr, "Error: -list requires at least one hostname\n");
        return -1;
      }

      // Build space-separated host list from remaining arguments
      host_list[0] = '\0';
      for (int j = i + 1; j < argc; j++) {
        if (strlen(host_list) + strlen(argv[j]) + 2 > host_list_size) {
          fprintf(stderr, "Error: Host list too long\n");
          return -1;
        }
        if (strlen(host_list) > 0) {
          strcat(host_list, " ");
        }
        strcat(host_list, argv[j]);
      }
      list_found = 1;
      break;  // All remaining arguments are hostnames
    }
  }

  if (!index_found) {
    fprintf(stderr, "Error: -myindex parameter is required\n");
    return -1;
  }

  if (!list_found) {
    fprintf(stderr, "Error: -list parameter is required\n");
    return -1;
  }

  return 0;
}

/**
 * Get current time in microseconds for performance measurement
 */
static double get_time_microseconds(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000.0 + tv.tv_usec;
}

/**
 * Initialize test data with process-specific values
 */
static void initialize_test_data(double *data, int size, int process_rank) {
  for (int i = 0; i < size; i++) {
    // Each process contributes different values based on its rank
    data[i] = (double)(process_rank * 1000 + i + 1);
  }
}

/**
 * Verify all-reduce results
 */
static int verify_all_reduce_results(double *result, int size, int num_processes) {
  int errors = 0;

  for (int i = 0; i < size; i++) {
    // Expected result: sum of (rank * 1000 + i + 1) for all ranks
    double expected = 0.0;
    for (int rank = 0; rank < num_processes; rank++) {
      expected += (double)(rank * 1000 + i + 1);
    }

    if (fabs(result[i] - expected) > 1e-9) {
      if (errors < 10) {  // Limit error output
        printf("Error at index %d: expected %.1f, got %.1f\n", i, expected, result[i]);
      }
      errors++;
    }
  }

  return errors;
}

/*
 * =============================================================================
 * Test Functions
 * =============================================================================
 */

/**
 * Test all-reduce collective operation
 */
static int test_all_reduce(pg_handle_t process_group, int process_rank, int num_processes) {
  printf("[Process %d] Testing all-reduce operation...\n", process_rank);

  // Allocate test data
  double *send_data = malloc(TEST_DATA_SIZE * sizeof(double));
  double *recv_data = malloc(TEST_DATA_SIZE * sizeof(double));

  if (!send_data || !recv_data) {
    fprintf(stderr, "[Process %d] Failed to allocate memory for all-reduce test\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  // Initialize test data
  initialize_test_data(send_data, TEST_DATA_SIZE, process_rank);

  // Warm up once to avoid cold-start effects on the timing run
  if (pg_all_reduce(process_group, send_data, recv_data, TEST_DATA_SIZE, PG_DATATYPE_DOUBLE, PG_OPERATION_SUM) !=
      PG_SUCCESS) {
    fprintf(stderr, "[Process %d] All-reduce warm-up failed\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  initialize_test_data(send_data, TEST_DATA_SIZE, process_rank);
  memset(recv_data, 0, TEST_DATA_SIZE * sizeof(double));

  // Measure performance
  double start_time = get_time_microseconds();

  // Perform all-reduce
  int result = pg_all_reduce(process_group, send_data, recv_data, TEST_DATA_SIZE, PG_DATATYPE_DOUBLE, PG_OPERATION_SUM);

  double end_time = get_time_microseconds();
  double elapsed_ms = (end_time - start_time) / 1000.0;

  if (result != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] All-reduce operation failed\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  // Verify results
  int errors = verify_all_reduce_results(recv_data, TEST_DATA_SIZE, num_processes);

  if (errors == 0) {
    double bytes_transferred = (double)TEST_DATA_SIZE * sizeof(double) * 2.0;  // 2x for send+recv
    double seconds = elapsed_ms / 1000.0;
    double throughput_mbps = seconds > 0.0 ? (bytes_transferred * 8.0) / (seconds * 1e6) : 0.0;
    printf("[Process %d] All-reduce PASSED (%.2f ms, %.2f Mb/s)\n", process_rank, elapsed_ms, throughput_mbps);
  } else {
    printf("[Process %d] All-reduce FAILED (%d errors)\n", process_rank, errors);
  }

  // Print sample results
  printf("[Process %d] Sample results: ", process_rank);
  for (int i = 0; i < 5 && i < TEST_DATA_SIZE; i++) {
    printf("%.1f ", recv_data[i]);
  }
  printf("...\n");

  free(send_data);
  free(recv_data);
  return (errors == 0) ? 0 : -1;
}

/**
 * Test reduce-scatter collective operation
 */
static int test_reduce_scatter(pg_handle_t process_group, int process_rank, int num_processes) {
  printf("[Process %d] Testing reduce-scatter operation...\n", process_rank);

  // Allocate test data
  double *send_data = malloc(TEST_DATA_SIZE * sizeof(double));
  double *recv_data = malloc((TEST_DATA_SIZE / num_processes) * sizeof(double));

  if (!send_data || !recv_data) {
    fprintf(stderr, "[Process %d] Failed to allocate memory for reduce-scatter test\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  // Initialize test data
  initialize_test_data(send_data, TEST_DATA_SIZE, process_rank);

  if (pg_reduce_scatter(process_group, send_data, recv_data, TEST_DATA_SIZE, PG_DATATYPE_DOUBLE, PG_OPERATION_SUM) !=
      PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Reduce-scatter warm-up failed\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  initialize_test_data(send_data, TEST_DATA_SIZE, process_rank);
  memset(recv_data, 0, (TEST_DATA_SIZE / num_processes) * sizeof(double));

  // Measure performance
  double start_time = get_time_microseconds();

  // Perform reduce-scatter
  int result =
      pg_reduce_scatter(process_group, send_data, recv_data, TEST_DATA_SIZE, PG_DATATYPE_DOUBLE, PG_OPERATION_SUM);

  double end_time = get_time_microseconds();
  double elapsed_ms = (end_time - start_time) / 1000.0;

  if (result != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Reduce-scatter operation failed\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  printf("[Process %d] Reduce-scatter PASSED (%.2f ms)\n", process_rank, elapsed_ms);

  // Print sample results (this process's chunk)
  printf("[Process %d] My chunk results: ", process_rank);
  int chunk_size = TEST_DATA_SIZE / num_processes;
  for (int i = 0; i < 5 && i < chunk_size; i++) {
    printf("%.1f ", recv_data[i]);
  }
  printf("...\n");

  free(send_data);
  free(recv_data);
  return 0;
}

/**
 * Test all-gather collective operation
 */
static int test_all_gather(pg_handle_t process_group, int process_rank, int num_processes) {
  printf("[Process %d] Testing all-gather operation...\n", process_rank);

  int chunk_size = TEST_DATA_SIZE / num_processes;

  // Allocate test data
  double *send_data = malloc(chunk_size * sizeof(double));
  double *recv_data = malloc(TEST_DATA_SIZE * sizeof(double));

  if (!send_data || !recv_data) {
    fprintf(stderr, "[Process %d] Failed to allocate memory for all-gather test\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  // Initialize this process's chunk
  for (int i = 0; i < chunk_size; i++) {
    send_data[i] = (double)(process_rank * 1000 + i + 1);
  }

  if (pg_all_gather(process_group, send_data, recv_data, TEST_DATA_SIZE, PG_DATATYPE_DOUBLE) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] All-gather warm-up failed\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  for (int i = 0; i < chunk_size; i++) {
    send_data[i] = (double)(process_rank * 1000 + i + 1);
  }
  memset(recv_data, 0, TEST_DATA_SIZE * sizeof(double));

  // Measure performance
  double start_time = get_time_microseconds();

  // Perform all-gather
  int result = pg_all_gather(process_group, send_data, recv_data, TEST_DATA_SIZE, PG_DATATYPE_DOUBLE);

  double end_time = get_time_microseconds();
  double elapsed_ms = (end_time - start_time) / 1000.0;

  if (result != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] All-gather operation failed\n", process_rank);
    free(send_data);
    free(recv_data);
    return -1;
  }

  printf("[Process %d] All-gather PASSED (%.2f ms)\n", process_rank, elapsed_ms);

  // Print sample results from each process's chunk
  printf("[Process %d] Gathered data: ", process_rank);
  for (int rank = 0; rank < num_processes; rank++) {
    int offset = rank * chunk_size;
    printf("P%d:[%.1f %.1f] ", rank, recv_data[offset], recv_data[offset + 1]);
  }
  printf("\n");

  free(send_data);
  free(recv_data);
  return 0;
}

/*
 * =============================================================================
 * Main Test Program
 * =============================================================================
 */

int main(int argc, char *argv[]) {
  int my_index;
  char host_list[4096];
  pg_handle_t process_group;
  int test_results = 0;

  printf("=== RDMA Process Group Test Program ===\n");

  // Parse command-line arguments
  if (parse_arguments(argc, argv, &my_index, host_list, sizeof(host_list)) != 0) {
    print_usage(argv[0]);
    return 1;
  }

  // Ensure library uses this process's intended rank when multiple
  // processes run on the same host (duplicate hostnames). This avoids
  // hostname-based rank collisions by explicitly setting PG_RANK.
  {
    char rank_str[16];
    snprintf(rank_str, sizeof(rank_str), "%d", my_index);
    if (setenv("PG_RANK", rank_str, 1) != 0) {
      perror("Failed to set PG_RANK");
      return 1;
    }
  }

  printf("Process Index: %d\n", my_index);
  printf("Host List: %s\n", host_list);

  // Count number of processes
  int num_processes = 1;
  for (char *p = host_list; *p; p++) {
    if (*p == ' ') num_processes++;
  }
  printf("Number of Processes: %d\n", num_processes);

  // Validate process index
  if (my_index < 0 || my_index >= num_processes) {
    fprintf(stderr, "Error: Process index %d is out of range [0, %d]\n", my_index, num_processes - 1);
    return 1;
  }

  printf("\n=== Initializing Process Group ===\n");

  // Initialize process group
  if (pg_initialize(host_list, &process_group) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Failed to initialize process group\n", my_index);
    return 1;
  }

  printf("[Process %d] Process group initialized successfully\n", my_index);

  // Add a small delay to ensure all processes are ready
  sleep(1);

  printf("=== Running RDMA Connectivity Test ===\n");

  /* Test basic RDMA connectivity first */
  printf("[Process %d] Testing RDMA connectivity...\n", my_index);
  if (pg_test_rdma_connectivity(process_group) == PG_SUCCESS) {
    printf("[Process %d] RDMA connectivity test PASSED\n", my_index);

    printf("=== Running Collective Operation Tests ===\n");

    // Test 1: All-Reduce
    if (test_all_reduce(process_group, my_index, num_processes) != 0) {
      test_results = -1;
    }
  } else {
    printf(
        "[Process %d] RDMA connectivity test FAILED - skipping collective "
        "operations\n",
        my_index);
    test_results = -1;
  }

  // Small delay between tests
  sleep(1);

  // Test 2: Reduce-Scatter
  if (test_reduce_scatter(process_group, my_index, num_processes) != 0) {
    test_results = -1;
  }

  // Small delay between tests
  sleep(1);

  // Test 3: All-Gather
  if (test_all_gather(process_group, my_index, num_processes) != 0) {
    test_results = -1;
  }

  printf("\n=== Cleaning Up ===\n");

  // Clean up process group
  if (pg_cleanup(process_group) != PG_SUCCESS) {
    fprintf(stderr, "[Process %d] Warning: Process group cleanup encountered errors\n", my_index);
  } else {
    printf("[Process %d] Process group cleanup completed successfully\n", my_index);
  }

  // Print final results
  printf("\n=== Test Results ===\n");
  if (test_results == 0) {
    printf("[Process %d] ALL TESTS PASSED!\n", my_index);
  } else {
    printf("[Process %d] SOME TESTS FAILED!\n", my_index);
  }

  return (test_results == 0) ? 0 : 1;
}
