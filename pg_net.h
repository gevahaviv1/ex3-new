#ifndef PG_NET_H
#define PG_NET_H

#include "RDMA_api.h"

/**
 * Process Group Network Bootstrap Module
 *
 * This module handles the TCP-based bootstrap phase for establishing
 * RDMA connections between processes in a process group. It provides
 * hostname parsing, TCP connection setup, and RDMA bootstrap information
 * exchange functionality.
 */

/*
 * =============================================================================
 * Hostname Management and Process Discovery
 * =============================================================================
 */

/**
 * Parse space-separated hostname list into array
 *
 * Converts a string containing space-separated hostnames into an array
 * of individual hostname strings. Memory is allocated for both the array
 * and individual hostname strings.
 *
 * @param server_list_string: Space-separated list of hostnames
 * @param hostname_count_ptr: Pointer to store number of parsed hostnames
 * @return: Array of hostname strings, or NULL on failure
 */
char **pgnet_parse_hostname_list(const char *server_list_string, int *hostname_count_ptr);

/**
 * Free hostname array and associated memory
 *
 * Properly deallocates memory for hostname array created by
 * pgnet_parse_hostname_list(). Safe to call with NULL pointer.
 *
 * @param hostname_array: Array of hostname strings to free
 * @param hostname_count: Number of hostnames in the array
 */
void pgnet_free_hostname_list(char **hostname_array, int hostname_count);

/**
 * Determine local process rank based on hostname
 *
 * Compares the local hostname with the provided hostname list to
 * determine this process's rank within the process group.
 *
 * @param hostname_array: Array of hostnames in the process group
 * @param process_group_size: Total number of processes in the group
 * @return: Process rank (0-based index), or -1 if hostname not found
 */
int pgnet_determine_process_rank(char **hostname_array, int process_group_size);

/*
 * =============================================================================
 * TCP Bootstrap Connection Management
 * =============================================================================
 */

/**
 * Establish TCP connection for bootstrap communication
 *
 * Creates either a server socket (listening) or client socket (connecting)
 * for exchanging RDMA bootstrap information. Server sockets accept one
 * connection and return the accepted socket descriptor.
 *
 * @param target_hostname: Hostname to connect to (ignored for servers)
 * @param tcp_port: Port number for connection
 * @param is_server_mode: True to create server socket, false for client
 * @return: Connected socket descriptor on success, -1 on failure
 */
int pgnet_establish_tcp_connection(const char *target_hostname, int tcp_port, int is_server_mode);

/*
 * =============================================================================
 * RDMA Bootstrap Information Exchange
 * =============================================================================
 */

/**
 * Exchange RDMA queue pair bootstrap information over TCP
 *
 * Performs bidirectional exchange of RDMA connection information between
 * two processes. The exchange order depends on whether this process is
 * acting as client or server in the TCP connection.
 *
 * Client mode: Send local info first, then receive remote info
 * Server mode: Receive remote info first, then send local info
 *
 * @param tcp_socket_fd: Connected TCP socket for communication
 * @param local_qp_info: Local RDMA bootstrap information to send
 * @param remote_qp_info: Buffer to store received remote information
 * @param is_client_mode: True if acting as TCP client, false if server
 * @return: PG_SUCCESS on successful exchange, PG_ERROR on failure
 */
int pgnet_exchange_rdma_bootstrap_info(int tcp_socket_fd, rdma_qp_bootstrap_info_t *local_qp_info,
                                       rdma_qp_bootstrap_info_t *remote_qp_info, int is_client_mode);

#endif /* PG_NET_H */
