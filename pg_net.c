/* Feature test macros for gethostname and strdup */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

#include "pg_net.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>

/*
 * =============================================================================
 * Private Helper Functions
 * =============================================================================
 */

/**
 * Create and configure TCP socket with appropriate options
 * 
 * @return: Socket file descriptor on success, -1 on failure
 */
static int create_tcp_socket(void) {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        perror("Failed to create TCP socket");
        return -1;
    }
    
    /* Enable address reuse to avoid "Address already in use" errors */
    int socket_option = 1;
    if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, 
                   &socket_option, sizeof(socket_option)) < 0) {
        perror("Warning: Failed to set SO_REUSEADDR");
        /* Continue anyway, this is not fatal */
    }
    
    return socket_fd;
}

/**
 * Setup server socket for accepting bootstrap connections
 * 
 * @param tcp_port: Port number to listen on
 * @return: Connected client socket on success, -1 on failure
 */
static int setup_bootstrap_server(int tcp_port) {
    int server_socket = create_tcp_socket();
    if (server_socket < 0) {
        return -1;
    }
    
    /* Configure server address */
    struct sockaddr_in server_address;
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(tcp_port);
    
    /* Bind to the specified port */
    if (bind(server_socket, (struct sockaddr*)&server_address, 
             sizeof(server_address)) < 0) {
        perror("Failed to bind server socket");
        close(server_socket);
        return -1;
    }
    
    /* Listen for incoming connections */
    if (listen(server_socket, PG_TCP_BACKLOG) < 0) {
        perror("Failed to listen on server socket");
        close(server_socket);
        return -1;
    }
    
    fprintf(stderr, "[TCP] server listening on %d\n", tcp_port);
    
    /* Accept one client connection */
    int client_socket = accept(server_socket, NULL, NULL);
    if (client_socket < 0) {
        perror("Failed to accept client connection");
    } else {
        fprintf(stderr, "[TCP] server accepted on %d\n", tcp_port);
    }
    
    /* Close the server socket as we only need one connection */
    close(server_socket);
    return client_socket;
}

/**
 * Setup client socket for connecting to bootstrap server
 * 
 * @param target_hostname: Hostname of server to connect to
 * @param tcp_port: Port number to connect to
 * @return: Connected socket on success, -1 on failure
 */
static int setup_bootstrap_client(const char *target_hostname, int tcp_port) {
    int client_socket = create_tcp_socket();
    if (client_socket < 0) {
        return -1;
    }
    
    /* Resolve hostname to IP address */
    struct hostent *host_entry = gethostbyname(target_hostname);
    if (!host_entry) {
        fprintf(stderr, "Failed to resolve hostname: %s\n", target_hostname);
        close(client_socket);
        return -1;
    }
    
    /* Configure server address */
    struct sockaddr_in server_address;
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(tcp_port);
    memcpy(&server_address.sin_addr, host_entry->h_addr_list[0], host_entry->h_length);
    
    /* Connect to the server with retry logic */
    int attempts = 0;
    while (1) {
        if (connect(client_socket, (struct sockaddr*)&server_address, sizeof(server_address)) == 0) {
            fprintf(stderr, "[TCP] client connect ok: port=%d attempts=%d\n", tcp_port, attempts);
            break;
        }
        if (errno != ECONNREFUSED && errno != ETIMEDOUT && errno != EHOSTUNREACH) {
            perror("Failed to connect to bootstrap server");
            close(client_socket);
            return -1;
        }
        if (++attempts >= 300) { // ~30s total
            perror("Failed to connect to bootstrap server");
            close(client_socket);
            return -1;
        }
        usleep(100 * 1000); // 100 ms
    }
    
    return client_socket;
}

/**
 * Send data reliably over TCP socket
 * 
 * Ensures all data is sent by handling partial sends.
 * 
 * @param socket_fd: TCP socket to send on
 * @param data_buffer: Buffer containing data to send
 * @param data_size: Number of bytes to send
 * @return: PG_SUCCESS on complete send, PG_ERROR on failure
 */
static int send_data_reliably(int socket_fd, const void *data_buffer, size_t data_size) {
    const char *byte_buffer = (const char*)data_buffer;
    size_t bytes_sent = 0;
    
    while (bytes_sent < data_size) {
        ssize_t result = send(socket_fd, byte_buffer + bytes_sent, 
                             data_size - bytes_sent, 0);
        
        if (result <= 0) {
            if (result == 0) {
                fprintf(stderr, "TCP connection closed during send\n");
            } else {
                perror("TCP send failed");
            }
            return PG_ERROR;
        }
        
        bytes_sent += result;
    }
    
    return PG_SUCCESS;
}

/**
 * Receive data reliably over TCP socket
 * 
 * Ensures all expected data is received by handling partial receives.
 * 
 * @param socket_fd: TCP socket to receive from
 * @param data_buffer: Buffer to store received data
 * @param data_size: Number of bytes to receive
 * @return: PG_SUCCESS on complete receive, PG_ERROR on failure
 */
static int receive_data_reliably(int socket_fd, void *data_buffer, size_t data_size) {
    char *byte_buffer = (char*)data_buffer;
    size_t bytes_received = 0;
    
    while (bytes_received < data_size) {
        ssize_t result = recv(socket_fd, byte_buffer + bytes_received, 
                             data_size - bytes_received, 0);
        
        if (result <= 0) {
            if (result == 0) {
                fprintf(stderr, "TCP connection closed during receive\n");
            } else {
                perror("TCP receive failed");
            }
            return PG_ERROR;
        }
        
        bytes_received += result;
    }
    
    return PG_SUCCESS;
}

/*
 * =============================================================================
 * Hostname Management and Process Discovery Implementation
 * =============================================================================
 */

char **pgnet_parse_hostname_list(const char *server_list_string, int *hostname_count_ptr) {
    PG_CHECK_NULL_PTR(server_list_string, "Server list string is NULL");
    PG_CHECK_NULL_PTR(hostname_count_ptr, "Hostname count pointer is NULL");
    
    /* Create working copy of the input string */
    char *list_copy = strdup(server_list_string);
    if (!list_copy) {
        fprintf(stderr, "Failed to allocate memory for hostname parsing\n");
        return NULL;
    }
    
    /* Allocate array for hostname pointers */
    char **hostname_array = malloc(PG_MAX_PROCESS_COUNT * sizeof(char*));
    if (!hostname_array) {
        fprintf(stderr, "Failed to allocate memory for hostname array\n");
        free(list_copy);
        return NULL;
    }
    
    /* Parse space-separated hostnames */
    int hostname_count = 0;
    char *current_token = strtok(list_copy, " \t\n");
    
    while (current_token && hostname_count < PG_MAX_PROCESS_COUNT) {
        /* Duplicate each hostname string */
        hostname_array[hostname_count] = strdup(current_token);
        if (!hostname_array[hostname_count]) {
            fprintf(stderr, "Failed to allocate memory for hostname %d\n", hostname_count);
            /* Clean up previously allocated hostnames */
            for (int i = 0; i < hostname_count; i++) {
                free(hostname_array[i]);
            }
            free(hostname_array);
            free(list_copy);
            return NULL;
        }
        
        hostname_count++;
        current_token = strtok(NULL, " \t\n");
    }
    
    *hostname_count_ptr = hostname_count;
    free(list_copy);
    return hostname_array;
}

void pgnet_free_hostname_list(char **hostname_array, int hostname_count) {
    if (!hostname_array) {
        return;
    }
    
    /* Free individual hostname strings */
    for (int i = 0; i < hostname_count; i++) {
        free(hostname_array[i]);
    }
    
    /* Free the array itself */
    free(hostname_array);
}

int pgnet_determine_process_rank(char **hostname_array, int process_group_size) {
    PG_CHECK_NULL(hostname_array, "Hostname array is NULL");
    
    /* Allow explicit override via environment for multi-rank-per-host cases */
    const char *env_rank = getenv("PG_RANK");
    if (env_rank && *env_rank) {
        int r = atoi(env_rank);
        if (r >= 0 && r < process_group_size) {
            fprintf(stderr, "[PG] Using rank from PG_RANK=%d\n", r);
            return r;
        } else {
            fprintf(stderr, "[PG] PG_RANK=%d out of range [0,%d)\n", r, process_group_size);
            return -1;
        }
    }
    
    /* Get local hostname */
    char local_hostname[PG_HOSTNAME_MAX_LENGTH];
    if (gethostname(local_hostname, sizeof(local_hostname)) != 0) {
        perror("Failed to get local hostname");
        return -1;
    }
    
    /* Search for local hostname in the process group list */
    for (int rank = 0; rank < process_group_size; rank++) {
        if (hostname_array[rank] && 
            strcmp(local_hostname, hostname_array[rank]) == 0) {
            return rank;
        }
    }
    
    fprintf(stderr, "Local hostname '%s' not found in process group\n", local_hostname);
    return -1;
}

/*
 * =============================================================================
 * TCP Bootstrap Connection Management Implementation
 * =============================================================================
 */

int pgnet_establish_tcp_connection(const char *target_hostname, 
                                  int tcp_port, 
                                  int is_server_mode) {
    if (is_server_mode) {
        /* Create server socket and accept one connection */
        return setup_bootstrap_server(tcp_port);
    } else {
        /* Create client socket and connect to server */
        PG_CHECK_NULL(target_hostname, "Target hostname is NULL for client connection");
        return setup_bootstrap_client(target_hostname, tcp_port);
    }
}

/*
 * =============================================================================
 * RDMA Bootstrap Information Exchange Implementation
 * =============================================================================
 */

int pgnet_exchange_rdma_bootstrap_info(int tcp_socket_fd, 
                                      rdma_qp_bootstrap_info_t *local_qp_info, 
                                      rdma_qp_bootstrap_info_t *remote_qp_info, 
                                      int is_client_mode) {
    PG_CHECK_NULL(local_qp_info, "Local QP info is NULL");
    PG_CHECK_NULL(remote_qp_info, "Remote QP info is NULL");
    
    if (tcp_socket_fd < 0) {
        fprintf(stderr, "Invalid TCP socket descriptor\n");
        return PG_ERROR;
    }
    
    /*
     * Exchange bootstrap information in deterministic order:
     * - Client sends first, then receives
     * - Server receives first, then sends
     * This prevents deadlocks in the exchange process
     */
    if (is_client_mode) {
        /* Client: Send local info first */
        if (send_data_reliably(tcp_socket_fd, local_qp_info, 
                              sizeof(*local_qp_info)) != PG_SUCCESS) {
            fprintf(stderr, "Failed to send local QP bootstrap info\n");
            return PG_ERROR;
        }
        
        /* Client: Then receive remote info */
        if (receive_data_reliably(tcp_socket_fd, remote_qp_info, 
                                 sizeof(*remote_qp_info)) != PG_SUCCESS) {
            fprintf(stderr, "Failed to receive remote QP bootstrap info\n");
            return PG_ERROR;
        }
    } else {
        /* Server: Receive remote info first */
        if (receive_data_reliably(tcp_socket_fd, remote_qp_info, 
                                 sizeof(*remote_qp_info)) != PG_SUCCESS) {
            fprintf(stderr, "Failed to receive remote QP bootstrap info\n");
            return PG_ERROR;
        }
        
        /* Server: Then send local info */
        if (send_data_reliably(tcp_socket_fd, local_qp_info, 
                              sizeof(*local_qp_info)) != PG_SUCCESS) {
            fprintf(stderr, "Failed to send local QP bootstrap info\n");
            return PG_ERROR;
        }
    }
    
    return PG_SUCCESS;
}
