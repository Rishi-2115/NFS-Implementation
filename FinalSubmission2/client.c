#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <errno.h>
#include <stdbool.h>
#include "time.h"
#include "dirent.h"
#include "sys/time.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "sys/socket.h"
#include "netinet/in.h"
#include <arpa/inet.h>
#include <poll.h>
#include <termios.h>

// #define NM_IP "192.168.133.76"
#define NM_IP "127.0.0.1"
#define NM_PORT 8080
#define BUFFER_SIZE 4096
#define MAX_COMMAND_LENGTH 1024
#define MAX_PATH_LENGTH 256

typedef struct
{
    char ip[16];
    int port;
} ServerInfo;

void handle_read(int sock, const char *path);
void handle_write(int sock, const char *path, int issync);
void handle_info(int sock, const char *path);
void handle_stream(int sock, const char *path);
bool check_mpv_installed(void);

void print_help(void);
void trim_newline(char *str);

ServerInfo get_storage_server(char *cmd, char *arg);

void print_help()
{
    printf("Commands:\n");
    printf("  read <path>    - Read the contents of a file\n");
    printf("  write <path>   - Write text to a file\n");
    printf("  info <path>    - Get information about a file\n");
    printf("  stream <path>  - Stream audio from a file\n");
    printf("  help           - Show this help message\n");
    printf("  exit           - Quit the program\n");
    printf("\n");
}

void trim_newline(char *str)
{
    size_t len = strlen(str);
    if (len > 0 && str[len - 1] == '\n')
    {
        str[len - 1] = '\0';
    }
}

void write_again(char *path)
{
    int naming_sock;
    struct sockaddr_in naming_addr;

    naming_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (naming_sock < 0)
    {
        perror("Naming server socket creation failed");
        return;
    }

    memset(&naming_addr, 0, sizeof(naming_addr));
    naming_addr.sin_family = AF_INET;
    naming_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &naming_addr.sin_addr) <= 0)
    {
        perror("Invalid naming server IP address");
        close(naming_sock);
        return;
    }

    if (connect(naming_sock, (struct sockaddr *)&naming_addr, sizeof(naming_addr)) < 0)
    {
        perror("Naming server connection failed");
        close(naming_sock);
        return;
    }
    char request[MAX_COMMAND_LENGTH + MAX_PATH_LENGTH];
    snprintf(request, sizeof(request), "DONE: This file has be written %s", path);

    if (send(naming_sock, request, strlen(request), 0) < 0)
    {
        perror("Failed to send request to naming server");
        close(naming_sock);
        return;
    }
}

ServerInfo get_storage_server(char *cmd, char *path)
{
    ServerInfo server_info = {0};
    int naming_sock;
    struct sockaddr_in naming_addr;
    struct timeval timeout;

    // Set timeout only for initial ACK
    timeout.tv_sec = 5; // 5 second timeout for ACK
    timeout.tv_usec = 0;

    naming_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (naming_sock < 0)
    {
        perror("Naming server socket creation failed");
        server_info.port = 0;
        return server_info;
    }

    memset(&naming_addr, 0, sizeof(naming_addr));
    naming_addr.sin_family = AF_INET;
    naming_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &naming_addr.sin_addr) <= 0)
    {
        perror("Invalid naming server IP address");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    if (connect(naming_sock, (struct sockaddr *)&naming_addr, sizeof(naming_addr)) < 0)
    {
        perror("Naming server connection failed");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    char request[MAX_COMMAND_LENGTH + MAX_PATH_LENGTH];
    snprintf(request, sizeof(request), "%s %s", cmd, path);
    printf("Sending request: %s\n", request);

    if (send(naming_sock, request, strlen(request), 0) < 0)
    {
        perror("Failed to send request to naming server");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    // Set socket timeout only for ACK reception
    if (setsockopt(naming_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        perror("Failed to set receive timeout");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    // Wait for initial acknowledgment with timeout
    char ack_buffer[32];
    ssize_t received = recv(naming_sock, ack_buffer, sizeof(ack_buffer) - 1, 0);

    if (received < 0)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            printf("Request acknowledgment timed out\n");
        }
        else
        {
            perror("Failed to receive acknowledgment");
        }
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    ack_buffer[received] = '\0';
    if (strcmp(ack_buffer, "ACK") != 0)
    {
        printf("Invalid acknowledgment received\n");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    printf("Received request recipt  acknowledgment\n");
    // Remove timeout for normal data reception
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    if (setsockopt(naming_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        perror("Failed to remove timeout");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    char response[256] = {0};
    int bytes_received = recv(naming_sock, response, sizeof(response) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive response from naming server");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    response[bytes_received] = '\0';

    if (strncmp(response, "ERROR:", 6) == 0)
    {
        printf("%s\n", response + 7);
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    if (sscanf(response, "%s %d", server_info.ip, &server_info.port) != 2)
    {
        fprintf(stderr, "Invalid response format from naming server\n");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    printf("Retrieved storage server - IP: %s, Port: %d\n", server_info.ip, server_info.port);
    close(naming_sock);
    return server_info;
}

int main()
{
    char command[MAX_COMMAND_LENGTH];
    char path[MAX_PATH_LENGTH];
    struct sockaddr_in server_addr;
    int current_sock = -1;

    printf("Type 'HELP' for commands, 'EXIT' to QUIT\n\n");

    while (1)
    {
        printf("\n> ");
        if (fgets(command, MAX_COMMAND_LENGTH, stdin) == NULL)
        {
            break;
        }
        trim_newline(command);

        if (strlen(command) == 0)
        {
            continue;
        }

        if (strcmp(command, "EXIT") == 0 || strcmp(command, "QUIT") == 0)
        {
            break;
        }

        if (strcmp(command, "HELP") == 0)
        {
            print_help();
            continue;
        }

        char *cmd = strtok(command, " ");
        char *first_arg = strtok(NULL, " ");
        char *second_arg = strtok(NULL, " ");
        if (cmd == NULL)
        {
            printf("Invalid command. Type 'help' for usage.\n");
            continue;
        }

        if (first_arg == NULL && strcmp(cmd, "HELP") != 0)
        {

            if (strcmp(cmd, "LISTPATHS") != 0)
            {
                printf("Invalid command. Type 'help' for usage.\n");
                continue;
            }
        }

        if (strcmp(cmd, "CREATE") == 0 || strcmp(cmd, "DELETE") == 0 ||
            strcmp(cmd, "COPY") == 0 || strcmp(cmd, "LISTPATHS") == 0)
        {

            if (strcmp(cmd, "COPY") == 0 && second_arg == NULL)
            {
                printf("COPY command requires source and destination paths.\n");
                continue;
            }

            if (current_sock != -1)
            {
                close(current_sock);
                current_sock = -1;
            }
            int naming_sock = handle_naming_server_command(cmd, first_arg, second_arg);
            if (naming_sock < 0)
            {
                continue;
            }
            close(naming_sock);
            continue;
        }
        if (strcmp(cmd, "READ") == 0 || strcmp(cmd, "WRITE") == 0 ||
            strcmp(cmd, "INFO") == 0 || strcmp(cmd, "STREAM") == 0)
        {
            ServerInfo storage_server = get_storage_server(cmd, first_arg);
            if (storage_server.port == 0)
            {
                continue;
            }

            current_sock = socket(AF_INET, SOCK_STREAM, 0);
            if (current_sock < 0)
            {
                perror("Storage server socket creation failed");
                continue;
            }

            memset(&server_addr, 0, sizeof(server_addr));
            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(storage_server.port);
            inet_pton(AF_INET, storage_server.ip, &server_addr.sin_addr);

            printf("Connected to storage server\n");

            if (connect(current_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
            {
                perror("Storage server connection failed");
                close(current_sock);
                current_sock = -1;
                continue;
            }
            if (strcmp(cmd, "READ") == 0)
            {
                handle_read(current_sock, first_arg);
            }
            else if (strcmp(cmd, "WRITE") == 0)
            {
                if (second_arg == NULL)
                {
                    handle_write(current_sock, first_arg, 0);
                }
                else if (strcmp(second_arg, "--SYNC") == 0)
                {
                    handle_write(current_sock, first_arg, 1);
                }
                else
                {
                    printf("Unknown command: %s\nType 'help' for usage.\n", cmd);
                }
            }
            else if (strcmp(cmd, "INFO") == 0)
            {
                handle_info(current_sock, first_arg);
            }
            else if (strcmp(cmd, "STREAM") == 0)
            {
                if (!check_mpv_installed())
                {
                    printf("Error: mpv player is not installed. Please install it first.\n");
                    continue;
                }
                handle_stream(current_sock, first_arg);
                printf("Stream ended\n");
            }
        }
        else
        {
            printf("Unknown command: %s\nType 'help' for usage.\n", cmd);
            continue;
        }

        close(current_sock);
        current_sock = -1;
    }

    if (current_sock != -1)
    {
        close(current_sock);
    }

    printf("Goodbye!\n");
    return 0;
}

void handle_read(int sock, const char *path)
{
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "READ %s", path);
    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Send failed");
        return;
    }
    printf("Sent read request\n");

    char size_buffer[BUFFER_SIZE];
    ssize_t bytes_received = recv(sock, size_buffer, sizeof(size_buffer) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive file size");
        return;
    }
    size_buffer[bytes_received] = '\0';
    long file_size = atol(size_buffer);
    printf("File size received: %ld bytes\n", file_size);

    if (file_size <= 0)
    {
        fprintf(stderr, "Invalid file size received: %ld\n", file_size);
        return;
    }

    const char *ack = "Received size";
    if (send(sock, ack, strlen(ack), 0) < 0)
    {
        perror("Acknowledgment send failed");
        return;
    }

    printf("\n--- Content of %s ---\n", path);

    char buffer[BUFFER_SIZE];
    long total_bytes_received = 0;

    while (total_bytes_received < file_size)
    {
        ssize_t chunk_size = (file_size - total_bytes_received > sizeof(buffer))
                                 ? sizeof(buffer)
                                 : file_size - total_bytes_received;

        bytes_received = recv(sock, buffer, chunk_size, 0);
        if (bytes_received <= 0)
        {
            perror("Error during data transfer");
            return;
        }

        fwrite(buffer, 1, bytes_received, stdout);
        total_bytes_received += bytes_received;
    }

    printf("\n--- End of content ---\n");
}

void handle_write(int sock, const char *path, int issync)
{
    char request[BUFFER_SIZE];
    char response[BUFFER_SIZE];

    if (issync == 1)
    {
        snprintf(request, sizeof(request), "WRITE %s --SYNC", path);
    }
    else
    {
        snprintf(request, sizeof(request), "WRITE %s", path);
    }
    printf("Sent write request\n");

    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Initial write request failed");
        return;
    }
    if (recv(sock, response, sizeof(response) - 1, 0) <= 0)
    {
        perror("Failed to receive acknowledgment");
        return;
    }

    printf("Enter text to write (Type 'END' on a new line to finish):\n");

    char buffer[BUFFER_SIZE];
    size_t total_size = 0;
    char *accumulated_data = NULL;
    int line_count = 0;

    while (fgets(buffer, sizeof(buffer), stdin) != NULL)
    {
        if (strcmp(buffer, "END\n") == 0)
        {
            break;
        }

        line_count++;
        size_t len = strlen(buffer);
        total_size += len;

        char *temp = realloc(accumulated_data, total_size + 1);
        if (temp == NULL)
        {
            free(accumulated_data);
            perror("Memory allocation failed");
            return;
        }
        accumulated_data = temp;
        memcpy(accumulated_data + total_size - len, buffer, len);
        accumulated_data[total_size] = '\0';
    }

    printf("  - Total lines: %d\n", line_count);
    printf("  - Total size: %zu bytes\n", total_size);

    char size_header[32];
    snprintf(size_header, sizeof(size_header), "SIZE %zu", total_size);

    if (send(sock, size_header, strlen(size_header), 0) < 0)
    {
        free(accumulated_data);
        perror("Failed to send size header");
        return;
    }

    memset(response, 0, sizeof(response));
    if (recv(sock, response, sizeof(response) - 1, 0) <= 0)
    {
        free(accumulated_data);
        perror("Failed to receive size header acknowledgment");
        return;
    }

    size_t bytes_sent = 0;
    int chunk_count = 0;

    while (bytes_sent < total_size)
    {
        chunk_count++;
        size_t chunk_size;
        if (total_size - bytes_sent > BUFFER_SIZE)
        {
            chunk_size = BUFFER_SIZE;
        }
        else
        {
            chunk_size = total_size - bytes_sent;
        }

        if (send(sock, accumulated_data + bytes_sent, chunk_size, 0) < 0)
        {
            free(accumulated_data);
            perror("Failed to send data chunk");
            return;
        }

        memset(response, 0, sizeof(response));

        bytes_sent += chunk_size;
    }

    free(accumulated_data);

    memset(response, 0, sizeof(response));
    int bytes_received = recv(sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        printf("%s\n",response);
        return;
    }

    fprintf(stderr, "Failed to receive final server response\n");
    return;
}

void handle_info(int sock, const char *path)
{
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "INFO %s", path);

    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Send failed");
        return;
    }

    printf("\n--- File Information for %s ---\n", path);

    char response[BUFFER_SIZE];
    int bytes_received = recv(sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        printf("--- End of File Information ---\n\n");
        printf("%s", response);
        return;
    }

    printf("Failed to receive file information\n");
    return;
}

int file_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}

bool check_mpv_installed(void)
{
    return system("which mpv >/dev/null 2>&1") == 0;
}

void handle_stream(int sock, const char *path)
{
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "STREAM %s\n", path);

    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Send failed");
        return;
    }

    char size_header[32];
    memset(size_header, 0, sizeof(size_header));
    ssize_t header_received = 0;

    while (header_received < sizeof(size_header) - 1)
    {
        ssize_t n = recv(sock, size_header + header_received, 1, 0);
        if (n <= 0)
        {
            perror("Failed to receive size header");
            return;
        }
        header_received += n;
        if (size_header[header_received - 1] == '\n')
            break;
    }

    size_t expected_size = 0;
    if (sscanf(size_header, "SIZE:%zu", &expected_size) != 1)
    {
        fprintf(stderr, "Invalid size header received: %s\n", size_header);
        return;
    }

    printf("Starting audio stream (%zu bytes)\n", expected_size);
    printf("Press 'q' to stop playback\n");

    int pipefd[2];
    if (pipe(pipefd) == -1)
    {
        perror("Pipe creation failed");
        return;
    }

    pid_t mpv_pid = fork();
    if (mpv_pid == -1)
    {
        perror("Fork failed");
        close(pipefd[0]);
        close(pipefd[1]);
        return;
    }

    if (mpv_pid == 0)
    {
        close(pipefd[1]);
        dup2(pipefd[0], STDIN_FILENO);
        close(pipefd[0]);

        execlp("mpv", "mpv", "-",
               "--no-terminal",
               "--no-cache",
               "--audio-file-auto=no",
               NULL);
        perror("Failed to start mpv");
        exit(1);
    }

    close(pipefd[0]);

    struct termios old_term, new_term;
    tcgetattr(STDIN_FILENO, &old_term);
    new_term = old_term;
    new_term.c_lflag &= ~(ICANON | ECHO);
    new_term.c_cc[VMIN] = 0;
    new_term.c_cc[VTIME] = 0;
    tcsetattr(STDIN_FILENO, TCSANOW, &new_term);

    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    char buffer[BUFFER_SIZE];
    size_t total_received = 0;
    bool stream_ended = false;
    bool stop_requested = false;
    bool end_marker_received = false;

    struct pollfd fds[2] = {
        {.fd = STDIN_FILENO, .events = POLLIN},
        {.fd = sock, .events = POLLIN}};

    while (!stream_ended && !stop_requested)
    {
        int poll_result = poll(fds, 2, 100);

        if (poll_result > 0)
        {
            if (fds[0].revents & POLLIN)
            {
                char c;
                if (read(STDIN_FILENO, &c, 1) > 0 && c == 'q')
                {
                    printf("\nStopping playback immediately...\n");
                    send(sock, "q", 1, 0);
                    stop_requested = true;
                    break;
                }
            }

            if (fds[1].revents & POLLIN)
            {
                ssize_t bytes_received = recv(sock, buffer, sizeof(buffer), 0);
                if (bytes_received > 0)
                {
                    char *end_marker = memmem(buffer, bytes_received, "\nSTREAM_END_MARKER\n", 18);
                    size_t bytes_to_write;

                    if (end_marker != NULL)
                    {
                        bytes_to_write = end_marker - buffer;
                        stream_ended = true;
                        end_marker_received = true;
                    }
                    else
                    {
                        bytes_to_write = bytes_received;
                    }

                    if (bytes_to_write > 0)
                    {
                        size_t written = 0;
                        while (written < bytes_to_write)
                        {
                            ssize_t n = write(pipefd[1],
                                              buffer + written,
                                              bytes_to_write - written);
                            if (n < 0)
                            {
                                if (errno == EAGAIN || errno == EWOULDBLOCK)
                                {
                                    usleep(1000);
                                    continue;
                                }
                                perror("Write to pipe failed");
                                goto cleanup;
                            }
                            written += n;
                        }
                        total_received += bytes_to_write;
                        printf("\rProgress: %.1f%%", (float)total_received / expected_size * 100);
                        fflush(stdout);
                    }

                    if (stream_ended && !end_marker_received)
                    {
                        continue;
                    }
                }
                else if (bytes_received == 0)
                {
                    printf("\nServer closed connection\n");
                    break;
                }
                else if (errno != EAGAIN && errno != EWOULDBLOCK)
                {
                    perror("Receive error");
                    break;
                }
            }
        }
    }

cleanup:
    if (!stop_requested)
    {
        usleep(500000);
    }

    tcsetattr(STDIN_FILENO, TCSANOW, &old_term);

    fcntl(sock, F_SETFL, flags);

    close(pipefd[1]);

    int status;
    struct timespec timeout = {.tv_sec = 2, .tv_nsec = 0};
    pid_t wait_result;

    while ((wait_result = waitpid(mpv_pid, &status, WNOHANG)) == 0)
    {
        if (nanosleep(&timeout, NULL) < 0)
            break;
    }

    if (wait_result == 0)
    {
        kill(mpv_pid, SIGKILL);
        waitpid(mpv_pid, NULL, 0);
    }

    tcflush(STDIN_FILENO, TCIFLUSH);

    char discard_buffer[1024];
    while (recv(sock, discard_buffer, sizeof(discard_buffer), MSG_DONTWAIT) > 0)
        ;

    printf("\nPlayback %s\n",
           stop_requested ? "stopped by user" : stream_ended ? "completed"
                                                             : "ended unexpectedly");
}

int handle_naming_server_command(const char *cmd, const char *first_arg, const char *second_arg)
{
    int naming_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (naming_sock < 0)
    {
        perror("Naming server socket creation failed");
        return -1;
    }

    struct sockaddr_in naming_addr;
    memset(&naming_addr, 0, sizeof(naming_addr));
    naming_addr.sin_family = AF_INET;
    naming_addr.sin_port = htons(NM_PORT);
    inet_pton(AF_INET, NM_IP, &naming_addr.sin_addr);

    if (connect(naming_sock, (struct sockaddr *)&naming_addr, sizeof(naming_addr)) < 0)
    {
        perror("Naming server connection failed");
        close(naming_sock);
        return -1;
    }

    int a = 1;
    printf("cmd: %s\n", cmd);
    char request[MAX_COMMAND_LENGTH + 2 * MAX_PATH_LENGTH];
    if (second_arg == NULL || first_arg == NULL)
    {
        if (strcmp(cmd, "LISTPATHS") == 0)
        {
            snprintf(request, sizeof(request), "%s", cmd);
            a = 0;
        }
        else
        {
            printf("Invalid command. Type 'help' for usage.\n");
            return -1;
        }
    }

    if (a == 1)
        snprintf(request, sizeof(request), "%s %s %s", cmd, first_arg, second_arg);
    printf("Sending command: %s\n", request);

    if (send(naming_sock, request, strlen(request), 0) < 0)
    {
        perror("Failed to send command to naming server");
        close(naming_sock);
        return -1;
    }

    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;

    if (setsockopt(naming_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        perror("Failed to set receive timeout");
        close(naming_sock);
        return -1;
    }

    char ack_buffer[32];
    ssize_t received = recv(naming_sock, ack_buffer, sizeof(ack_buffer) - 1, 0);

    if (received < 0)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            printf("Request acknowledgment timed out\n");
        }
        else
        {
            perror("Failed to receive acknowledgment");
        }
        close(naming_sock);
        return -1;
    }

    ack_buffer[received] = '\0';
    if (strcmp(ack_buffer, "ACK") != 0)
    {
        printf("Invalid acknowledgment received\n");
        close(naming_sock);
        return -1;
    }

    printf("Received request receipt acknowledgment\n");

    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    if (setsockopt(naming_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        perror("Failed to remove timeout");
        close(naming_sock);
        return -1;
    }

    char response[100000] = {0};
    int bytes_received = recv(naming_sock, response, sizeof(response) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive response from naming server");
        close(naming_sock);
        return -1;
    }

    response[bytes_received] = '\0';
    printf("%s\n", response);

    return naming_sock;
}
