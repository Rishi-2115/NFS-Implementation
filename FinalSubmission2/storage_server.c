// storage_server.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <dirent.h>
#include <stdbool.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <signal.h>
#include <poll.h>
#include <termios.h>

#define MAX_PATHS 10000
#define NM_IP "127.0.0.1"
#define NM_PORT 8080
#define BUFFER_SIZE 100000
#define ASYNC_THRESHOLD 100
#define MKDIR(dir) mkdir(dir, 0755)

pthread_mutex_t servers_mutex = PTHREAD_MUTEX_INITIALIZER;

int nm_sock;
int client_port;
int num_accessible_paths = 0;
int count = 0;
int server_sock;
int cli_addr;

void *handle_naming_server_commands(void *arg);
void *handle_naming_server_connections(void *arg);
void *handle_client_requests(void *arg);
void *handle_client_connections(void *arg);

void register_with_naming_server();
int create_nested_directories(const char *path);
void create(char *command);
void delete(char *command);
void copy(char *command);
void copy_file(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int final);
void copy_directory(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int final);

void read_file(int client_sock, const char *path);
void write_file(int client_sock, const char *path, int is_sync);
void get_file_info(int client_sock, const char *path);
void stream_audio(int client_sock, const char *path);

void gather_paths(const char *base_path);
char *get_name_from_path(const char *path);
void get_parent_path(const char *full_path, char *parent_path, size_t size);
void list(int client_sock, char *path);

typedef struct
{
    char *data;
    size_t size;
    char path[BUFFER_SIZE];
    bool is_sync;
    char server_id[32];
} WriteRequest;

char *accessible_paths[1000];
int used_paths[1000];

void gather_paths(const char *base_path)
{
    accessible_paths[num_accessible_paths] = strdup(base_path);
    used_paths[num_accessible_paths] = 0;
    num_accessible_paths++;
    DIR *dir = opendir(base_path);
    if (dir == NULL)
    {
        perror("Failed to open directory");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL)
    {
        if (entry->d_name[0] == '.')
        {
            continue;
        }

        char full_path[BUFFER_SIZE];
        snprintf(full_path, sizeof(full_path), "%s/%s", base_path, entry->d_name);

        struct stat path_stat;
        if (stat(full_path, &path_stat) == 0)
        {
            if (S_ISDIR(path_stat.st_mode))
            {
                gather_paths(full_path);
            }
            else if (S_ISREG(path_stat.st_mode))
            {
                if (num_accessible_paths < MAX_PATHS)
                {
                    accessible_paths[num_accessible_paths] = strdup(full_path);
                    used_paths[num_accessible_paths] = 0;
                    num_accessible_paths++;
                }
                else
                {
                    fprintf(stderr, "Exceeded maximum accessible paths limit!\n");
                    closedir(dir);
                    return;
                }
            }
        }
    }

    closedir(dir);
}

char *get_name_from_path(const char *path)
{
    char *last_slash = strrchr(path, '/');
    if (last_slash != NULL)
    {
        return last_slash + 1;
    }
    return (char *)path;
}

void get_parent_path(const char *full_path, char *parent_path, size_t size)
{
    strncpy(parent_path, full_path, size);
    parent_path[size - 1] = '\0';
    char *last_slash = strrchr(parent_path, '/');
    if (last_slash != NULL)
    {
        *last_slash = '\0';
    }
}

int create_nested_directories(const char *path)
{
    char buffer[1024];
    char *p;

    strncpy(buffer, path, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';

    p = buffer;
    while (*p == '/')
        p++;

    while (*p != '\0')
    {
        char *next = p;
        while (*next != '\0' && *next != '/')
            next++;

        char save = *next;
        *next = '\0';

        if (MKDIR(buffer) == -1)
        {
            if (errno != EEXIST)
            {
                char error_msg[BUFFER_SIZE];
                snprintf(error_msg, sizeof(error_msg), "Unable to make directory");
                send(nm_sock, error_msg, strlen(error_msg), 0);
                return -1;
            }
        }

        *next = save;

        p = (*next == '/') ? next + 1 : next;
    }
    send(nm_sock, "Created successfully", 24, 0);
    return 0;
}

void list(int client_sock, char *path)
{
    struct dirent *entry;
    DIR *dir = opendir(path);

    if (dir == NULL)
    {
        perror("Unable to open directory");
        return;
    }

    char buffer[BUFFER_SIZE];
    buffer[0] = '\0';
    while ((entry = readdir(dir)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }
        if (entry->d_type == DT_DIR)
        {
            strcat(buffer, "DIR ");
            strcat(buffer, entry->d_name);
            strcat(buffer, "\n");
        }
        else
        {
            strcat(buffer, "FILE ");
            strcat(buffer, entry->d_name);
            strcat(buffer, "\n");
        }
    }
    send(client_sock, buffer, strlen(buffer), 0);
    closedir(dir);
    return buffer;
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        printf("Usage: %s <clientport> <accessible_paths>\n", argv[0]);
        return -1;
    }

    client_port = atoi(argv[1]);
    for (int i = 2; i < argc; i++)
    {
        gather_paths(argv[i]);
    }

    register_with_naming_server();

    pthread_t nm_thread, client_thread;

    int client_sock;
    struct sockaddr_in server_addr;

    server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
    {
        perror("Failed to create client socket");
        return 1;
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(client_port);

    if (bind(server_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("Failed to bind client socket");
        close(server_sock);
        return 1;
    }

    listen(server_sock, 5);

    if (pthread_create(&client_thread, NULL, handle_client_connections, NULL) != 0)
    {
        perror("Failed to create client thread");
        exit(EXIT_FAILURE);
    }

    if (pthread_create(&nm_thread, NULL, handle_naming_server_connections, NULL) != 0)
    {
        perror("Failed to create naming server thread");
        exit(EXIT_FAILURE);
    }

    printf("Storage Server is ready for naming server connections on port %d...\n", NM_PORT);
    printf("Storage Server is ready for client connections on port %d...\n", client_port);

    pthread_join(nm_thread, NULL);
    pthread_join(client_thread, NULL);

    return 0;
}

void register_with_naming_server(void)
{
    struct sockaddr_in serv_addr;
    nm_sock = -1;

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(NM_PORT);
    inet_pton(AF_INET, NM_IP, &serv_addr.sin_addr);

    printf("Attempting to connect to Naming Server...\n");

    while (1)
    {
        nm_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (nm_sock < 0)
        {
            perror("Failed to create socket");
            return;
        }

        if (connect(nm_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) == 0)
        {
            printf("Connected to Naming Server successfully!\n");
            break;
        }

        perror("Failed to connect to Naming Server, retrying...");
        close(nm_sock);
        nm_sock = -1;
        sleep(5);
    }

    char indicator[BUFFER_SIZE];
    snprintf(indicator, sizeof(indicator), "Storage Server");

    printf("Registering with Naming Server...\n");
    if (send(nm_sock, indicator, strlen(indicator), 0) < 0)
    {
        perror("Failed to send indicator to Naming Server");
        close(nm_sock);
        exit(1);
    }

    printf("Sent indicator to Naming Server: %s\n", indicator);

    char hostname_ip[BUFFER_SIZE] = "Unknown";
    FILE *fp = popen("hostname -I", "r");
    if (fp)
    {
        if (fgets(hostname_ip, sizeof(hostname_ip), fp) != NULL)
        {
            hostname_ip[strcspn(hostname_ip, "\n")] = '\0';
            hostname_ip[strcspn(hostname_ip, " ")] = '\0';
        }
        pclose(fp);
    }
    else
    {
        perror("Failed to execute 'hostname -I'");
    }

    char registration_info[BUFFER_SIZE];
    snprintf(registration_info, sizeof(registration_info),
             "Storage Server: Client Port: %d, IP: %s, Paths: ", client_port, hostname_ip);

    for (int i = 0; i < num_accessible_paths; i++)
    {
        strncat(registration_info, accessible_paths[i], sizeof(registration_info) - strlen(registration_info) - 1);
        if (i < num_accessible_paths - 1)
        {
            strncat(registration_info, ", ", sizeof(registration_info) - strlen(registration_info) - 1);
        }
    }

    usleep(1000);

    if (send(nm_sock, registration_info, strlen(registration_info), 0) < 0)
    {
        perror("Failed to send registration information");
        close(nm_sock);
        exit(1);
    }
    printf("Sent registration to Naming Server\n");

    char confirmation[BUFFER_SIZE];
    int bytes_received = recv(nm_sock, confirmation, sizeof(confirmation) - 1, 0);
    if (bytes_received > 0)
    {
        confirmation[bytes_received] = '\0';
        printf("Received confirmation from Naming Server: %s\n", confirmation);
        const char *success_msg = "Registration completed";
        send(nm_sock, success_msg, strlen(success_msg), 0);
    }
    else
    {
        perror("Failed to receive confirmation from Naming Server");
        close(nm_sock);
        exit(1);
    }

    serv_addr.sin_port = htons(9999);

    printf("Attempting to connect to PING_PORT (9999)...\n");

    while (1)
    {
        int ping_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (ping_sock < 0)
        {
            perror("Failed to create ping socket");
            return;
        }

        if (connect(ping_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) == 0)
        {
            printf("Connected to PING_PORT successfully!\n");
            break;
        }

        perror("Failed to connect to PING_PORT, retrying...");
        close(ping_sock);
        ping_sock = -1;
        sleep(5);
    }
}

void *handle_naming_server_connections(void *arg)
{
    char command[BUFFER_SIZE];
    ssize_t bytes_received;

    while (1)
    {
        printf("Storage Server listening for NM commands...\n");

        memset(command, 0, BUFFER_SIZE);

        bytes_received = recv(nm_sock, command, sizeof(command) - 1, 0);

        if (bytes_received <= 0)
        {
            if (bytes_received == 0)
            {
                printf("Connection with naming server closed\n");

                register_with_naming_server();

                pthread_t nm_thread, client_thread;

                if (pthread_create(&nm_thread, NULL, handle_naming_server_connections, NULL) != 0)
                {
                    perror("Failed to create naming server thread");
                    exit(EXIT_FAILURE);
                }

                if (pthread_create(&client_thread, NULL, handle_client_connections, NULL) != 0)
                {
                    perror("Failed to create client thread");
                    exit(EXIT_FAILURE);
                }

                printf("Storage Server is ready for naming server connections on port %d...\n", NM_PORT);
                printf("Storage Server is ready for client connections on port %d...\n", client_port);

                pthread_join(nm_thread, NULL);
                pthread_join(client_thread, NULL);
            }
            else
            {
                perror("Error receiving data from naming server");
            }
            break;
        }

        command[bytes_received] = '\0';

        char *thread_command = strdup(command);
        if (!thread_command)
        {
            perror("Failed to allocate memory for command");
            continue;
        }

        pthread_t command_thread;
        if (pthread_create(&command_thread, NULL, handle_naming_server_commands, thread_command) != 0)
        {
            perror("Failed to create thread for naming server command");
            free(thread_command);
            continue;
        }
        pthread_detach(command_thread);
    }

    close(nm_sock);
    pthread_exit(NULL);
}

void create(char *command)
{
    char type[10], full_path[BUFFER_SIZE];
    if (sscanf(command, "CREATE %s %s", type, full_path) != 2)
    {
        send(nm_sock, "2 Invalid input command format", 31, 0);
        return;
    }

    char *name = get_name_from_path(full_path);
    char parent_path[BUFFER_SIZE];
    get_parent_path(full_path, parent_path, sizeof(parent_path));

    if (access(parent_path, W_OK) != 0)
    {
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Parent directory %s is not accessible", parent_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    if (strcmp(type, "FILE") == 0)
    {
        char file_path[BUFFER_SIZE];

        if (snprintf(file_path, sizeof(file_path), "%s/%s", parent_path, name) >= sizeof(file_path))
        {
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "4 Buffer limit exceeded");
            send(nm_sock, error_msg, strlen(error_msg), 0);
            return;
        }

        int fd = open(file_path, O_CREAT | O_WRONLY, 0644);
        if (fd < 0)
        {
            perror("Failed to create file");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to create file %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);
        }
        else
        {
            printf("Created empty file: %s\n", file_path);
            close(fd);
            char success_msg[BUFFER_SIZE];
            snprintf(success_msg, sizeof(success_msg), "0 File created: %s", name);
            send(nm_sock, success_msg, strlen(success_msg), 0);

            pthread_mutex_lock(&servers_mutex);
            accessible_paths[num_accessible_paths] = strdup(parent_path);
            used_paths[num_accessible_paths] = 0;
            num_accessible_paths++;
            pthread_mutex_unlock(&servers_mutex);
        }
    }
    else if (strcmp(type, "DIR") == 0)
    {
        char dir_path[BUFFER_SIZE];
        if (snprintf(dir_path, sizeof(dir_path), "%s/%s", parent_path, name) >= sizeof(dir_path))
        {
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "4 Buffer limit exceeded");
            send(nm_sock, error_msg, strlen(error_msg), 0);
            return;
        }

        if (mkdir(dir_path, 0755) < 0)
        {
            perror("Failed to create directory");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to create directory %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);
        }
        else
        {
            printf("Created directory: %s\n", dir_path);
            char success_msg[BUFFER_SIZE];
            snprintf(success_msg, sizeof(success_msg), "0 Directory created: %s", name);
            send(nm_sock, success_msg, strlen(success_msg), 0);

            pthread_mutex_lock(&servers_mutex);
            accessible_paths[num_accessible_paths] = strdup(parent_path);
            used_paths[num_accessible_paths] = 0;
            num_accessible_paths++;
            pthread_mutex_unlock(&servers_mutex);
        }
    }
    else
    {
        printf("Unknown type: %s\n", type);
        send(nm_sock, "2 Unknown type", 18, 0);
    }
}

int delete_directory_contents(const char *path)
{
    DIR *dir = opendir(path);
    if (!dir)
    {
        perror("Failed to open directory");
        return -1;
    }

    struct dirent *entry;
    char file_path[BUFFER_SIZE];

    while ((entry = readdir(dir)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        snprintf(file_path, sizeof(file_path), "%s/%s", path, entry->d_name);
        if (entry->d_type == DT_DIR)
        {

            pthread_mutex_lock(&servers_mutex);
            int index = -1;
            for (int i = 0; i < num_accessible_paths; i++)
            {
                if (strcmp(accessible_paths[i], file_path) == 0)
                {
                    index = i;
                    break;
                }
            }
            pthread_mutex_unlock(&servers_mutex);

            if (index != -1)
            {
                while (used_paths[index] == 1)
                {
                }
            }

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 1;
            }
            pthread_mutex_unlock(&servers_mutex);

            if (delete_directory_contents(file_path) < 0)
            {
                closedir(dir);
                return -1;
            }
            if (rmdir(file_path) < 0)
            {
                perror("Failed to delete subdirectory");
                closedir(dir);

                pthread_mutex_lock(&servers_mutex);
                if (index != -1)
                {
                    used_paths[index] = 0;
                }
                pthread_mutex_unlock(&servers_mutex);

                return -1;
            }
        }
        else
        {

            pthread_mutex_lock(&servers_mutex);
            int index = -1;
            for (int i = 0; i < num_accessible_paths; i++)
            {
                if (strcmp(accessible_paths[i], file_path) == 0)
                {
                    index = i;
                    break;
                }
            }
            pthread_mutex_unlock(&servers_mutex);

            if (index != -1)
            {
                while (used_paths[index] == 1)
                {
                }
            }

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 1;
            }
            pthread_mutex_unlock(&servers_mutex);

            if (remove(file_path) < 0)
            {
                perror("Failed to delete file");
                closedir(dir);

                pthread_mutex_lock(&servers_mutex);
                if (index != -1)
                {
                    used_paths[index] = 0;
                }
                pthread_mutex_unlock(&servers_mutex);
                return -1;
            }
        }
    }
    closedir(dir);
    return 0;
}

char *collect_directory_paths(const char *dir_path, char *path_list, size_t max_size)
{
    DIR *dir;
    struct dirent *entry;
    char full_path[BUFFER_SIZE];
    size_t current_length = 0;

    current_length = snprintf(path_list, max_size, "%s", dir_path);

    dir = opendir(dir_path);
    if (dir != NULL)
    {
        while ((entry = readdir(dir)) != NULL)
        {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            {
                continue;
            }

            snprintf(full_path, BUFFER_SIZE, "%s/%s", dir_path, entry->d_name);

            struct stat path_stat;
            stat(full_path, &path_stat);

            if (current_length > 0)
            {
                current_length += snprintf(path_list + current_length,
                                           max_size - current_length, "|");
            }

            current_length += snprintf(path_list + current_length,
                                       max_size - current_length, "%s", full_path);

            if (S_ISDIR(path_stat.st_mode))
            {
                collect_directory_paths(full_path, path_list + current_length,
                                        max_size - current_length);
                current_length = strlen(path_list);
            }
        }
        closedir(dir);
    }
    return path_list;
}

void delete(char *command)
{
    char type[10], full_path[BUFFER_SIZE];
    if (sscanf(command, "DELETE %s %s", type, full_path) != 2)
    {
        send(nm_sock, "2 Invalid input command format", 33, 0);
        return;
    }
    char *name = get_name_from_path(full_path);
    char parent_path[BUFFER_SIZE];
    get_parent_path(full_path, parent_path, sizeof(parent_path));

    if (access(parent_path, W_OK) != 0)
    {
        perror("No such folder exist");
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Parent directory %s is not accessible", parent_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    if (strcmp(type, "FILE") == 0)
    {
        char file_path[BUFFER_SIZE];
        if (snprintf(file_path, sizeof(file_path), "%s/%s", parent_path, name) >= sizeof(file_path))
        {
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "4 Buffer limit exceeded");
            send(nm_sock, error_msg, strlen(error_msg), 0);
            return;
        }

        if (access(file_path, F_OK) != 0)
        {
            perror("No such file exist");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 File %s not found", name);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            return;
        }

        pthread_mutex_lock(&servers_mutex);
        int index = -1;
        for (int i = 0; i < num_accessible_paths; i++)
        {
            if (strcmp(accessible_paths[i], file_path) == 0)
            {
                index = i;
                break;
            }
        }
        pthread_mutex_unlock(&servers_mutex);

        if (index != -1)
        {
            while (used_paths[index] == 1)
            {
            }
        }

        pthread_mutex_lock(&servers_mutex);
        if (index != -1)
        {
            used_paths[index] = 1;
        }
        pthread_mutex_unlock(&servers_mutex);

        if (unlink(file_path) < 0)
        {
            perror("Failed to delete file");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to delete file %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 0;
            }
            pthread_mutex_unlock(&servers_mutex);
        }
        else
        {
            printf("Deleted file: %s\n", file_path);

            char success_msg[BUFFER_SIZE];
            snprintf(success_msg, sizeof(success_msg), "0 Deleted the file %s successfully", file_path);
            send(nm_sock, success_msg, strlen(success_msg), 0);

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 0;
            }
            pthread_mutex_unlock(&servers_mutex);
        }
    }

    else if (strcmp(type, "DIR") == 0)
    {
        char dir_path[BUFFER_SIZE];
        if (snprintf(dir_path, sizeof(dir_path), "%s/%s", parent_path, name) >= sizeof(dir_path))
        {
            send(nm_sock, "4 Buffer limit exceeded", 24, 0);
            return;
        }

        if (access(dir_path, F_OK) != 0)
        {
            perror("No such file exist");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Directory %s not found", name);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            return;
        }

        int index = -1;
        for (int i = 0; i < num_accessible_paths; i++)
        {
            if (strcmp(accessible_paths[i], dir_path) == 0)
            {
                index = i;
                break;
            }
        }
        if (index != -1)
        {
            while (used_paths[index] == 1)
            {
            }
        }

        pthread_mutex_lock(&servers_mutex);
        if (index != -1)
        {
            used_paths[index] = 1;
        }
        pthread_mutex_unlock(&servers_mutex);

        char *path_list = malloc(BUFFER_SIZE * 64);
        if (!path_list)
        {
            send(nm_sock, "4 Memory allocation failed", strlen("4 Memory allocation failed"), 0);
            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 0;
            }
            pthread_mutex_unlock(&servers_mutex);
            return;
        }
        path_list[0] = '\0';

        collect_directory_paths(dir_path, path_list, BUFFER_SIZE * 64);

        if (delete_directory_contents(dir_path) < 0 || rmdir(dir_path) < 0)
        {
            perror("Failed to delete directory");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to delete directory %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 0;
            }
            pthread_mutex_unlock(&servers_mutex);
        }
        else
        {
            printf("Deleted directory: %s\n", dir_path);

            char success_msg[BUFFER_SIZE * 64];
            snprintf(success_msg, sizeof(success_msg), "0 Deleted the dir %s successfully", path_list);
            int bytes_sent = send(nm_sock, success_msg, strlen(success_msg), 0);
            if (bytes_sent < 0)
            {
                perror("Send failed");
            }
            free(path_list);

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 0;
            }
            pthread_mutex_unlock(&servers_mutex);
        }
    }

    else
    {
        printf("Unknown type: %s\n", type);
        send(nm_sock, "2 Unknown type", 18, 0);
    }
}

void copy(char *command)
{
    char type[10], src_path[BUFFER_SIZE], dest_path[BUFFER_SIZE], src_ip[16];
    int src_port;
    if (sscanf(command, "COPY %9s %255s %255s %15s %d", type, src_path, dest_path, src_ip, &src_port) != 5)
    {
        send(nm_sock, "1 Invalid input command format", 31, 0);
        return;
    }

    int src_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (src_sock < 0)
    {
        perror("Failed to create socket");
        send(nm_sock, "ERROR: Socket creation failed", 27, 0);
        return;
    }

    struct sockaddr_in src_addr;
    memset(&src_addr, 0, sizeof(src_addr));
    src_addr.sin_family = AF_INET;
    src_addr.sin_port = htons(src_port);

    if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) <= 0)
    {
        perror("Invalid IP address");
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "5 Invalid IP address %s", src_ip);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        close(src_sock);
    }

    if (connect(src_sock, (struct sockaddr *)&src_addr, sizeof(src_addr)) < 0)
    {
        perror("Failed to connect to source storage server");
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "5 Failed to connect to source server %s", src_ip);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        close(src_sock);
    }

    if (strcmp(type, "FILE") == 0)
    {
        // pthread_mutex_lock(&servers_mutex);
        // int index = -1;
        // for (int i = 0; i < num_accessible_paths; i++)
        // {
        //     if (strcmp(accessible_paths[i], dest_path) == 0)
        //     {
        //         index = i;
        //         break;
        //     }
        // }
        // if (index != -1)
        // {
        //     while (used_paths[index] == 1)
        //     {
        //     }
        // }
        // if (index != -1)
        // {
        //     used_paths[index] = 1;
        // }
        // else
        // {
        //     accessible_paths[num_accessible_paths] = strdup(dest_path);
        //     used_paths[num_accessible_paths] = 1;
        //     index = num_accessible_paths;
        //     num_accessible_paths++;
        // }
        // pthread_mutex_unlock(&servers_mutex);

        copy_file(src_path, dest_path, src_ip, src_sock, 1);

        // pthread_mutex_lock(&servers_mutex);
        // if (index != -1)
        // {
        //     used_paths[index] = 0;
        // }
        // pthread_mutex_unlock(&servers_mutex);
    }
    else if (strcmp(type, "DIR") == 0)
    {
        // pthread_mutex_lock(&servers_mutex);
        // int index = -1;
        // for (int i = 0; i < num_accessible_paths; i++)
        // {
        //     if (strcmp(accessible_paths[i], dest_path) == 0)
        //     {
        //         index = i;
        //         break;
        //     }
        // }
        // if (index != -1)
        // {
        //     while (used_paths[index] == 1)
        //     {
        //     }
        // }
        // if (index != -1)
        // {
        //     used_paths[index] = 1;
        // }
        // else
        // {
        //     accessible_paths[num_accessible_paths] = strdup(dest_path);
        //     used_paths[num_accessible_paths] = 1;
        //     index = num_accessible_paths;
        //     num_accessible_paths++;
        // }
        // pthread_mutex_unlock(&servers_mutex);

        copy_directory(src_path, dest_path, src_ip, src_sock, 1);

        // pthread_mutex_lock(&servers_mutex);
        // if (index != -1)
        // {
        //     used_paths[index] = 0;
        // }
        // pthread_mutex_unlock(&servers_mutex);
    }
    else
    {
        printf("Unknown type: %s\n", type);
        send(nm_sock, "1 Unknown type", 18, 0);
    }

    close(src_sock);
}

void copy_directory(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int final)
{
    if (mkdir(dest_path, 0755) < 0 && errno != EEXIST)
    {
        perror("Failed to create destination directory");
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Failed to create destination directory %s", dest_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "LIST %s", src_path);
    send(src_sock, request, strlen(request), 0);
    char buffer[BUFFER_SIZE];
    char message[BUFFER_SIZE];

    ssize_t bytes_received = recv(src_sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive directory listing");
        snprintf(message, sizeof(message), "5 Failed to receive data from source server %s", src_ip);
        send(nm_sock, message, sizeof(message), 0);
        close(src_sock);
        return;
    }
    buffer[bytes_received] = '\0';
    char copy[BUFFER_SIZE];
    strcpy(copy, buffer);

    char *line = strtok(buffer, "\n");
    int increament = 0;
    while (line != NULL)
    {
        char line_copy[256];
        strcpy(line_copy, line);

        char item_type[10], item_name[256];
        sscanf(line, "%s %s", item_type, item_name);
        char src_item_path[BUFFER_SIZE], dest_item_path[BUFFER_SIZE];
        snprintf(src_item_path, sizeof(src_item_path), "%s/%s", src_path, item_name);
        snprintf(dest_item_path, sizeof(dest_item_path), "%s/%s", dest_path, item_name);

        if (item_name[0] != '.')
        {
            if (strcmp(item_type, "FILE") == 0)
            {
                copy_file(src_item_path, dest_item_path, src_ip, src_sock, 0);
            }
            else if (strcmp(item_type, "DIR") == 0)
            {
                copy_directory(src_item_path, dest_item_path, src_ip, src_sock, 0);
            }
        }
        strcpy(buffer, copy);
        line = strtok(buffer, "\n");
        increament++;
        for (int i = 0; i < increament; i++)
        {
            line = strtok(NULL, "\n");
        }
    }
    if (final)
    {
        close(src_sock);
        char success_msg[BUFFER_SIZE];
        snprintf(success_msg, sizeof(success_msg), "0 Directory copied from %s to %s", src_path, dest_path);
        if (send(nm_sock, success_msg, strlen(success_msg), 0) < 0)
        {
            perror("Failed to send message");
        }
        printf("Directory copied from %s to %s", src_path, dest_path);
    }
}

void copy_file(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int final)
{
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "READ %s", src_path);
    send(src_sock, request, strlen(request), 0);
    char message[BUFFER_SIZE];

    char size_buffer[BUFFER_SIZE];
    ssize_t bytes_received = recv(src_sock, size_buffer, sizeof(size_buffer) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive file size");
        snprintf(message, sizeof(message), "5 Failed to receive data from source server %s", src_ip);
        send(nm_sock, message, sizeof(message), 0);
        close(src_sock);
        return;
    }
    size_buffer[bytes_received] = '\0';
    long file_size = atol(size_buffer);

    snprintf(request, sizeof(request), "Received size");
    send(src_sock, request, strlen(request), 0);

    int dest_fd = open(dest_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dest_fd < 0)
    {
        perror("Failed to create destination file");
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Failed to create destination file %s", dest_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        close(src_sock);
        return;
    }

    char buffer[BUFFER_SIZE];
    long bytes_left = file_size;
    while (bytes_left > 0)
    {
        ssize_t bytes_to_read = (bytes_left > sizeof(buffer)) ? sizeof(buffer) : bytes_left;
        ssize_t bytes_received = recv(src_sock, buffer, bytes_to_read, 0);
        if (bytes_received <= 0)
        {
            perror("Error receiving data from source server");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "5 Failed to receive data from source server %s", src_ip);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            close(src_sock);
            close(dest_fd);
            return;
        }

        if (write(dest_fd, buffer, bytes_received) != bytes_received)
        {
            perror("Failed to write to destination file");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to write to destination file");
            send(nm_sock, error_msg, strlen(error_msg), 0);
            close(src_sock);
            close(dest_fd);
            return;
        }

        bytes_left -= bytes_received;
    }
    printf("file copied\n");

    close(dest_fd);

    if (final == 1)
    {
        pthread_mutex_lock(&servers_mutex);
        close(src_sock);
        char success_msg[BUFFER_SIZE];
        snprintf(success_msg, sizeof(success_msg), "0 File copied from %s to %s", src_path, dest_path);
        printf("File copied from %s to %s", src_path, dest_path);
        if (send(nm_sock, success_msg, strlen(success_msg), 0) < 0)
        {
            perror("Failed to send message");
        }
        pthread_mutex_unlock(&servers_mutex);
    }
}

void *handle_naming_server_commands(void *args)
{
    if (!args)
    {
        perror("Error: NULL thread arguments");
        return NULL;
    }

    char *command = (char *)args;
    char type[10] = {0};
    char src_path[BUFFER_SIZE] = {0};
    char dest_path[BUFFER_SIZE] = {0};
    char src_ip[16] = {0};
    int status = 0;

    printf("Received command from Naming Server: %s\n", command);

    if (strncmp(command, "CREATE", 6) == 0)
    {
        printf("Command: %s\n", command);
        create(command);
    }
    else if (strncmp(command, "DELETE", 6) == 0)
    {
        printf("Command: %s\n", command);
        delete (command);
    }
    else if (strncmp(command, "COPY", 4) == 0)
    {
        printf("Command: %s\n", command);
        copy(command);
    }
    else if (strncmp(command, "NESTED", 6) == 0)
    {
        char path[BUFFER_SIZE];
        sscanf(command, "NESTED: %s", path);
        create_nested_directories(path);
    }
    else
    {
        printf("Unknown command received: %s\n", command);
        send(nm_sock, "ERROR: Unknown command", 20, 0);
    }
    free(command);
    return NULL;
}

void *handle_client_connections(void *arg)
{
    while (1)
    {
        
        socklen_t client_len = sizeof(cli_addr);
        int client_sock = accept(server_sock, (struct sockaddr *)&cli_addr, &client_len);
        if (client_sock < 0)
        {
            perror("Failed to accept client connection");
            continue;
        }

        printf("Accepted connection from Client\n");
        int *client_sock_ptr = (int*)malloc(sizeof(int)*4);
        if (!client_sock_ptr)
        {
            perror("Failed to allocate memory for client socket pointer");
            close(client_sock);
            continue;
        }
        *client_sock_ptr = client_sock;

        pthread_t client_thread;
        pthread_create(&client_thread, NULL, handle_client_requests, client_sock_ptr);
        pthread_detach(client_thread);
    }

    close(server_sock);
    pthread_exit(NULL);
}

void *async_write_thread(void *arg)
{
    WriteRequest *req = (WriteRequest *)arg;


    int fd = open(req->path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (fd < 0)
    {
        perror("Failed to open file");
        return;
    }

    ssize_t bytes_written = write(fd,req->data,req->size);
    if(bytes_written != req->size)
    {
        perror("Failed to write data");
    }
    write(fd, req->data, req->size);
    close(fd);

    pthread_exit(NULL);
}

void *handle_client_requests(void *arg)
{
    int client_sock = *(int *)arg;
    char command[BUFFER_SIZE];
    ssize_t bytes_received;

    printf("Storage Server listening for client requests...\n");

    while ((bytes_received = recv(client_sock, command, sizeof(command) - 1, 0)) > 0)
    {
        command[bytes_received] = '\0';
        printf("Received client command: %s\n", command);

        if (strncmp(command, "READ", 4) == 0)
        {
            char path[BUFFER_SIZE];
            if (sscanf(command, "READ %s", path) != 1)
            {
                send(client_sock, "Invalid input command format", 29, 0);
                break;
            }

            pthread_mutex_lock(&servers_mutex);
            int index = -1;
            for (int i = 0; i < num_accessible_paths; i++)
            {
                if (strcmp(accessible_paths[i], path) == 0)
                {
                    index = i;
                    break;
                }
            }
            pthread_mutex_unlock(&servers_mutex);

            read_file(client_sock, path);
        }
        else if (strncmp(command, "WRITE", 5) == 0)
        {
            char path[BUFFER_SIZE];
            int is_sync = 0;
            char *sync_flag = strstr(command, "--SYNC");
            if (sync_flag)
            {
                is_sync = 1;
                *sync_flag = '\0';
            }
            if (sscanf(command, "WRITE %s", path) != 1)
            {
                send(client_sock, "Invalid input command format", 29, 0);
                break;
            }

            pthread_mutex_lock(&servers_mutex);
            int index = -1;
            for (int i = 0; i < num_accessible_paths; i++)
            {
                if (strcmp(accessible_paths[i], path) == 0)
                {
                    index = i;
                    break;
                }
            }
            pthread_mutex_unlock(&servers_mutex);

            if (index != -1)
            {
                while (used_paths[index] == 1)
                {
                }
            }

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 1;
            }
            pthread_mutex_unlock(&servers_mutex);

            write_file(client_sock, path, is_sync);

            pthread_mutex_lock(&servers_mutex);
            if (index != -1)
            {
                used_paths[index] = 0;
            }
            pthread_mutex_unlock(&servers_mutex);
            sleep(2);        }
        else if (strncmp(command, "INFO", 4) == 0)
        {
            char path[BUFFER_SIZE];
            if (sscanf(command, "INFO %s", path) != 1)
            {
                send(client_sock, "Invalid input command format", 29, 0);
                break;
            }

            pthread_mutex_lock(&servers_mutex);
            int index = -1;
            for (int i = 0; i < num_accessible_paths; i++)
            {
                if (strcmp(accessible_paths[i], path) == 0)
                {
                    index = i;
                    break;
                }
            }
            pthread_mutex_unlock(&servers_mutex);

            if (index != -1)
            {
                while (used_paths[index] == 1)
                {
                }
            }
            get_file_info(client_sock, path);
        }
        else if (strncmp(command, "STREAM", 6) == 0)
        {
            char path[BUFFER_SIZE];
            if (sscanf(command, "STREAM %s", path) != 1)
            {
                send(client_sock, "Invalid input command format", 29, 0);
                break;
            }

            pthread_mutex_lock(&servers_mutex);
            int index = -1;
            for (int i = 0; i < num_accessible_paths; i++)
            {
                if (strcmp(accessible_paths[i], path) == 0)
                {
                    index = i;
                    break;
                }
            }
            pthread_mutex_unlock(&servers_mutex);

            if (index != -1)
            {
                while (used_paths[index] == 1)
                {
                }
            }

            stream_audio(client_sock, path);
        }
        else if (strncmp(command, "LIST", 4) == 0)
        {
            char path[BUFFER_SIZE];
            if (sscanf(command, "LIST %s", path) != 1)
            {
                send(client_sock, "Invalid input command format", 29, 0);
                break;
            }
            list(client_sock, path);
        }
        else
        {
            char error_msg[] = "ERROR: Unknown command\n";
            send(client_sock, error_msg, strlen(error_msg), 0);
        }
    }

    if (bytes_received <= 0)
    {
        perror("Client disconnected");
    }

    return;
}

void read_file(int client_sock, const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        perror("Failed to open file for reading");
        char error_msg[] = "ERROR: Failed to read file\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }
    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        perror("Failed to get file size");
        char error_msg[] = "ERROR: Failed to get file size\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        close(fd);
        return;
    }
    long file_size = st.st_size;

    char size_buffer[BUFFER_SIZE];
    snprintf(size_buffer, sizeof(size_buffer), "%ld", file_size);
    send(client_sock, size_buffer, strlen(size_buffer), 0);

    char client_response[BUFFER_SIZE];
    ssize_t bytes_received = recv(client_sock, client_response, sizeof(client_response) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive client response");
        close(fd);
        return;
    }
    client_response[bytes_received] = '\0';

    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    while ((bytes_read = read(fd, buffer, sizeof(buffer))) > 0)
    {
        send(client_sock, buffer, bytes_read, 0);
    }

    close(fd);
    printf("Sent file contents of %s to client\n", path);
}

void write_file(int client_sock, const char *path, int is_sync)
{

    if (client_sock < 0)
    {
        perror("Invalid client socket");
        return;
    }

    if (!path)
    {
        perror("Path does not exist");
        return;
    }

    char response[BUFFER_SIZE] = {0};
    const char *ack = "ACK";

    if (send(client_sock, ack, strlen(ack), 0) < 0)
    {
        perror("Failed to send initial ACK");
        return;
    }

    char size_str[BUFFER_SIZE] = {0};
    ssize_t bytes_received = recv(client_sock, size_str, sizeof(size_str) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive size header");
        return;
    }

    size_str[bytes_received] = '\0';
    size_t total_size = 0;
    if (sscanf(size_str, "SIZE %zu", &total_size) != 1)
    {
        perror("Invalid size format received");
        return;
    }

    if (total_size == 0)
    {
        return;
    }

    if (send(client_sock, ack, strlen(ack), 0) < 0)
    {
        perror("Failed to send size ACK");
        return;
    }

    bool use_async = !is_sync && (total_size > ASYNC_THRESHOLD);

    WriteRequest *req = NULL;
    if (use_async)
    {
        printf("Printing using async");
        req = (WriteRequest *)malloc(sizeof(WriteRequest));
        if (!req)
        {
            perror("Failed to allocate WriteRequest");
            return;
        }

        req->data = (char *)malloc(sizeof(char)*(total_size+100));
        if (!req->data)
        {
            perror("Failed to allocate data buffer");
            free(req);
            return;
        }
        req->size = total_size;
        strncpy(req->path, path, BUFFER_SIZE - 1);
        req->path[BUFFER_SIZE - 1] = '\0';
        req->is_sync = is_sync;
    }

    int fd = -1;
    if (!use_async)
    {
        fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0)
        {
            perror("        Failed to open file for writing");
            char error_msg[] = "ERROR: Failed to write to file\n";
            send(client_sock, error_msg, strlen(error_msg), 0);
            return;
        }
    }

    char buffer[BUFFER_SIZE];
    size_t total_received = 0;
    int chunk_count = 0;

    while (total_received < total_size)
    {
        memset(buffer, 0, sizeof(buffer));
        ssize_t chunk_size = sizeof(buffer) - 1;
        if (total_received + chunk_size > total_size)
        {
            chunk_size = total_size - total_received;
        }

        bytes_received = recv(client_sock, buffer, chunk_size, 0);

        total_received += bytes_received;

        if (bytes_received <= 0)
        {
            perror("Connection error or client disconnected");
            break;
        }

        if (use_async && req && req->data)
        {
            strncpy(req->data,buffer,bytes_received);
        }

        else if (fd != -1)
        {
            ssize_t bytes_written = write(fd, buffer, bytes_received);
            if (bytes_written < 0)
            {
                perror("Write failed");
                break;
            }
        }
    }

    if (total_received != total_size)
    {
        perror("Failed to receive all data");
        if (use_async && req)
        {
            free(req->data);
            free(req);
        }
        if (fd != -1)
        {
            close(fd);
        }
        return;
    }

    printf("File transfer complete: %zu bytes received\n", total_received);

    if (use_async && req && req->data)
    {
        req->data[total_size] = '\0';
        pthread_t write_thread;
        if (pthread_create(&write_thread, NULL, async_write_thread, req) != 0)
        {
            perror("Failed to create async write thread");
            return;
        }
        pthread_detach(write_thread);

        const char *async_msg = "Request accepted for async writing\n";
        send(client_sock, async_msg, strlen(async_msg), 0);
    }
    else
    {
        if (fd != -1)
        {
            close(fd);
            const char *sync_msg = "File written successfully (sync)\n";
            send(client_sock, sync_msg, strlen(sync_msg), 0);
        }
    }

    printf("Write operation completed for %s\n", path);
}

void stream_audio(int client_sock, const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        perror("Failed to open audio file for streaming");
        char error_msg[] = "ERROR: Failed to stream audio file\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        perror("Failed to get file stats");
        close(fd);
        return;
    }

    char size_header[32];
    snprintf(size_header, sizeof(size_header), "SIZE:%ld\n", st.st_size);
    if (send(client_sock, size_header, strlen(size_header), 0) < 0)
    {
        perror("Failed to send size header");
        close(fd);
        return;
    }

    printf("Sent size header: %s", size_header);

    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    size_t total_sent = 0;
    bool client_stopped = false;

    int flags = fcntl(client_sock, F_GETFL, 0);
    fcntl(client_sock, F_SETFL, flags | O_NONBLOCK);

    struct pollfd pfd = {
        .fd = client_sock,
        .events = POLLIN};

    while ((bytes_read = read(fd, buffer, sizeof(buffer))) > 0 && !client_stopped)
    {
        if (poll(&pfd, 1, 0) > 0 && (pfd.revents & POLLIN))
        {
            char cmd;
            ssize_t cmd_received = recv(client_sock, &cmd, 1, MSG_DONTWAIT);
            if (cmd_received > 0 && cmd == 'q')
            {
                printf("\nReceived stop command from client\n");
                client_stopped = true;
                const char *end_marker = "\nSTREAM_END_MARKER\n";
                send(client_sock, end_marker, strlen(end_marker), MSG_NOSIGNAL);
                break;
            }
        }

        size_t bytes_to_send = bytes_read;
        size_t bytes_sent_total = 0;

        while (bytes_sent_total < bytes_to_send && !client_stopped)
        {
            ssize_t bytes_sent = send(client_sock,
                                      buffer + bytes_sent_total,
                                      bytes_to_send - bytes_sent_total,
                                      MSG_NOSIGNAL);

            if (bytes_sent < 0)
            {
                if (errno == EPIPE)
                {
                    printf("\nClient disconnected\n");
                    goto cleanup;
                }
                else if (errno == EWOULDBLOCK || errno == EAGAIN)
                {
                    if (poll(&pfd, 1, 0) > 0 && (pfd.revents & POLLIN))
                    {
                        char cmd;
                        if (recv(client_sock, &cmd, 1, MSG_DONTWAIT) > 0 && cmd == 'q')
                        {
                            printf("\nReceived stop command from client\n");
                            client_stopped = true;
                            break;
                        }
                    }
                    usleep(1000);
                    continue;
                }
                perror("Send failed");
                goto cleanup;
            }

            bytes_sent_total += bytes_sent;
            total_sent += bytes_sent;
            printf("\rSent: %zu bytes of %ld", total_sent, st.st_size);
            fflush(stdout);
        }

        usleep(1000);
    }

cleanup:
    if (!client_stopped)
    {
        usleep(100000);

        if (total_sent == st.st_size)
        {
            const char *end_marker = "\nSTREAM_END_MARKER\n";
            send(client_sock, end_marker, strlen(end_marker), MSG_NOSIGNAL);
            printf("\nStream completed successfully\n");
        }
        else
        {
            printf("\nStream ended unexpectedly\n");
        }
    }
    else
    {
        printf("\nStream stopped by client\n");
    }

    fcntl(client_sock, F_SETFL, flags);
    close(fd);
}

void get_file_info(int client_sock, const char *path)
{
    struct stat file_stat;
    if (lstat(path, &file_stat) < 0)
    {
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "ERROR: Could not retrieve file info for %s: %s\n",
                 path, strerror(errno));
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    struct passwd *pw = getpwuid(file_stat.st_uid);
    struct group *gr = getgrgid(file_stat.st_gid);

    char access_time[30], mod_time[30], change_time[30];

    struct tm *tm_info_a = localtime(&file_stat.st_atime);
    strftime(access_time, sizeof(access_time), "%Y-%m-%d %H:%M:%S", tm_info_a);
    struct tm *tm_info_m = localtime(&file_stat.st_mtime);
    strftime(mod_time, sizeof(mod_time), "%Y-%m-%d %H:%M:%S", tm_info_m);
    struct tm *tm_info_c = localtime(&file_stat.st_ctime);
    strftime(change_time, sizeof(change_time), "%Y-%m-%d %H:%M:%S", tm_info_c);

    char perms[11];
    snprintf(perms, 11, "%c%c%c%c%c%c%c%c%c%c",
             (file_stat.st_mode & S_IRUSR) ? 'r' : '-',
             (file_stat.st_mode & S_IWUSR) ? 'w' : '-',
             (file_stat.st_mode & S_IXUSR) ? 'x' : '-',
             (file_stat.st_mode & S_IRGRP) ? 'r' : '-',
             (file_stat.st_mode & S_IWGRP) ? 'w' : '-',
             (file_stat.st_mode & S_IXGRP) ? 'x' : '-',
             (file_stat.st_mode & S_IROTH) ? 'r' : '-',
             (file_stat.st_mode & S_IWOTH) ? 'w' : '-',
             (file_stat.st_mode & S_IXOTH) ? 'x' : '-',
             '\0');

    char file_type[30];
    if (S_ISREG(file_stat.st_mode))
    {
        strcpy(file_type, "Regular File");
    }
    else if (S_ISDIR(file_stat.st_mode))
    {
        strcpy(file_type, "Directory");
    }
    else if (S_ISLNK(file_stat.st_mode))
    {
        strcpy(file_type, "Symbolic Link");
    }
    else if (S_ISBLK(file_stat.st_mode))
    {
        strcpy(file_type, "Block Device");
    }
    else if (S_ISCHR(file_stat.st_mode))
    {
        strcpy(file_type, "Character Device");
    }
    else if (S_ISFIFO(file_stat.st_mode))
    {
        strcpy(file_type, "FIFO/Pipe");
    }
    else if (S_ISSOCK(file_stat.st_mode))
    {
        strcpy(file_type, "Socket");
    }
    else
    {
        strcpy(file_type, 'Unknown');
    }

    char size_str[20];
    if (file_stat.st_size < 1024)
    {
        snprintf(size_str, sizeof(size_str), "%ld B", file_stat.st_size);
    }
    else if (file_stat.st_size < 1024 * 1024)
    {
        snprintf(size_str, sizeof(size_str), "%.2f KB", file_stat.st_size / 1024.0);
    }
    else if (file_stat.st_size < 1024 * 1024 * 1024)
    {
        snprintf(size_str, sizeof(size_str), "%.2f MB", file_stat.st_size / (1024.0 * 1024.0));
    }
    else
    {
        snprintf(size_str, sizeof(size_str), "%.2f GB", file_stat.st_size / (1024.0 * 1024.0 * 1024.0));
    }

    char info[BUFFER_SIZE];
    snprintf(info, sizeof(info),
             "File: %s\n"
             "Type: %s\n"
             "Size: %s (%ld bytes)\n"
             "Permissions: %s (Octal: %o)\n"
             "Owner: %s (%d)\n"
             "Group: %s (%d)\n"
             "Device ID: %ld\n"
             "Inode: %ld\n"
             "Links: %ld\n"
             "Access Time: %s\n"
             "Modify Time: %s\n"
             "Change Time: %s\n"
             "Block Size: %ld\n"
             "Blocks Allocated: %ld\n",
             path,
             file_type,
             size_str, file_stat.st_size,
             perms, file_stat.st_mode & 0777,
             pw ? pw->pw_name : "unknown", file_stat.st_uid,
             gr ? gr->gr_name : "unknown", file_stat.st_gid,
             (long)file_stat.st_dev,
             (long)file_stat.st_ino,
             (long)file_stat.st_nlink,
             access_time,
             mod_time,
             change_time,
             (long)file_stat.st_blksize,
             (long)file_stat.st_blocks);

    if (S_ISLNK(file_stat.st_mode))
    {
        char link_target[BUFFER_SIZE];
        ssize_t len = readlink(path, link_target, sizeof(link_target) - 1);
        if (len != -1)
        {
            link_target[len] = '\0';
            char link_info[BUFFER_SIZE];
            snprintf(link_info, sizeof(link_info), "Link Target: %s\n", link_target);
            strncat(info, link_info, sizeof(info) - strlen(info) - 1);
        }
    }

    send(client_sock, info, strlen(info), 0);
    printf("Sent file info for %s to client\n", path);
}