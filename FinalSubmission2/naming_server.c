// naming_server.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <asm-generic/socket.h>
#include <stdbool.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <stdarg.h>

#define NM_PORT 8080
#define BUFFER_SIZE 100000
#define MAX_PATH_LENGTH 256
#define LRU_CACHE_SIZE 100
#define LOG_FILE "naming_server.log"
#define PATH_SEPARATOR '/'
#define MAX_PATHS 200
#define MAX_FILES 10000
#define HASHMAP_INITIAL_SIZE 100
#define LOAD_FACTOR_THRESHOLD 0.75
#define MAX_STORAGE_SERVERS 10
#define PING_PORT 9999

typedef struct
{
    char file_path[MAX_PATH_LENGTH];
    int primary_ss;
    int replicas[2];
} FileReplicationMetadata;

typedef struct
{
    char ip[16];
    int port;
    int transfer_port;
    char **accessible_paths;
    int num_paths;
    int socket;
    int replicated;
    int replica1;
    int replica2;
    int removed;
} storage_server_info;

typedef struct
{
    int sock;
    char buffer[BUFFER_SIZE];
} ThreadArgs;

typedef struct TrieNode
{
    struct TrieNode *children[256];
    int is_end;
    int server_index;
    int is_file;
} TrieNode;

typedef struct LRUNode
{
    char *path;
    int server_index;
    struct LRUNode *prev;
    struct LRUNode *next;
} LRUNode;

typedef struct
{
    int capacity;
    int size;
    LRUNode *head;
    LRUNode *tail;
    LRUNode **hash_table;
} LRUCache;

typedef struct HashNode
{
    char *path;
    int server_index;
    struct HashNode *next;
} HashNode;

typedef struct
{
    HashNode **buckets;
    size_t size;
    size_t capacity;
} HashMap;

typedef struct
{
    int nm_sock;
    int ping_sock;
} Crosssocket;

LRUCache *cache = NULL;
HashMap *map = NULL;
FileReplicationMetadata replication_table[MAX_FILES];
int replication_count = 0;
storage_server_info storage_servers[MAX_STORAGE_SERVERS];
int num_storage_servers = 0;
pthread_mutex_t servers_mutex = PTHREAD_MUTEX_INITIALIZER;
int fail_sock;

LRUCache *create_lru_cache(int capacity);
void insert_path(const char *path, int server_index);
void put_in_cache(LRUCache *cache, const char *path, int server_index);
int search_path(const char *path);
int get_from_cache(LRUCache *cache, const char *path);

void free_storage_servers();

void *handle_storage_server(int *client_sock_ptr);
void *handle_client(void *args);
int checkifclient(char *buffer);

char *get_name_from_path(const char *path);
void get_parent_path(const char *full_path, char *parent_path, size_t size);

int send_create_command(const char *type, const char *name, const char *path);
int send_delete_command(const char *type, const char *name, const char *path);
int send_copy_command(const char *type, const char *src_path, const char *dest_path);

void add_replication_entry();
char *create_path(const char *base, const char *suffix);
int create_nested_directories(const char *path, int index, const char *home_path);

void initialize_naming_server();
void handle_shutdown(int signum);

void log_event(const char *format, ...);
void log_separator(const char *message);

typedef struct ThreadArgs2
{
    int ab;
    const char *path;
    const char *str;
} ThreadArgs2;

size_t strlcat(char *dst, const char *src, size_t size)
{
    size_t dst_len = strnlen(dst, size);
    size_t src_len = strlen(src);

    if (dst_len == size)
    {
        return size + src_len;
    }

    size_t to_copy = size - dst_len - 1;
    if (to_copy > src_len)
    {
        to_copy = src_len;
    }

    memcpy(dst + dst_len, src, to_copy);
    dst[dst_len + to_copy] = '\0';

    return dst_len + src_len;
}

void add_replication_entry()
{
    for (int primary_ss = 0; primary_ss < num_storage_servers; primary_ss++)
    {
        const char *type_ss;
        const char *last_slash_ss = strrchr(storage_servers[primary_ss].accessible_paths[0], '/');

        if (last_slash_ss != NULL)
        {
            type_ss = (strchr(last_slash_ss + 1, '.') != NULL) ? "FILE" : "DIR";
        }
        else
        {
            type_ss = (strchr(storage_servers[primary_ss].accessible_paths[0], '.') != NULL) ? "FILE" : "DIR";
        }
        if (strcmp(type_ss, "FILE") == 0)
        {
            continue;
        }
        if (storage_servers[primary_ss].replicated != 2)
        {
            if (num_storage_servers > 2 && storage_servers[primary_ss].replicated == 0)
            {
                int j = primary_ss;
                int replica1 = -1;
                int replica2 = -1;
                for (int i = 0; i < num_storage_servers; i++)
                {
                    j = (j - 1 + num_storage_servers) % num_storage_servers;

                    if (j == primary_ss)
                    {
                        continue;
                    }

                    const char *type;
                    const char *last_slash = strrchr(storage_servers[j].accessible_paths[0], '/');

                    if (last_slash != NULL)
                    {
                        type = (strchr(last_slash + 1, '.') != NULL) ? "FILE" : "DIR";
                    }
                    else
                    {
                        type = (strchr(storage_servers[j].accessible_paths[0], '.') != NULL) ? "FILE" : "DIR";
                    }
                    if (strcmp(type, type_ss) == 0)
                    {
                        if (replica1 == -1)
                        {
                            replica1 = j;
                        }
                        else
                        {
                            replica2 = j;
                            break;
                        }
                    }
                }
                if (replica1 == -1 || replica2 == -1)
                {
                    log_event("Unable to backup data");
                    continue;
                }

                storage_servers[primary_ss].replica1 = replica1;
                storage_servers[primary_ss].replica2 = replica2;

                const char *type;
                const char *last_slash = strrchr(storage_servers[j].accessible_paths[0], '/');

                if (last_slash != NULL)
                {
                    type = (strchr(last_slash + 1, '.') != NULL) ? "FILE" : "DIR";
                }
                else
                {
                    type = (strchr(storage_servers[j].accessible_paths[0], '.') != NULL) ? "FILE" : "DIR";
                }

                char *str1 = create_path(storage_servers[replica1].accessible_paths[0], storage_servers[primary_ss].accessible_paths[0]);
                char *str2 = create_path(storage_servers[replica2].accessible_paths[0], storage_servers[primary_ss].accessible_paths[0]);

                if (str1 == NULL || str2 == NULL)
                {
                    free(str1);
                    free(str2);
                    return;
                }

                char message[BUFFER_SIZE];
                message[0] = '\0';

                snprintf(message, sizeof(message), "NESTED: %s", str1);

                log_event("Sent command: %s", message);
                send(storage_servers[replica1].socket, message, strlen(message), 0);

                int bytes_received1 = recv(storage_servers[replica1].socket, message, sizeof(message), 0);
                {
                    log_event("Received Response: %s", message);
                }

                char response[BUFFER_SIZE];
                create_nested_directories(str1, replica1, storage_servers[replica1].accessible_paths[0]);

                snprintf(message, sizeof(message), "NESTED: %s", str2);
                log_event("Sent command: %s", message);
                send(storage_servers[replica2].socket, message, strlen(message), 0);

                int bytes_received2 = recv(storage_servers[replica2].socket, message, sizeof(message), 0);
                {
                    log_event("Received Response: %s", message);
                }

                create_nested_directories(str2, replica2, storage_servers[replica2].accessible_paths[0]);

                send_copy_command(type, storage_servers[primary_ss].accessible_paths[0], str1);
                send_copy_command(type, storage_servers[primary_ss].accessible_paths[0], str2);

                storage_servers[primary_ss].replicated = 2;

                free(str1);
                free(str2);
            }
        }
    }
}

char *create_path(const char *base, const char *suffix)
{
    size_t len = strlen(base) + strlen(suffix) + 3;
    char *path = malloc(len);
    if (path == NULL)
        return NULL;

    snprintf(path, len, "%s/.%s", base, suffix + 1);
    return path;
}

int create_nested_directories(const char *path, int index, const char *home_path)
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

        if (strlen(buffer) >= strlen(home_path))
        {
            insert_path(buffer, index);
        }

        *next = save;

        p = (*next == '/') ? next + 1 : next;
    }

    return 0;
}

int is_file_path(const char *path)
{
    const char *last_slash = strrchr(path, '/');
    if (!last_slash)
        return 0;
    return strchr(last_slash, '.') != NULL;
}

char *get_timestamp()
{
    time_t now;
    time(&now);
    char *timestamp = malloc(26);
    strftime(timestamp, 26, "[%Y-%m-%d %H:%M:%S]", localtime(&now));
    return timestamp;
}

void init_hashmap()
{
    map = (HashMap *)malloc(sizeof(HashMap));
    if (!map)
    {
        char *timestamp = get_timestamp();
        log_event("Failed to allocate memory for hashmap");
        free(timestamp);
        exit(1);
    }

    map->capacity = HASHMAP_INITIAL_SIZE;
    map->size = 0;
    map->buckets = (HashNode **)calloc(map->capacity, sizeof(HashNode *));

    if (!map->buckets)
    {
        char *timestamp = get_timestamp();
        log_event("Failed to allocate memory for hashmap buckets");
        free(timestamp);
        free(map);
        exit(1);
    }
}

uint32_t hash_function(const char *str)
{
    uint32_t hash = 2166136261u;
    const char *p = str;

    while (*str)
    {
        hash ^= (uint8_t)*str;
        hash *= 16777619u;
        str++;
    }

    return hash;
}

char *normalize_path(const char *path)
{
    if (!path)
    {
        char *timestamp = get_timestamp();
        log_event("Error: NULL path provided to normalize_path");
        free(timestamp);
        return NULL;
    }

    while (*path == ' ')
        path++;

    char *normalized = strdup(path);
    if (!normalized)
    {
        char *timestamp = get_timestamp();
        log_event("Error: Failed to allocate memory for normalized path");
        free(timestamp);
        return NULL;
    }

    size_t len = strlen(normalized);
    while (len > 0 && (normalized[len - 1] == ' ' || normalized[len - 1] == '/'))
    {
        normalized[len - 1] = '\0';
        len--;
    }

    return normalized;
}

void insert_path(const char *path, int server_index)
{
    if (!map)
    {
        init_hashmap();
    }

    char *normalized_path = normalize_path(path);
    if (!normalized_path)
    {
        log_event("Error: Failed to normalize path");
        return;
    }

    uint32_t hash = hash_function(normalized_path);
    uint32_t bucket = hash % map->capacity;

    HashNode *current = map->buckets[bucket];
    while (current)
    {
        if (strcmp(current->path, normalized_path) == 0)
        {
            log_event("Found existing path, updating server index from %d to %d", current->server_index, server_index);

            current->server_index = server_index;
            if (cache)
            {
                put_in_cache(cache, normalized_path, server_index);
            }
            free(normalized_path);
            return;
        }
        current = current->next;
    }

    HashNode *new_node = (HashNode *)malloc(sizeof(HashNode));
    if (!new_node)
    {
        log_event("Failed to allocate memory for new hash node");
        free(normalized_path);
        return;
    }

    new_node->path = strdup(normalized_path);
    if (!new_node->path)
    {
        log_event("Failed to allocate memory for path string");
        free(new_node);
        free(normalized_path);
        return;
    }

    new_node->server_index = server_index;
    new_node->next = map->buckets[bucket];
    map->buckets[bucket] = new_node;
    map->size++;
    log_event("Path added %s at index %d", new_node->path, new_node->server_index);

    if (cache)
    {
        put_in_cache(cache, normalized_path, server_index);
    }

    free(normalized_path);
}

int search_path(const char *path)
{
    char *timestamp = get_timestamp();
    free(timestamp);

    if (!map)
    {
        timestamp = get_timestamp();
        log_event("Hashmap not initialized");
        free(timestamp);
        return -1;
    }

    char *normalized_path = normalize_path(path);
    if (!normalized_path)
    {
        timestamp = get_timestamp();
        log_event("Error: Failed to normalize path during search");
        free(timestamp);
        return -1;
    }

    uint32_t hash = hash_function(normalized_path);
    uint32_t bucket = hash % map->capacity;

    HashNode *current = map->buckets[bucket];
    while (current)
    {
        if (strcmp(current->path, normalized_path) == 0)
        {
            timestamp = get_timestamp();
            log_event("Found match! Server index: %d", current->server_index);
            free(timestamp);

            int result = current->server_index;
            if (cache)
            {
                put_in_cache(cache, normalized_path, result);
            }
            free(normalized_path);
            return result;
        }
        current = current->next;
    }

    timestamp = get_timestamp();
    log_event("Path not found in bucket %u", bucket);
    free(timestamp);

    free(normalized_path);
    return -1;
}

void cleanup_hashmap()
{
    char *timestamp = get_timestamp();
    printf("Cleaning up hashmap");
    free(timestamp);

    if (!map)
        return;

    for (size_t i = 0; i < map->capacity; i++)
    {
        HashNode *current = map->buckets[i];
        while (current)
        {
            HashNode *next = current->next;
            free(current->path);
            free(current);
            current = next;
        }
    }

    free(map->buckets);
    free(map);
    map = NULL;

    timestamp = get_timestamp();
    log_event("Hashmap cleanup complete");
    free(timestamp);
}

LRUCache *create_lru_cache(int capacity)
{
    LRUCache *cache = (LRUCache *)malloc(sizeof(LRUCache));
    cache->capacity = capacity;
    cache->size = 0;
    cache->head = NULL;
    cache->tail = NULL;
    cache->hash_table = (LRUNode **)calloc(capacity, sizeof(LRUNode *));
    return cache;
}

int get_from_cache(LRUCache *cache, const char *path)
{
    unsigned long hash = 5381;
    const char *str = path;
    while (*str)
    {
        hash = ((hash << 5) + hash) + *str++;
    }
    int index = hash % cache->capacity;

    LRUNode *node = cache->hash_table[index];
    if (node && strcmp(node->path, path) == 0)
    {
        if (node != cache->tail)
        {
            if (node == cache->head)
            {
                cache->head = node->next;
                if (cache->head)
                {
                    cache->head->prev = NULL;
                }
            }
            else
            {
                node->prev->next = node->next;
                if (node->next)
                {
                    node->next->prev = node->prev;
                }
            }

            node->prev = cache->tail;
            node->next = NULL;
            if (cache->tail)
            {
                cache->tail->next = node;
            }
            cache->tail = node;
        }

        return node->server_index;
    }
    return -1;
}

void remove_from_cache(LRUCache *cache, const char *path)
{
    if (!cache || !path)
    {
        return;
    }

    unsigned long hash = 5381;
    const char *str = path;
    while (*str)
    {
        hash = ((hash << 5) + hash) + *str++;
    }
    int index = hash % cache->capacity;

    LRUNode *node = cache->hash_table[index];
    if (!node || strcmp(node->path, path) != 0)
    {
        return;
    }

    cache->hash_table[index] = NULL;

    if (node == cache->head && node == cache->tail)
    {
        cache->head = NULL;
        cache->tail = NULL;
    }
    else if (node == cache->head)
    {
        cache->head = node->next;
        if (cache->head)
        {
            cache->head->prev = NULL;
        }
    }
    else if (node == cache->tail)
    {
        cache->tail = node->prev;
        if (cache->tail)
        {
            cache->tail->next = NULL;
        }
    }
    else
    {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    free(node->path);
    free(node);

    cache->size--;
}

void put_in_cache(LRUCache *cache, const char *path, int server_index)
{
    unsigned long hash = 5381;
    const char *str = path;
    while (*str)
    {
        hash = ((hash << 5) + hash) + *str++;
    }
    int index = hash % cache->capacity;

    LRUNode *new_node = (LRUNode *)malloc(sizeof(LRUNode));
    new_node->path = strdup(path);
    new_node->server_index = server_index;
    new_node->prev = cache->tail;
    new_node->next = NULL;

    cache->hash_table[index] = new_node;

    if (cache->tail)
    {
        cache->tail->next = new_node;
    }
    cache->tail = new_node;

    if (!cache->head)
    {
        cache->head = new_node;
    }

    if (cache->size >= cache->capacity)
    {
        LRUNode *old_head = cache->head;
        cache->head = old_head->next;
        if (cache->head)
        {
            cache->head->prev = NULL;
        }

        unsigned long old_hash = 5381;
        str = old_head->path;
        while (*str)
        {
            old_hash = ((old_hash << 5) + old_hash) + *str++;
        }
        int old_index = old_hash % cache->capacity;
        cache->hash_table[old_index] = NULL;

        free(old_head->path);
        free(old_head);
    }
    else
    {
        cache->size++;
    }
}

void initialize_naming_server()
{
    init_hashmap();
    cache = create_lru_cache(LRU_CACHE_SIZE);
}

int checkifclient(char *buffer)
{
    char operation[32];
    char path[256];
    sscanf(buffer, "%s %s", operation, path);
    printf("operation: %s\n", operation);
    return (strcmp(operation, "READ") == 0 || strcmp(operation, "WRITE") == 0 || strcmp(operation, "INFO") == 0 || strcmp(operation, "STREAM") == 0 ||
            strcmp(operation, "CREATE") == 0 || strcmp(operation, "DELETE") == 0 || strcmp(operation, "COPY") == 0 || strcmp(operation, "LISTPATHS") == 0);
}

void log_event(const char *format, ...)
{
    va_list args;
    va_start(args, format);

    FILE *log_file = fopen(LOG_FILE, "a");
    if (log_file)
    {
        time_t now = time(NULL);
        struct tm *time_info = localtime(&now);

        char timestamp[20];
        if (time_info && strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", time_info))
        {
            fprintf(log_file, "[%s] ", timestamp);
            vfprintf(log_file, format, args);
            fprintf(log_file, "\n");
        }
        else
        {
            fprintf(log_file, "[UNKNOWN TIME] ");
            vfprintf(log_file, format, args);
            fprintf(log_file, "\n");
        }

        fclose(log_file);
    }
    else
    {
        fprintf(stderr, "ERROR: Unable to open log file '%s' for writing.\n", LOG_FILE);
    }

    va_end(args);
}

void log_separator(const char *message)
{
    FILE *log_file = fopen(LOG_FILE, "a");
    if (log_file)
    {
        fprintf(log_file, "\n");
        fprintf(log_file, "===============================================================\n");
        fprintf(log_file, "%s\n", message);
        fprintf(log_file, "===============================================================\n");
        fprintf(log_file, "\n");

        fclose(log_file);
    }
    else
    {
        fprintf(stderr, "ERROR: Unable to open log file '%s' for writing.\n", LOG_FILE);
    }
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

void handle_shutdown(int signum)
{
    printf("Shutting down Naming Server...\n");
    log_event("Shutting down Naming Server...");
    free_storage_servers();
    exit(EXIT_SUCCESS);
}

void free_storage_servers()
{
    pthread_mutex_lock(&servers_mutex);
    for (int i = 0; i < num_storage_servers; i++)
    {
        for (int j = 0; j < storage_servers[i].num_paths; j++)
        {
            free(storage_servers[i].accessible_paths[j]);
        }
        free(storage_servers[i].accessible_paths);
    }
    pthread_mutex_unlock(&servers_mutex);
}

int main()
{
    initialize_naming_server();

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
    {
        log_event("Failed to create server socket");
        return EXIT_FAILURE;
    }

    if (setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0)
    {
        log_event("Failed to set socket options");
        close(server_sock);
        return EXIT_FAILURE;
    }
    if (setsockopt(server_sock, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int)) < 0)
    {
        log_event("Failed to set socket options");
        close(server_sock);
        return EXIT_FAILURE;
    }
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(NM_PORT);
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(server_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        log_event("Failed to bind server socket");
        close(server_sock);
        return EXIT_FAILURE;
    }
    if (listen(server_sock, 5) < 0)
    {
        log_event("Failed to listen on server socket");
        close(server_sock);
        return EXIT_FAILURE;
    }

    fail_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (fail_sock < 0)
    {
        log_event("Failed to create server socket");
        return EXIT_FAILURE;
    }
    if (setsockopt(fail_sock, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0)
    {
        log_event("Failed to set socket options");
        close(fail_sock);
        return EXIT_FAILURE;
    }
    if (setsockopt(fail_sock, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int)) < 0)
    {
        log_event("Failed to set socket options");
        close(fail_sock);
        return EXIT_FAILURE;
    }
    struct sockaddr_in ping_addr;
    memset(&ping_addr, 0, sizeof(ping_addr));
    ping_addr.sin_family = AF_INET;
    ping_addr.sin_port = htons(PING_PORT);
    ping_addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(fail_sock, (struct sockaddr *)&ping_addr, sizeof(ping_addr)) < 0)
    {
        log_event("Failed to bind server socket");
        close(fail_sock);
        return EXIT_FAILURE;
    }
    if (listen(fail_sock, 5) < 0)
    {
        log_event("Failed to listen on server socket");
        close(fail_sock);
        return EXIT_FAILURE;
    }

    log_separator("Starting new server session: Running Naming server on port 8080");
    while (1)
    {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int client_sock = accept(server_sock, (struct sockaddr *)&cli_addr, &cli_len);
        if (client_sock < 0)
        {
            log_event("Failed to accept connection");
            continue;
        }

        char initial_message[BUFFER_SIZE];
        int bytes_received = recv(client_sock, initial_message, sizeof(initial_message) - 1, 0);
        if (bytes_received <= 0)
        {
            log_event("Failed to receive data from connection");
            close(client_sock);
            continue;
        }
        initial_message[bytes_received] = '\0';

        log_event("Received connection: %s", initial_message);
        printf("11\n");
        wait(2);

        ThreadArgs *args = malloc(sizeof(ThreadArgs));
        if (args == NULL)
        {
            log_event("Failed to allocate thread arguments");
            close(client_sock);
            continue;
        }
        args->sock = client_sock;
        strncpy(args->buffer, initial_message, BUFFER_SIZE - 1);

        if (strstr(initial_message, "Storage Server") != NULL)
        {
            pthread_t storage_thread;
            if (pthread_create(&storage_thread, NULL, handle_storage_server, args) != 0)
            {
                log_event("Failed to create storage server thread");
                free(args);
                close(client_sock);
            }
            else
            {
                pthread_detach(storage_thread);
            }
        }
        else if (checkifclient(initial_message))
        {
            pthread_t client_thread;
            if (pthread_create(&client_thread, NULL, handle_client, args) != 0)
            {
                log_event("Failed to create client thread");
                free(args);
                close(client_sock);
            }
            else
            {
                pthread_detach(client_thread);
            }
        }
        else if (strncmp(initial_message, "DONE", 4) == 0)
        {
            char path[BUFFER_SIZE];
            if (sscanf(initial_message, "written: %s", path))
            {
                int server_index = search_path(path);
                if (server_index >= 0 && server_index <= MAX_STORAGE_SERVERS && storage_servers[server_index].removed == 0)
                {
                    if (storage_servers[server_index].replica1 >= 0 && storage_servers[server_index].replica1 <= MAX_STORAGE_SERVERS && storage_servers[storage_servers[server_index].replica1].removed == 0)
                    {
                        char *str1 = create_path(storage_servers[storage_servers[server_index].replica1].accessible_paths[0], path);
                        send_copy_command("FILE", storage_servers[storage_servers[server_index].replica1].accessible_paths[0], str1);
                        free(str1);
                    }
                }
                if (server_index >= 0 && server_index <= MAX_STORAGE_SERVERS && storage_servers[server_index].removed == 0)
                {
                    if (storage_servers[server_index].replica2 >= 0 && storage_servers[server_index].replica2 <= MAX_STORAGE_SERVERS && storage_servers[storage_servers[server_index].replica2].removed == 0)
                    {
                        char *str2 = create_path(storage_servers[storage_servers[server_index].replica2].accessible_paths[0], path);
                        send_copy_command("FILE", storage_servers[storage_servers[server_index].replica2].accessible_paths[0], str2);
                    }
                }
            }
            free(args);
            close(client_sock);
        }
        else
        {
            log_event("Unknown connection type");
            free(args);
            close(client_sock);
        }
    }

    close(server_sock);

    free_storage_servers();
    return EXIT_SUCCESS;
}

//          0: Success
//          1: Path not accessible
//          2: Send error
//          3: Receive error
//          4: System error (includes unknown responses)
//          5: Invalid input format
//          6: Path already exists
//          7: Connection error
//          8: Path limit exceeded
//          9: Path does/does not exist

int send_create_command(const char *type, const char *name, const char *path)
{
    if (!type || !name || !path)
    {
        log_event("Invalid input parameters: type/name/path is NULL");
        return 5;
    }

    char full_path[BUFFER_SIZE];

    strcpy(full_path, path);
    strcat(full_path, "/");
    strcat(full_path, name);

    if (!name)
    {
        log_event("Invalid path format: Unable to extract name");
        return 5;
    }
    printf("full path: %s\n", path);
    char debug_buffer[1024];
    int server = search_path(path);
    printf("server: %d\n", server);

    if (server < 0)
    {
        log_event("Path is not accessible: %s", path);
        return 1;
    }

    if (search_path(full_path) >= 0)
    {
        log_event("Path already exists: %s", full_path);
        return 6;
    }

    int client_sock = storage_servers[server].socket;
    if (client_sock < 0)
    {
        log_event("Invalid socket connection");
        return 7;
    }

    char command[BUFFER_SIZE];
    if (snprintf(command, sizeof(command), "CREATE %s %s", type, full_path) >= sizeof(command))
    {
        log_event("Command buffer overflow");
        return 5;
    }

    if (send(client_sock, command, strlen(command), 0) < 0)
    {
        log_event("Failed to send CREATE command: %s", command);
        return 2;
    }

    log_event("Sent CREATE command to Storage Server: %s", command);

    char response[BUFFER_SIZE] = {0};
    int bytes_received = recv(client_sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        log_event("Received response from Storage Server: %s", response + 2);

        if (bytes_received < 1)
        {
            return 4;
        }

        int response_code = response[0] - '0';
        switch (response_code)
        {
        case 0:
            if (storage_servers[server].num_paths >= MAX_PATHS)
            {
                log_event("Maximum path limit reached");
                return 0;
            }
            storage_servers[server].accessible_paths[storage_servers[server].num_paths++] = strdup(full_path);
            insert_path(full_path, server);
            return 0;
        case 1:
            return 4;
        case 2:
            return 5;
        case 3:
            return 6;
        case 4:
            return 8;
        case 5:
            return 7;
        default:
            return 4;
        }
    }
    else
    {
        log_event("Failed to receive response from Storage Server");
        return 3;
    }
}

void delete_from_hash(const char *path)
{
    if (!map || !path)
    {
        log_event("Invalid map or path for deletion");
        return;
    }

    char *normalized_path = normalize_path(path);
    if (!normalized_path)
    {
        log_event("Error: Failed to normalize path during deletion: %s", path);
        return;
    }

    size_t prefix_len = strlen(normalized_path);

    for (uint32_t bucket = 0; bucket < map->capacity; bucket++)
    {
        HashNode *current = map->buckets[bucket];
        HashNode *prev = NULL;

        while (current)
        {
            if (strncmp(current->path, normalized_path, prefix_len) == 0 &&
                (current->path[prefix_len] == '\0' || current->path[prefix_len] == '/'))
            {
                if (prev)
                {
                    prev->next = current->next;
                }
                else
                {
                    map->buckets[bucket] = current->next;
                }

                log_event("Deleted path with prefix: '%s'", current->path);

                HashNode *to_delete = current;
                current = current->next;

                if (cache)
                {
                    remove_from_cache(cache, to_delete->path);
                }

                free(to_delete->path);
                free(to_delete);
                map->size--;
            }
            else
            {
                prev = current;
                current = current->next;
            }
        }
    }

    free(normalized_path);
}

int send_delete_command(const char *type, const char *name, const char *path)
{
    if (!type || !name || !path)
    {
        log_event("Invalid input parameters: type/name/path is NULL");
        return 5;
    }

    char full_path[BUFFER_SIZE];
    strcpy(full_path, path);
    strcat(full_path, "/");
    strcat(full_path, name);

    int server = search_path(path);
    if (server < 0)
    {
        log_event("Path is not accessible: %s", path);
        return 1;
    }

    if (search_path(full_path) < 0)
    {
        log_event("Path does not exist: %s", full_path);
        return 9;
    }

    int client_sock = storage_servers[server].socket;
    if (client_sock < 0)
    {
        log_event("Invalid socket connection");
        return 7;
    }

    char command[BUFFER_SIZE];
    if (snprintf(command, sizeof(command), "DELETE %s %s", type, full_path) >= sizeof(command))
    {
        log_event("Command buffer overflow");
        return 5;
    }

    if (send(client_sock, command, strlen(command), 0) < 0)
    {
        log_event("Failed to send DELETE command: %s", command);
        return 2;
    }

    log_event("Sent DELETE command to Storage Server: %s", command);

    char response[BUFFER_SIZE] = {0};
    int bytes_received = recv(client_sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        log_event("Received response from Storage Server: %s", response + 2);
        printf("response: %s\n", response);

        int response_code = response[0] - '0';

        switch (response_code)
        {
        case 0:
            char *paths_start = strstr(response, "PATHS:");
            if (paths_start)
            {
                paths_start += 6;

                char *path = strtok(paths_start, "|");
                while (path != NULL)
                {
                    delete_from_hash(path);

                    for (int i = 0; i < storage_servers[server].num_paths; i++)
                    {
                        if (strcmp(storage_servers[server].accessible_paths[i], path) == 0)
                        {
                            for (int j = i; j < storage_servers[server].num_paths - 1; j++)
                            {
                                strcpy(storage_servers[server].accessible_paths[j],
                                       storage_servers[server].accessible_paths[j + 1]);
                            }
                            storage_servers[server].num_paths--;
                            break;
                        }
                    }
                    path = strtok(NULL, "|");
                }
                char message[BUFFER_SIZE];
                snprintf(message, sizeof(message), "0 Delete operation successful for paths: %s", paths_start);
                ssize_t bytes_sent = send(client_sock, message, strlen(message), 0);
                if (bytes_sent < 0)
                {
                    perror("Send failed");
                }
                log_event("Successfully processed delete operation for paths");

                return 0;
            case 1:
                return 4;
            case 2:
                return 5;
            case 4:
                return 9;
            default:
                return 4;
            }
        }
    }
    else
    {
        log_event("Failed to receive response from Storage Server");
        return 3;
    }
}

void move_in_hash(const char *src_path, const char *dest_path)
{
    if (!map || !src_path || !dest_path)
    {
        log_event("Invalid map or paths for moving");
        return;
    }

    char *normalized_src = normalize_path(src_path);
    char *normalized_dest = normalize_path(dest_path);

    if (!normalized_src || !normalized_dest)
    {
        log_event("Error: Failed to normalize paths");
        free(normalized_src);
        free(normalized_dest);
        return;
    }

    size_t src_prefix_len = strlen(normalized_src);

    int dest_server_index = search_path(dest_path);
    if (dest_server_index == -1)
    {
        log_event("Destination path not found on any server: %s", dest_path);
        free(normalized_src);
        free(normalized_dest);
        return;
    }

    for (uint32_t bucket = 0; bucket < map->capacity; bucket++)
    {
        HashNode *current = map->buckets[bucket];

        while (current)
        {
            if (strncmp(current->path, normalized_src, src_prefix_len) == 0 &&
                (current->path[src_prefix_len] == '\0' || current->path[src_prefix_len] == '/'))
            {
                const char *relative_path = current->path + src_prefix_len;

                char new_path[BUFFER_SIZE];
                snprintf(new_path, sizeof(new_path), "%s%s", normalized_dest, relative_path);

                log_event("Copying path: '%s' to '%s'", current->path, new_path);

                insert_path(new_path, dest_server_index);
            }

            current = current->next;
        }
    }

    free(normalized_src);
    free(normalized_dest);
}
int send_copy_command(const char *type, const char *src_path, const char *dest_path)
{
    if (!type || !src_path || !dest_path)
    {
        log_event("Invalid input parameters: type/path is NULL");
        return 5;
    }

    int src_server = search_path(src_path);
    int dest_server = search_path(dest_path);
    if (src_server < 0)
    {
        log_event("Path is not accessible: %s", src_path);
        return 1;
    }

    if (dest_server < 0)
    {
        log_event("Path does not exist: %s", dest_path);
        return 9;
    }

    int dest_sock = storage_servers[dest_server].socket;
    char src_ip[16];
    int src_port = -1;
    strcpy(src_ip, storage_servers[src_server].ip);
    src_port = storage_servers[src_server].port;

    if (dest_sock < 0)
    {
        log_event("Invalid socket connection");
        return 7;
    }

    char command[BUFFER_SIZE] = {0};

    if (!snprintf(command, sizeof(command), "COPY %s %s %s %s %d\n", type, src_path, dest_path, src_ip, src_port))
    {
        log_event("Command buffer overflow");
        return 5;
    }

    if (send(dest_sock, command, strlen(command), 0) < 0)
    {
        log_event("Failed to send COPY command: %s", command);
        return 2;
    }

    log_event("Sent COPY command to Storage Server: %s", command);

    char response[BUFFER_SIZE];
    response[0] = '\0';
    int bytes_received = recv(dest_sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        log_event("Received response from Storage Server: %s", response + 2);

        int response_code = response[0] - '0';
        switch (response_code)
        {
        case 0:
            move_in_hash(src_path, dest_path);
            return 0;
        case 1:
            return 4;
        case 2:
            return 5;
        case 4:
            return 9;
        default:
            return 4;
        }
    }
    else
    {
        log_event("Failed to receive response from Storage Server");
        return 3;
    }
}

void *handle_storage_server(int *client_sock_ptr)
{
    if (!client_sock_ptr)
    {
        log_event("ERROR: NULL client socket pointer");
        return NULL;
    }

    int client_sock = *client_sock_ptr;
    free(client_sock_ptr);

    char buffer[BUFFER_SIZE] = {0};

    int bytes_read = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0)
    {
        log_event("Error receiving data from client: %s", strerror(errno));
        close(client_sock);
        return NULL;
    }

    buffer[bytes_read] = '\0';
    log_event("Received registration: %s", buffer);

    int port = 0;
    char *port_start = strstr(buffer, "Client Port: ");
    if (!port_start)
    {
        log_event("ERROR: Client Port not found in message");
        close(client_sock);
        return NULL;
    }

    port_start += 13;
    char *end_ptr;
    port = strtol(port_start, &end_ptr, 10);
    if (port <= 0 || port > 65535)
    {
        log_event("ERROR: Invalid port number: %d", port);
        close(client_sock);
        return NULL;
    }

    char ip[INET_ADDRSTRLEN] = {0};
    char *ip_start = strstr(buffer, "IP: ");
    if (!ip_start)
    {
        log_event("ERROR: IP not found in message");
        close(client_sock);
        return NULL;
    }

    ip_start += 4;
    char *ip_end = strchr(ip_start, ',');
    if (ip_end)
    {
        size_t ip_len = ip_end - ip_start;
        if (ip_len >= sizeof(ip))
            ip_len = sizeof(ip) - 1;
        strncpy(ip, ip_start, ip_len);
        ip[ip_len] = '\0';
    }
    else
    {
        log_event("ERROR: Malformed IP field in message");
        close(client_sock);
        return NULL;
    }

    pthread_mutex_lock(&servers_mutex);

    if (num_storage_servers >= MAX_STORAGE_SERVERS)
    {
        log_event("ERROR: Maximum number of storage servers reached");
        pthread_mutex_unlock(&servers_mutex);
        close(client_sock);
        return NULL;
    }

    char *paths_start = strstr(buffer, "Paths: ");
    if (!paths_start)
    {
        log_event("ERROR: Paths not found in message");
        pthread_mutex_unlock(&servers_mutex);
        close(client_sock);
        return NULL;
    }
    paths_start += 7;
    int flag = 0;
    int server_index = num_storage_servers;
    char ip_rev[2000];
    strncpy(ip_rev, ip, INET_ADDRSTRLEN - 1);
    ip_rev[INET_ADDRSTRLEN - 1] = '\0';
    for (int i = 0; i < num_storage_servers; i++)
    {

        if (storage_servers[i].port == port && strcmp(ip_rev, storage_servers[i].ip) == 0)
        {
            storage_servers[i].removed = 0;
            log_event("%s server marked", storage_servers[i].accessible_paths[0]);
            flag = 1;
        }
    }
    if (flag == 0)
    {
        storage_server_info *server = &storage_servers[num_storage_servers];
        strncpy(server->ip, ip, INET_ADDRSTRLEN - 1);
        server->ip[INET_ADDRSTRLEN - 1] = '\0';
        server->port = port;
        server->transfer_port = port;
        server->socket = client_sock;
        server->removed = 0;
        char *path = strtok(paths_start, ",");
        storage_servers[num_storage_servers].accessible_paths = (char **)malloc(sizeof(char *) * 1000);
        storage_servers[num_storage_servers].num_paths = 0;
        while (path != NULL)
        {
            storage_servers[num_storage_servers].accessible_paths[storage_servers[num_storage_servers].num_paths] = strdup(path);
            insert_path(path, server_index);
            storage_servers[num_storage_servers].num_paths++;
            path = strtok(NULL, ",");
        }
        num_storage_servers++;
    }
    pthread_mutex_unlock(&servers_mutex);

    const char *ack_message = "Registration confirmed";
    if (send(client_sock, ack_message, strlen(ack_message), 0) < 0)
    {
        log_event("Error sending acknowledgment: %s", strerror(errno));
        close(client_sock);
        return NULL;
    }

    log_event("Storage server registered successfully - IP: %s, Port: %d", ip, port);

    memset(buffer, 0, sizeof(buffer));
    bytes_read = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read > 0)
    {
        buffer[bytes_read] = '\0';
        if (flag == 0)
            add_replication_entry();
        log_event("Received response from Storage Server: %s", buffer);
    }
    else
    {
        log_event("Error receiving response from Storage Server: %s", strerror(errno));
    }

    struct sockaddr_in cli_addr_ping;
    socklen_t cli_len_ping = sizeof(cli_addr_ping);
    int ping_client_sock = accept(fail_sock, (struct sockaddr *)&cli_addr_ping, &cli_len_ping);
    if (ping_client_sock < 0)
    {
        log_event("Failed to accept connection");
        return NULL;
    }

    int bytes_received = recv(ping_client_sock, buffer, sizeof(buffer), 0);
    if (bytes_received <= 0)
    {
        storage_servers[server_index].removed = 1;
        log_event("Storage server %s %d disconnected", storage_servers[server_index].ip, storage_servers[server_index].port);
    }
}

int send_paths_to_client(int client_sock)
{
    // Validate input parameters
    if (client_sock < 0)
    {
        log_event("LISTPATHS: Invalid client socket");
        return -1;
    }

    // Additional null checks
    if (!map || !map->buckets)
    {
        log_event("LISTPATHS: Hashmap or buckets not initialized");
        return 1;
    }

    char *response = NULL;
    size_t response_size = 1000000;
    response = malloc(response_size);
    if (!response)
    {
        log_event("LISTPATHS: Memory allocation failed for response");
        return -1;
    }
    memset(response, 0, response_size);

    char temp_buffer[512];
    char *timestamp = NULL;

    timestamp = get_timestamp();
    if (!timestamp)
    {
        log_event("LISTPATHS: Failed to get timestamp");
        free(response);
        return -1;
    }

    snprintf(temp_buffer, sizeof(temp_buffer), "%s Available Paths:\n", timestamp);
    free(timestamp);

    size_t response_len = 0;
    if (strlcat(response, temp_buffer, response_size) >= response_size)
    {
        log_event("LISTPATHS: Buffer overflow during initial concatenation");
        free(response);
        return -1;
    }

    int path_count = 0;
    int send_success = 0;

    size_t safe_capacity = (map->capacity > 1000000) ? 1000000 : map->capacity;

    for (size_t i = 0; i < safe_capacity; i++)
    {
        HashNode *current = map->buckets[i];
        while (current && current->path)
        {
            path_count++;
            timestamp = get_timestamp();
            if (!timestamp)
            {
                log_event("LISTPATHS: Failed to get timestamp during iteration");
                break;
            }

            snprintf(temp_buffer, sizeof(temp_buffer),
                     " [%d] Path: '%s'\n",
                     path_count, current->path);

            response_len = strlen(response);
            if (strlcat(response, temp_buffer, response_size) >= response_size)
            {
                log_event("LISTPATHS: Response buffer overflow");
                free(timestamp);
                break;
            }

            free(timestamp);
            send_success = 1;
            current = current->next;
        }
    }

    timestamp = get_timestamp();
    if (!timestamp)
    {
        log_event("LISTPATHS: Failed to get final timestamp");
        free(response);
        return -1;
    }

    snprintf(temp_buffer, sizeof(temp_buffer),
             "%s Total Paths: %d\n", timestamp, path_count);
    free(timestamp);

    response_len = strlen(response);
    if (strlcat(response, temp_buffer, response_size) >= response_size)
    {
        log_event("LISTPATHS: Final buffer overflow");
        free(response);
        return -1;
    }

    if (!send_success)
    {
        log_event("LISTPATHS: No paths found in hashmap");
        free(response);
        return 1;
    }

    ssize_t bytes_sent = send(client_sock, response, strlen(response), 0);
    if (bytes_sent == -1)
    {
        log_event("LISTPATHS: Failed to send response to client. Error: %s", strerror(errno));
        free(response);
        return 2;
    }

    log_event("LISTPATHS: Successfully sent %d paths to client", path_count);
    free(response);
    return 0;
}

void *handle_client(void *arg)
{
    ThreadArgs *args = (ThreadArgs *)arg;
    if (!args)
    {
        log_event("ERROR: NULL thread arguments");
        return NULL;
    }

    int client_sock = args->sock;
    char buffer[BUFFER_SIZE];
    strncpy(buffer, args->buffer, BUFFER_SIZE - 1);
    buffer[BUFFER_SIZE - 1] = '\0';

    free(args);

    log_event("Processing client request: %s", buffer);

    if (send(client_sock, "ACK", 3, 0) < 0)
    {
        log_event("ERROR: Failed to send acknowledgment");
        close(client_sock);
        return NULL;
    }

    char command[32] = {0};
    char path[MAX_PATH_LENGTH] = {0};
    char name[MAX_PATH_LENGTH] = {0};
    char dest_path[MAX_PATH_LENGTH] = {0};

    int parsed_items = sscanf(buffer, "%31s %511s %511s", command, path, name);
    char response[BUFFER_SIZE];

    if (strcmp(command, "READ") == 0 || strcmp(command, "WRITE") == 0 || strcmp(command, "INFO") == 0 || strcmp(command, "STREAM") == 0)
    {
        if (parsed_items != 2)
        {
            log_event("Invalid input format");
            strcpy(response, "ERROR: Invalid command format");
            send(client_sock, response, strlen(response), 0);
            close(client_sock);
            return NULL;
        }

        pthread_mutex_lock(&servers_mutex);

        log_event("Searching for path: %s", path);
        int server_index = search_path(path);
        log_event("Server index for path %s: %d", path, server_index);

        if (server_index >= 0 && server_index < num_storage_servers)
        {
            if (storage_servers[server_index].removed && strcmp(command, "READ") != 0)
            {
                snprintf(response, sizeof(response), "ERROR: No such path is accessible on any storage server");
                log_event("Path not found: %s", path);
            }
            else if (storage_servers[server_index].removed)
            {
                if (storage_servers[storage_servers[server_index].replica1].removed == 0)
                {
                    strcat(path, "/.");
                    strcat(path, storage_servers[storage_servers[server_index].replica1].accessible_paths[0]);
                    snprintf(response, sizeof(response), "%s %d %d",
                             storage_servers[storage_servers[server_index].replica1].ip,
                             storage_servers[storage_servers[server_index].replica1].port,
                             storage_servers[storage_servers[server_index].replica1].transfer_port);
                    log_event("Sending server info: IP=%s, Port=%d, Transfer Port=%d",
                              storage_servers[storage_servers[server_index].replica1].ip,
                              storage_servers[storage_servers[server_index].replica1].port,
                              storage_servers[storage_servers[server_index].replica1].transfer_port);
                }
                else if (storage_servers[storage_servers[server_index].replica2].removed == 0)
                {
                    strcat(path, "/.");
                    strcat(path, storage_servers[storage_servers[server_index].replica2].accessible_paths[0]);
                    snprintf(response, sizeof(response), "%s %d %d",
                             storage_servers[storage_servers[server_index].replica2].ip,
                             storage_servers[storage_servers[server_index].replica2].port,
                             storage_servers[storage_servers[server_index].replica2].transfer_port);
                    log_event("Sending server info: IP=%s, Port=%d, Transfer Port=%d",
                              storage_servers[storage_servers[server_index].replica2].ip,
                              storage_servers[storage_servers[server_index].replica2].port,
                              storage_servers[storage_servers[server_index].replica2].transfer_port);
                }
                else
                {
                    snprintf(response, sizeof(response), "ERROR: No such path is accessible on any storage server");
                    log_event("Path not found: %s", path);
                }
            }
            else
            {
                snprintf(response, sizeof(response), "%s %d %d",
                         storage_servers[server_index].ip,
                         storage_servers[server_index].port,
                         storage_servers[server_index].transfer_port);
                log_event("Sending server info: IP=%s, Port=%d, Transfer Port=%d",
                          storage_servers[server_index].ip,
                          storage_servers[server_index].port,
                          storage_servers[server_index].transfer_port);

                sleep(1);
            }
        }
        else
        {
            snprintf(response, sizeof(response), "ERROR: No such path is accessible on any storage server");
            log_event("Path not found: %s", path);
        }
        pthread_mutex_unlock(&servers_mutex);

        if (send(client_sock, response, strlen(response), 0) < 0)
        {
            log_event("Failed to send response to client: %s", strerror(errno));
        }
    }
    else
    {
        if (parsed_items != 3 && strcmp(command, "LISTPATHS") != 0)
        {
            log_event("ERROR: Invalid input format");
            strcpy(response, "ERROR: Invalid input format");
            send(client_sock, response, strlen(response), 0);
            close(client_sock);
            return NULL;
        }

        const char *type = (strchr(name, '.') != NULL) ? "FILE" : "DIR";
        const char *typecopy = (strchr(path, '.') != NULL) ? "FILE" : "DIR";
        int success = -1;

        if (strcmp(command, "CREATE") == 0)
        {
            log_event("CREATE command - Type: %s, Path: %s, Name: %s", type, path, name);

            success = send_create_command(type, name, path);
            int server_index = search_path(path);
            if (num_storage_servers > 2 && storage_servers[server_index].replicated == 2 && success == 0)
            {
                char str[5000];
                str[0] = '\0';
                strcpy(str, storage_servers[server_index].accessible_paths[0]);
                int success2 = -1;
                int success3 = -1;
                if (storage_servers[storage_servers[server_index].replica1].removed == 0)
                {
                    char str1[5000];
                    strcpy(str1, storage_servers[storage_servers[server_index].replica1].accessible_paths[0]);
                    strcat(str1, "/.");
                    strcat(str1, path + 1);
                    success2 = send_create_command(type, name, str1);
                    printf("%s\t%s\t%s", str1, name, path);
                }
                if (storage_servers[storage_servers[server_index].replica2].removed == 0)
                {
                    char str2[5000];
                    strcpy(str2, storage_servers[storage_servers[server_index].replica2].accessible_paths[0]);
                    strcat(str2, "/.");
                    strcat(str2, path + 1);
                    printf("%s\t%s\t%s", str2, name, path);
                    success3 = send_create_command(type, name, str2);
                }
            }
        }
        else if (strcmp(command, "DELETE") == 0)
        {
            log_event("DELETE command - Type: %s, Path: %s, Name: %s", type, path, name);

            success = send_delete_command(type, name, path);
            int server_index = search_path(path);
            if (num_storage_servers > 2 && storage_servers[server_index].replicated == 2 && success == 0)
            {
                char str[5000];
                str[0] = '\0';
                strcpy(str, storage_servers[server_index].accessible_paths[0]);
                int success2 = -1;
                int success3 = -1;
                if (storage_servers[storage_servers[server_index].replica1].removed == 0)
                {
                    char str1[5000];
                    strcpy(str1, storage_servers[storage_servers[server_index].replica1].accessible_paths[0]);
                    strcat(str1, "/.");
                    strcat(str1, path + 1);
                    success2 = send_delete_command(type, name, str1);
                }
                if (storage_servers[storage_servers[server_index].replica2].removed == 0)
                {
                    char str2[5000];
                    strcpy(str2, storage_servers[storage_servers[server_index].replica2].accessible_paths[0]);
                    strcat(str2, "/.");
                    strcat(str2, path + 1);
                    success3 = send_delete_command(type, name, str2);
                }
            }
        }
        else if (strcmp(command, "COPY") == 0)
        {
            log_event("COPY command - Type: %s, Path: %s, Destination: %s", typecopy, path, name);
            success = send_copy_command(typecopy, path, name);
            int server_index = search_path(path);
            if (num_storage_servers > 2 && storage_servers[server_index].replicated == 2 && success == 0)
            {
                char str[5000];
                str[0] = '\0';
                strcpy(str, storage_servers[server_index].accessible_paths[0]);
                int success2 = -1;
                int success3 = -1;
                if (storage_servers[storage_servers[server_index].replica1].removed == 0)
                {
                    char str1[5000];
                    strcpy(str1, storage_servers[storage_servers[server_index].replica1].accessible_paths[0]);
                    strcat(str1, "/.");
                    strcat(str1, name + 1);
                    success2 = send_copy_command(typecopy, path, str1);
                }
                if (storage_servers[storage_servers[server_index].replica2].removed == 0)
                {
                    char str2[5000];
                    strcpy(str2, storage_servers[storage_servers[server_index].replica2].accessible_paths[0]);
                    strcat(str2, "/.");
                    strcat(str2, name + 1);
                    success3 = send_copy_command(typecopy, path, str2);
                }
            }
        }
        else if (strcmp(command, "LISTPATHS") == 0)
        {
            log_event("LISTPATHS command - Type: %s, Path: %s, Name: %s", type, path, name);
            success = send_paths_to_client(client_sock);
        }
        else
        {
            const char *error_msg = "ERROR: Invalid input format";
            send(client_sock, error_msg, strlen(error_msg), 0);
            close(client_sock);
            return NULL;
        }

        const char *response;
        switch (success)
        {
        case 0:
            response = "SUCCESS: Request successful";
            break;
        case 1:
            response = "ERROR: No such path is accessible on any storage server";
            break;
        case 2:
            response = "ERROR: Naming server failed to send command to storage server";
            break;
        case 3:
            response = "ERROR: No response received from storage server";
            break;
        case 4:
            response = "ERROR: System error occurred on storage server";
            break;
        case 5:
            response = "ERROR: Invalid input format";
            break;
        case 6:
            response = "ERROR: Error in establishing connection between storage servers";
            break;
        default:
            response = "ERROR: Unknown error occurred";
        }

        send(client_sock, response, strlen(response), 0);
    }

    close(client_sock);
    return NULL;
}