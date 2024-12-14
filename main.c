#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <bits/stdint-uintn.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

const int32_t ALL = 0;
const int32_t DEFAULT = 1;
const int32_t DIRECT = 2;
const int32_t SYNC = 3;
const int32_t FSYNC = 4;
const int32_t DSYNC = 5;
const int32_t DIRECT_SYNC = 6;
const int32_t DIRECT_FSYNC = 7;
const int32_t DIRECT_DSYNC = 8;

const int32_t MODE_COUNT = 8;
const char *MODE_NAMES[] = { "all", "default", "direct", "sync", "fsync", "dsync", "direct_sync", "direct_fsync", "direct_dsync" };
const int32_t MODES[] = { DEFAULT, DIRECT, SYNC, FSYNC, DSYNC, DIRECT_SYNC, DIRECT_FSYNC, DIRECT_DSYNC };

typedef struct {
    int32_t fd;
    int32_t thread_id;
    int32_t thread_count;
    int32_t operation_count;
    int32_t data_size;
    int32_t type;
    int32_t mode;
} info_t;

uint64_t epoch_millis() {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    char time_str[32];
    sprintf(time_str, "%ld.%.9ld", tv.tv_sec, tv.tv_nsec);
    char *end;
    double epoch = strtod(time_str, &end);
    if (end == time_str) {
        printf("Cant convert time_str to epoch double");
        exit(-1);
    }
    return (uint64_t) (epoch * 1E3);
}

void *handle_operations(void *arg) {
    info_t *info = (info_t*)arg;
    uint8_t data[info->data_size];
    for (int i = 0; i < info->data_size; i++) {
//        data[i] = (uint8_t) (((double) rand_r(&seed) / (double) RAND_MAX) * 256);
        data[i] = 97 + info->thread_id;
    }

    //TODO: Figure out why I have to use hacky __O_DIRECT instead of having proper declaration
    if (info->fd == -1) {
        printf("Unable to open file.txt\n");
        return NULL;
    }
    uint64_t mark = epoch_millis();
    for (int i = 0; i < info->operation_count; i++) {
        pwrite(info->fd, &data, info->data_size, i * info->data_size * info->thread_count + info->thread_id * info->data_size);
//        printf("Thread(%d) %zd %d\n", info->thread_id, wrote, info->data_size);
//        if (info->mode == DIRECT_FSYNC || info->mode == FSYNC) fsync(info->fd);
//        else if (info->mode == DIRECT_DSYNC || info->mode == DSYNC) fdatasync(info->fd);
    }
    uint64_t result = epoch_millis() - mark;
    char *type_name = info->type == O_WRONLY ? "write" : "read";
    printf("Thread(%d) completed %d %s %s operations in %ld milliseconds\n",
           info->thread_id, info->operation_count, MODE_NAMES[info->mode], type_name, result);
    fsync(info->fd);
    free(info);
    return NULL;
}

void run(int32_t operation_count, int32_t thread_count, int32_t data_size, int32_t type, int32_t mode) {
    if (access("file.txt", F_OK) == 0) {
        remove("file.txt");
    }

    int flags;
    if (mode == DIRECT) flags = __O_DIRECT;
    else if (mode == SYNC) flags = O_SYNC;
    else if (mode == FSYNC) flags = O_FSYNC;
    else if (mode == DSYNC) flags = O_DSYNC;
    else if (mode == DIRECT_SYNC) flags = __O_DIRECT | O_SYNC;
    else if (mode == DIRECT_FSYNC) flags = __O_DIRECT | O_FSYNC;
    else if (mode == DIRECT_DSYNC) flags = __O_DIRECT | O_DSYNC;
    else flags = 0;

    int32_t fd = open("file.txt", O_CREAT | type | flags);

    pthread_t threads[thread_count];
    for (int32_t i = 0; i < thread_count; i++) {
        info_t *info = malloc(sizeof(info_t));
        info->fd = fd;
        info->thread_id = i;
        info->thread_count = thread_count;
        info->operation_count = operation_count;
        info->data_size = data_size;
        info->type = type;
        info->mode = mode;
        if (pthread_create(&threads[i], NULL, handle_operations, info) != 0) {
            perror("Failed to create thread");
            exit(-1);
        }
    }
    for (int32_t i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    close(fd);
}

int main(int argc, char *argv[]) {
    if (argc != 6) {
        printf("Expected arguments: (operation_count) (thread_count) (data_size) (read|write) (all|default|direct|sync|fsync|dsync|direct_sync|direct_fsync|direct_dsync)\n");
        exit(-1);
    }
    char* end;
    int32_t operation_count = (int32_t) strtol(argv[1], &end, 10);
    if (end == argv[1]) {
        printf("Cant convert %s to integer", argv[1]);
        exit(-1);
    }
    int32_t thread_count = (int32_t) strtol(argv[2], &end, 10);
    if (end == argv[2]) {
        printf("Cant convert %s to integer", argv[2]);
        exit(-1);
    }
    int32_t data_size = (int32_t) strtol(argv[3], &end, 10);
    if (end == argv[3]) {
        printf("Cant convert %s to integer", argv[3]);
        exit(-1);
    }
    char *type_name = argv[4];
    int32_t type;
    if (strcmp(type_name, "write") == 0) type = O_WRONLY;
    else if (strcmp(type_name, "read") == 0) type = O_RDONLY;
    else {
        printf("Type was not read|write");
        exit(-1);
    }
    char *mode_name = argv[5];
    int32_t mode;
    if (strcmp(mode_name, "default") == 0) mode = DEFAULT;
    else if (strcmp(mode_name, "direct") == 0) mode = DIRECT;
    else if (strcmp(mode_name, "sync") == 0) mode = SYNC;
    else if (strcmp(mode_name, "fsync") == 0) mode = FSYNC;
    else if (strcmp(mode_name, "dsync") == 0) mode = DSYNC;
    else if (strcmp(mode_name, "direct_sync") == 0) mode = DIRECT_SYNC;
    else if (strcmp(mode_name, "direct_fsync") == 0) mode = DIRECT_FSYNC;
    else if (strcmp(mode_name, "direct_dsync") == 0) mode = DIRECT_DSYNC;
    else if (strcmp(mode_name, "all") == 0) mode = ALL;
    else {
        printf("Mode was not default|osync|odirect_osync|odirect_fsync|odirect_dsync");
        exit(-1);
    }
    if (mode == ALL) {
        for (int i = 0; i < MODE_COUNT; i++) {
            run(operation_count, thread_count, data_size, type, MODES[i]);
        }
    } else {
        run(operation_count, thread_count, data_size, type, mode);
    }
    return 0;
}
