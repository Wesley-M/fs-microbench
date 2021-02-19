#define _LARGEFILE64_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h> //usleep
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <stdint.h> //uint64_t
#include <stdlib.h> //rand

#define __STDC_FORMAT_MACRO
#include <inttypes.h>

#define ACCESS_PERMISSION 0777

uint64_t mean(uint64_t * numbers, size_t size) {
    uint64_t sum = 0;
    for (size_t i = 0; i < size; i++) {
        sum += numbers[i];
    }
    return sum/size;
}

static uint64_t stamp (void) {
   struct timespec tspec;
   if (clock_gettime (CLOCK_MONOTONIC, &tspec)) {
       perror("Error getting timestamp");
       exit(EXIT_FAILURE);
   }
   return (tspec.tv_sec * 1000000000ULL) + tspec.tv_nsec;
}

void issue_stats(uint64_t* stat_latencies, char *root_path, int num_ops, int num_dirs, int files_per_dir) {
    int op, dir_id, file_id;
    uint64_t begin, end;
    char pathbuf[256];

    struct stat st;

    srand(time(NULL));

    for (op = 0; op < num_ops; ++op) {
        dir_id = rand() % num_dirs;
        file_id = rand() % files_per_dir;

        snprintf(pathbuf, sizeof pathbuf, "%s/%d/%d", root_path, dir_id, file_id);

        begin = stamp();

        if (stat(pathbuf, &st) != 0) {
            fprintf(stderr, "Couldn't stat() to %s\n", pathbuf);
            stat_latencies[op] = -1;
            return;
        }

        end = stamp();

        // Saving latency
        stat_latencies[op] = (end - begin);
    }
}

// Print latency(ies) in nanoseconds
void print_latencies(int num_ops, uint64_t * stat_latencies, int detailed_latency) {
    for (int i = 0; i < num_ops; ++i) {
        if (stat_latencies[i] == (uint64_t) -1) {
            fprintf(stderr, "Ignoring latencies due stat() error(s).\n");
            exit(EXIT_FAILURE);
        }
    }

    if (detailed_latency) {
        for (int i = 0; i < num_ops; i++) {
            printf("%ld\n", stat_latencies[i]);
        }
    } else {
        printf("%ld\n", mean(stat_latencies, num_ops));
    }
}

void create_file_tree(char* root_path, int num_dirs, int files_per_dir, int deep) {
    char* dst_path = (char*) calloc(256, sizeof(char));
    if (0 == deep) {
        for (int file = 0; file < files_per_dir; ++file) {
            sprintf(dst_path, "%s/%d", root_path, file);
            if ((0 != mknod(dst_path, S_IFREG | ACCESS_PERMISSION, 0)) && (errno != EEXIST)) {
                perror("Failed to create file");
                exit(EXIT_FAILURE);
            }
        }
    } else {
        for (int dir = 0; dir < num_dirs; ++dir) {
            sprintf(dst_path, "%s/%d", root_path, dir);

            if ((0 != mkdir(dst_path, ACCESS_PERMISSION)) && (errno != EEXIST)) {
                perror("Failed to create directory");
                exit(EXIT_FAILURE);
            }

            create_file_tree(dst_path, num_dirs, files_per_dir, deep - 1);
        }
    }
}

// To run, type: ./stat <path> <num_ops> <num_dirs> <files_per_dir> full-lat|res-lat
int main(int argc, char* argv[]) {
    if (argc < 6) {
        fprintf(stderr, "Usage: ./stat <path> <num_ops> <num_dirs> <files_per_dir> full-lat|res-lat.\n");
        exit(EXIT_FAILURE);
    }

    char* path = argv[1];
    int num_ops = atoi(argv[2]);
    int num_dirs = atoi(argv[3]);
    int files_per_dir = atoi(argv[4]);

    int detailed_latency;
    if (strcmp(argv[5], "full-lat") == 0) {
        detailed_latency = 1;
    } else if (strcmp(argv[5], "res-lat") == 0) {
        detailed_latency = 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: full-lat or res-lat.\n", argv[5]);
        exit(EXIT_FAILURE);
    }

    create_file_tree(path, num_dirs, files_per_dir, 1);

    uint64_t * stat_latencies = (uint64_t*) calloc (num_ops, sizeof(uint64_t));
    issue_stats(stat_latencies, path, num_ops, num_dirs, files_per_dir);
    print_latencies(num_ops, stat_latencies, detailed_latency);

    return EXIT_SUCCESS;
}

