#define _LARGEFILE64_SOURCE
#define _XOPEN_SOURCE 500 // FTW_DEPTH | FTW_PHYS
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
#include <ftw.h> // ntfw

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

uint64_t issue_stat(char *root_path, int num_dirs, int files_per_dir) {
    int dir_id, file_id;
    uint64_t begin, end;
    char pathbuf[256];

    struct stat st;

    dir_id = rand() % num_dirs;
    file_id = rand() % files_per_dir;

    snprintf(pathbuf, sizeof pathbuf, "%s/%d/%d", root_path, dir_id, file_id);

    begin = stamp();

    if (stat(pathbuf, &st) != 0) {
        fprintf(stderr, "Couldn't stat() to %s\n", pathbuf);
        return -1;
    }

    end = stamp();

    return (end - begin);
}

void issue_operation_based_stats(uint64_t* stat_latencies, char *root_path, int num_ops, int num_dirs, int files_per_dir) {
    for (int op = 0; op < num_ops; ++op) {
        stat_latencies[op] = issue_stat(root_path, num_dirs, files_per_dir);
    }
}

uint64_t issue_time_based_stats(char *root_path, uint64_t user_defined_runtime, int num_dirs, int files_per_dir) {
    uint64_t curr_runtime = 0;
    uint64_t ops = 0;
    uint64_t user_defined_runtime_ns = user_defined_runtime * 1000000000ULL;
    
    while(curr_runtime < user_defined_runtime_ns) {
        curr_runtime += issue_stat(root_path, num_dirs, files_per_dir);
        ops += 1;
    }
    
    return curr_runtime/ops;
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

int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    int rv = remove(fpath);
    if (rv) {
        perror(fpath);
    }
    return rv;
}

int recursive_remove(char *path) {
    return nftw(path, unlink_cb, FTW_D, FTW_DEPTH | FTW_PHYS);
}

void delete_file_tree(char* root_path, int num_dirs) {
    char* dst_path = (char*) calloc(256, sizeof(char));

    for (int dir = 0; dir < num_dirs; ++dir) {
        sprintf(dst_path, "%s/%d", root_path, dir);
        recursive_remove(dst_path);
    }
}

// To run, type: ./stat <path> <num_ops> <num_dirs> <files_per_dir> full-lat|res-lat
int main(int argc, char* argv[]) {
    if (argc < 7) {
        fprintf(stderr, "Usage: ./stat <path> <num_ops> <num_dirs> <files_per_dir> full-lat|res-lat time-based|no-time.\n");
        exit(EXIT_FAILURE);
    }

    char* path = argv[1];

    // It can represent the time in seconds or number of operations based in what flag
    // is being used: time-based or no-time 
    int stat_load = atoi(argv[2]);

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

    int time_based;
    if (strcmp(argv[6], "time-based") == 0) {
        time_based = 1;
    } else if (strcmp(argv[6], "no-time") == 0) {
        time_based = 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: time-based or no-time.\n", argv[6]);
        exit(EXIT_FAILURE);
    }

    create_file_tree(path, num_dirs, files_per_dir, 1);
    
    // Creating random seed
    srand(time(NULL));

    if (time_based) {
        printf("%ld", issue_time_based_stats(path, stat_load, num_dirs, files_per_dir));
    } else {
        uint64_t * stat_latencies = (uint64_t*) calloc (stat_load, sizeof(uint64_t));
        issue_operation_based_stats(stat_latencies, path, stat_load, num_dirs, files_per_dir);
        print_latencies(stat_load, stat_latencies, detailed_latency);
    }

    delete_file_tree(path, num_dirs);

    return EXIT_SUCCESS;
}

