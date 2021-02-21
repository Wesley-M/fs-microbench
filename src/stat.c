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

typedef int8_t error_t;

typedef struct thread_stat_load {
    int thread_id;
    uint64_t* stat_latencies;
    size_t stat_latencies_size;
    char *root_path;
    uint64_t num_ops;
    uint64_t max_ops;
    int num_dirs;
    int files_per_dir;
    uint64_t elapsed_time_ns;
    uint64_t maximum_time_ns;
    error_t error;
} thread_stat_load;

static uint64_t stamp (void) {
   struct timespec tspec;
   if (clock_gettime (CLOCK_MONOTONIC, &tspec)) {
       perror("Error getting timestamp");
       exit(EXIT_FAILURE);
   }
   return (tspec.tv_sec * 1000000000ULL) + tspec.tv_nsec;
}

error_t issue_stat(struct thread_stat_load* load) {
    int dir_id, file_id;
    uint64_t begin, end;
    char pathbuf[256];

    struct stat st;

    dir_id = rand() % load->num_dirs;
    file_id = rand() % load->files_per_dir;

    snprintf(pathbuf, sizeof pathbuf, "%s/%d/%d", load->root_path, dir_id, file_id);

    begin = stamp();

    if (stat(pathbuf, &st) != 0) {
        fprintf(stderr, "Couldn't stat() to %s\n", pathbuf);
        return -1;
    }

    end = stamp();

    if (load->max_ops != UINT64_MAX) {
        load->stat_latencies[load->num_ops] = end - begin;
    }

    ++load->num_ops;
    load->elapsed_time_ns += end - begin;

    return 0;
}



static void* thread_init(void* args) {
    thread_stat_load* load = (thread_stat_load*) args;

    while ((load->elapsed_time_ns < load->maximum_time_ns) && (load->num_ops < load->max_ops)) {
        load->error = issue_stat(load);
        if (-1 == load->error) {
            fprintf(stderr, "Aborting on thread %d due stat() error.\n", load->thread_id);
            return NULL;
        }
    }

    return NULL;
}

// Print latency(ies) in nanoseconds
void print_latencies(thread_stat_load* load, int threads, int detailed_latency) {
    for (int thread = 0; thread < threads; ++thread) {
        if (-1 == load[thread].error) {
            fprintf(stderr, "Ignoring latencies due stat() error(s).\n");
            exit(EXIT_FAILURE);
        }
    }

    if (detailed_latency) {
        for (int thread = 0; thread < threads; ++thread) {
            for (size_t i = 0; i < load[thread].stat_latencies_size; ++i) {
                printf("%ld\n", load[thread].stat_latencies[i]);
            }
        }
    } else {    // show average of latencies
        uint64_t average = 0UL;
        uint64_t total_ops = 0UL;
        for (int thread = 0; thread < threads; thread++) {
            total_ops += load[thread].num_ops;
            average += load[thread].elapsed_time_ns;
        }
        printf("%ld\n", average / total_ops);
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

// To run, type: ./stat <path> <load> <num_dirs> <files_per_dir> full-lat|res-lat  time-based|no-time
int main(int argc, char* argv[]) {
    if (argc < 7) {
        fprintf(stderr, "Usage: ./stat <path> <load> <num_dirs> <files_per_dir> <num_threads> full-lat|res-lat  time-based|no-time.\n");
        exit(EXIT_FAILURE);
    }

    char* path = argv[1];

    // It can represent the time in seconds or number of operations based in what flag
    // is being used: time-based or no-time
    int stat_load = atoi(argv[2]);

    int num_dirs = atoi(argv[3]);
    int files_per_dir = atoi(argv[4]);
    int num_threads = atoi(argv[5]);

    int detailed_latency;
    if (strcmp(argv[6], "full-lat") == 0) {
        detailed_latency = 1;
    } else if (strcmp(argv[6], "res-lat") == 0) {
        detailed_latency = 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: full-lat or res-lat.\n", argv[6]);
        exit(EXIT_FAILURE);
    }

    int time_based;
    if (strcmp(argv[7], "time-based") == 0) {
        time_based = 1;
    } else if (strcmp(argv[7], "no-time") == 0) {
        time_based = 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: time-based or no-time.\n", argv[7]);
        exit(EXIT_FAILURE);
    }

    create_file_tree(path, num_dirs, files_per_dir, 1);

    // Creating random seed
    srand(time(NULL));

    thread_stat_load* load = (thread_stat_load*) calloc(num_threads, sizeof(struct thread_stat_load));

    if (time_based) {
        uint64_t* stat_latencies = (uint64_t*) calloc(num_threads, sizeof(uint64_t));
        for (int thread = 0; thread < num_threads; ++thread) {
            load[thread].thread_id = thread;
            load[thread].stat_latencies = &(stat_latencies[thread]);
            load[thread].stat_latencies_size = 1UL;
            load[thread].root_path = path;
            load[thread].num_ops = 0UL;
            load[thread].max_ops = UINT64_MAX;
            load[thread].num_dirs = num_dirs;
            load[thread].files_per_dir = files_per_dir;
            load[thread].elapsed_time_ns = 0UL;
            load[thread].maximum_time_ns = stat_load;
            load[thread].error = 0;
        }
    } else {
        uint64_t * stat_latencies = (uint64_t*) calloc(num_threads * stat_load, sizeof(uint64_t));
        for (int thread = 0; thread < num_threads; ++thread) {
            load[thread].thread_id = thread;
            load[thread].stat_latencies = &(stat_latencies[thread * stat_load]);
            load[thread].stat_latencies_size = stat_load;
            load[thread].root_path = path;
            load[thread].num_ops = 0UL;
            load[thread].max_ops = stat_load;
            load[thread].num_dirs = num_dirs;
            load[thread].files_per_dir = files_per_dir;
            load[thread].elapsed_time_ns = 0UL;
            load[thread].maximum_time_ns = UINT64_MAX;
            load[thread].error = 0;
        }
    }

    pthread_t* requesters = (pthread_t*) malloc(num_threads * sizeof(pthread_t));
    for (int thread = 0; thread < num_threads; ++thread) {
        pthread_create(&requesters[thread], NULL, thread_init, (void*) &load[thread]);
    }

    for (int thread = 0; thread < num_threads; ++thread) {
        pthread_join(requesters[thread], NULL);
    }

    print_latencies(load, num_threads, detailed_latency);
    delete_file_tree(path, num_dirs);

    return EXIT_SUCCESS;
}

