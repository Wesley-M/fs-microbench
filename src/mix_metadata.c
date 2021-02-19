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

typedef struct latencies {
    uint64_t* create;
    uint64_t* stat;
    uint64_t* unlink;
} latencies;

typedef struct thread_load {
    int thread_id;
    latencies latencies_out;
    char *root_path;
    int num_mixes;
    int offset;
} thread_load;

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

void issue_mixes(struct latencies latencies_out, char *root_path, int num_mixes, int offset) {
    int mix;
    uint64_t begin, end;

    struct stat st;

    char* dst_path = (char*) calloc(256, sizeof(char));

    for (mix = 0; mix < num_mixes; ++mix) {

        sprintf(dst_path, "%s/%d", root_path, mix+offset);

        // Measuring the create operation

        begin = stamp();

        if ((0 != mknod(dst_path, S_IFREG | ACCESS_PERMISSION, 0)) && (errno != EEXIST)) {
            fprintf(stderr, "Couldn't mknod() to %s\n", dst_path);
            latencies_out.create[mix+offset] = -1;
            return;
        }

        end = stamp();
        latencies_out.create[mix+offset] = (end - begin);

        // Measuring the stat operation

        begin = stamp();

        if (stat(dst_path, &st) != 0) {
            fprintf(stderr, "Couldn't stat() to %s\n", dst_path);
            latencies_out.stat[mix+offset] = -1;
            return;
        }

        end = stamp();
        latencies_out.stat[mix+offset] = (end - begin);

        // Measuring the unlink operation

        begin = stamp();

        if (unlink(dst_path) != 0) {
            fprintf(stderr, "Couldn't unlink() to %s\n", dst_path);
            latencies_out.unlink[mix+offset] = -1;
            return;
        }

        end = stamp();
        latencies_out.unlink[mix+offset] = (end - begin);
    }
}

static void* thread_init(void* args) {
    thread_load* load = (thread_load*) args;
    issue_mixes(load->latencies_out, load->root_path, load->num_mixes, load->offset);
    return NULL;
}

// Print latency(ies) in nanoseconds
void print_latencies(int latencies, struct latencies latencies_out, int detailed_latency) {
    int error = 0;
    for (int i = 0; i < latencies; ++i) {
        if (latencies_out.create[i] == (uint64_t) -1) {
            fprintf(stderr, "Ignoring latencies due mknod() error(s).\n");
            error = 1;
        }
        if (latencies_out.stat[i] == (uint64_t) -1) {
            fprintf(stderr, "Ignoring latencies due stat() error(s).\n");
            error = 1;
        } 
        if (latencies_out.unlink[i] == (uint64_t) -1) {
            fprintf(stderr, "Ignoring latencies due unlink() error(s).\n");
            error = 1;
        }

        if (error) {
            exit(EXIT_FAILURE);
        }
    }

    if (detailed_latency) {
        for (int i = 0; i < latencies; i++) {
            printf("%ld,%ld,%ld\n", latencies_out.create[i], latencies_out.stat[i], latencies_out.unlink[i]);
        }
    } else {
        printf("%ld,%ld,%ld\n", 
                mean(latencies_out.create, latencies), 
                mean(latencies_out.stat, latencies), 
                mean(latencies_out.unlink, latencies));
    }
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: ./stat <path> <num_mixes_per_thread> <num_threads> full-lat|res-lat\n");
        exit(EXIT_FAILURE);
    }

    char* path = argv[1];
    int num_mixes_per_thread = atoi(argv[2]);
    int num_threads = atoi(argv[3]);

    int detailed_latency;
    if (strcmp(argv[4], "full-lat") == 0) {
        detailed_latency = 1;
    } else if (strcmp(argv[4], "res-lat") == 0) {
        detailed_latency = 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: full-lat or res-lat.\n", argv[6]);
        exit(EXIT_FAILURE);
    }

    thread_load* load = (thread_load*) malloc(num_threads * sizeof(struct thread_load));

    latencies latencies_out = {
        .create = (uint64_t*) calloc(num_mixes_per_thread * num_threads, sizeof(uint64_t)),
        .stat = (uint64_t*) calloc(num_mixes_per_thread * num_threads, sizeof(uint64_t)),
        .unlink = (uint64_t*) calloc(num_mixes_per_thread * num_threads, sizeof(uint64_t))
    };

    for (int thread = 0; thread < num_threads; ++thread) {
        load[thread].thread_id = thread;
        load[thread].latencies_out = latencies_out;
        load[thread].root_path = path;
        load[thread].num_mixes = num_mixes_per_thread;
        load[thread].offset = (thread * num_mixes_per_thread);
    }

    pthread_t* requesters = (pthread_t*) malloc (num_threads * sizeof (pthread_t));
    for (int thread = 0; thread < num_threads; ++thread) {
        pthread_create(&requesters[thread], NULL, thread_init, (void *) &load[thread]);
    }

    for (int thread = 0; thread < num_threads; ++thread) {
        pthread_join(requesters[thread], NULL);
    }

    print_latencies(num_mixes_per_thread * num_threads, latencies_out, detailed_latency);
    return EXIT_SUCCESS;
}