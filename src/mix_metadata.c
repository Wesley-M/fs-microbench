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
    char *root_path;
    int mix_load;
    int offset;
} thread_load;

enum ops{
    CREATE, 
    STAT, 
    UNLINK
};

latencies op_based_latencies;
latencies time_based_latencies;

int detailed_latency;
int time_based;

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

uint64_t * issue_mix(char *root_path, char * filename) {
    uint64_t begin, end;
    uint64_t * latencies = (uint64_t*) calloc(3, sizeof(uint64_t));
    
    struct stat st;

    char* dst_path = (char*) calloc(256, sizeof(char));

    sprintf(dst_path, "%s/%s", root_path, filename);

    // Measuring the create operation

    begin = stamp();

    if ((0 != mknod(dst_path, S_IFREG | ACCESS_PERMISSION, 0)) && (errno != EEXIST)) {
        fprintf(stderr, "Couldn't mknod() to %s\n", dst_path);
        latencies[CREATE] = -1;
    } else {
        end = stamp();
        latencies[CREATE] = (end - begin);
    }

    // Measuring the stat operation

    begin = stamp();

    if (stat(dst_path, &st) != 0) {
        fprintf(stderr, "Couldn't stat() to %s\n", dst_path);
        latencies[STAT] = -1;
    } else {
        end = stamp();
        latencies[STAT] = (end - begin);
    }

    // Measuring the unlink operation

    begin = stamp();

    if (unlink(dst_path) != 0) {
        fprintf(stderr, "Couldn't unlink() to %s\n", dst_path);
        latencies[UNLINK] = -1;
    } else {
        end = stamp();
        latencies[UNLINK] = (end - begin);
    }
    
    return latencies;
}


void issue_operation_based_mixes(char *root_path, int num_mixes, int offset, int thread_id) {
    int mix;
    char * filename = (char*) calloc(256, sizeof(char));
    uint64_t * latencies;

    for (mix = 0; mix < num_mixes; ++mix) {
        snprintf(filename, sizeof filename, "%s-%d-%d", "mix", thread_id, mix);

        latencies = issue_mix(root_path, filename);
        op_based_latencies.create[mix+offset] = latencies[CREATE];
        op_based_latencies.stat[mix+offset] = latencies[STAT];
        op_based_latencies.unlink[mix+offset] = latencies[UNLINK];
    }
}

void issue_time_based_mixes(char *root_path, uint64_t user_defined_runtime, int thread_id) {
    uint64_t curr_runtime = 0;
    uint64_t user_defined_runtime_ns = user_defined_runtime * 1000000000ULL;

    uint64_t creates = 0, create_latencies = 0;
    uint64_t stats = 0, stat_latencies = 0;
    uint64_t unlinks = 0, unlink_latencies = 0;

    char * filename = (char*) calloc(256, sizeof(char));
    uint64_t * latencies;

    while(curr_runtime < user_defined_runtime_ns) {
        snprintf(filename, sizeof filename, "%s-%d-%ld", "mix", thread_id, creates);

        latencies = issue_mix(root_path, filename);

        create_latencies += latencies[CREATE];
        stat_latencies += latencies[STAT];
        unlink_latencies += latencies[UNLINK]; 

        curr_runtime += latencies[CREATE] + latencies[STAT] + latencies[UNLINK];

        creates += 1;
        stats += 1;
        unlinks += 1;
    }

    time_based_latencies.create[thread_id] = create_latencies/creates;
    time_based_latencies.stat[thread_id] = stat_latencies/stats;
    time_based_latencies.unlink[thread_id] = unlink_latencies/unlinks;
}

static void* thread_init(void* args) {
    thread_load* load = (thread_load*) args;

    if (time_based) {
        issue_time_based_mixes(load->root_path, load->mix_load, load->thread_id);
    } else {
        issue_operation_based_mixes(load->root_path, load->mix_load, load->offset, load->thread_id);
    }

    return NULL;
}

// Print latency(ies) in nanoseconds. 
// In case of time based execution, the output can be, depending on the latency flag:
//    full-lat: average latencies for each thread (create, stat, unlink)
//    res-lat: average latencies for all threads (create, stat, unlink)
// In case of operation based execution, the output can be, depending on the latency flag:
//    full-lat: all latencies of all thread (create, stat, unlink)
//    res-lat: average latencies for all threads (create, stat, unlink)
void print_latencies(struct latencies latencies_out, int size) {
    int error = 0;
    for (int i = 0; i < size; ++i) {
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
        for (int i = 0; i < size; i++) {
            printf("%ld,%ld,%ld\n", latencies_out.create[i], latencies_out.stat[i], latencies_out.unlink[i]);
        }
    } else {
        printf("%ld,%ld,%ld\n", 
                mean(latencies_out.create, size), 
                mean(latencies_out.stat, size), 
                mean(latencies_out.unlink, size));
    }
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: ./stat <path> <load_per_thread> <num_threads> full-lat|res-lat time-based|no-time\n");
        exit(EXIT_FAILURE);
    }

    char* path = argv[1];

    // It can represent the time in seconds or number of mixes based in what flag
    // is being used: time-based or no-time 
    int mix_load = atoi(argv[2]);

    int num_threads = atoi(argv[3]);

    if (strcmp(argv[4], "full-lat") == 0) {
        detailed_latency = 1;
    } else if (strcmp(argv[4], "res-lat") == 0) {
        detailed_latency = 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: full-lat or res-lat.\n", argv[6]);
        exit(EXIT_FAILURE);
    }

    if (strcmp(argv[5], "time-based") == 0) {
        time_based = 1;
    } else if (strcmp(argv[5], "no-time") == 0) {
        time_based = 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: time-based or no-time.\n", argv[6]);
        exit(EXIT_FAILURE);
    }

    thread_load* load = (thread_load*) malloc(num_threads * sizeof(struct thread_load));

    if (time_based) {
        time_based_latencies.create = (uint64_t*) calloc(num_threads, sizeof(uint64_t));
        time_based_latencies.stat = (uint64_t*) calloc(num_threads, sizeof(uint64_t));
        time_based_latencies.unlink = (uint64_t*) calloc(num_threads, sizeof(uint64_t));
    } else {
        op_based_latencies.create = (uint64_t*) calloc(mix_load * num_threads, sizeof(uint64_t));
        op_based_latencies.stat = (uint64_t*) calloc(mix_load * num_threads, sizeof(uint64_t));
        op_based_latencies.unlink = (uint64_t*) calloc(mix_load * num_threads, sizeof(uint64_t));
    }

    for (int thread = 0; thread < num_threads; ++thread) {
        load[thread].thread_id = thread;
        load[thread].root_path = path;
        load[thread].mix_load = mix_load;
        load[thread].offset = (thread * mix_load);
    }

    pthread_t* requesters = (pthread_t*) malloc (num_threads * sizeof (pthread_t));
    for (int thread = 0; thread < num_threads; ++thread) {
        pthread_create(&requesters[thread], NULL, thread_init, (void *) &load[thread]);
    }

    for (int thread = 0; thread < num_threads; ++thread) {
        pthread_join(requesters[thread], NULL);
    }

    if (time_based) {
        print_latencies(time_based_latencies, num_threads);
    } else {
        print_latencies(op_based_latencies, mix_load * num_threads);
    }

    return EXIT_SUCCESS;
}