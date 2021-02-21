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
#define NSEC 1000000000ULL

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

/*
 Gets the mean of an array

 Params:
  - numbers: Array of numbers
  - size: Array size

 Returns: The mean of the array elements
*/
uint64_t mean(uint64_t * numbers, size_t size) {
    uint64_t sum = 0;
    for (size_t i = 0; i < size; i++) {
        sum += numbers[i];
    }
    return sum/size;
}

/*
 Gets the current timestamp in nanoseconds

 Params: none

 Errors: It fails and exits the program if it's not possible to get the timestamp 
 Returns: The current timestamp in nanoseconds
*/
static uint64_t stamp (void) {
   struct timespec tspec;
   if (clock_gettime (CLOCK_MONOTONIC, &tspec)) {
       perror("Error getting timestamp");
       exit(EXIT_FAILURE);
   }
   return (tspec.tv_sec * NSEC) + tspec.tv_nsec;
}

/*
 Issues a mix of three operations (create, stat, unlink)

 Params:
  - root_path: Path where the operations will occur
  - filename: identification of the file in which the operations will occur

 Errors: It fails and exits the program if one of the operations can't be made
 Returns: Array containing each latency measured [create, stat, unlink]
*/
uint64_t * issue_mix(char *root_path, char * filename) {
    uint64_t begin, end;
    uint64_t * latencies = (uint64_t*) calloc(3, sizeof(uint64_t));
    
    struct stat st;

    char* dst_path = (char*) calloc(256, sizeof(char));

    sprintf(dst_path, "%s/%s", root_path, filename);

    begin = stamp();

    if (0 != mknod(dst_path, S_IFREG | ACCESS_PERMISSION, 0)) {
        fprintf(stderr, "Couldn't mknod() to %s\n", dst_path);
        exit(EXIT_FAILURE);
    } else {
        end = stamp();
        latencies[CREATE] = (end - begin);
    }

    begin = stamp();

    if (stat(dst_path, &st) != 0) {
        fprintf(stderr, "Couldn't stat() to %s\n", dst_path);
        exit(EXIT_FAILURE);
    } else {
        end = stamp();
        latencies[STAT] = (end - begin);
    }

    begin = stamp();

    if (unlink(dst_path) != 0) {
        fprintf(stderr, "Couldn't unlink() to %s\n", dst_path);
        exit(EXIT_FAILURE);
    } else {
        end = stamp();
        latencies[UNLINK] = (end - begin);
    }
    
    return latencies;
}

/*
 Issues a number of mixes (create, stat, unlink)
 
 Params:
  - root_path: Path where the operations will occur
  - num_mixes: Number of mixes to be issued
  - offset: It determines where the thread should write in the array of latencies
  - thread_id: Thread identification

 Errors: It fails and exits the program if one of the operations can't be made
 Returns: none
*/
void issue_operation_based_mixes(char *root_path, int num_mixes, int offset, int thread_id) {
    int mix;
    char * filename = (char*) calloc(256, sizeof(char));
    uint64_t * latencies;

    for (mix = 0; mix < num_mixes; ++mix) {
        snprintf(filename, sizeof filename, "mix-%d-%d", thread_id, mix);

        latencies = issue_mix(root_path, filename);
        op_based_latencies.create[mix+offset] = latencies[CREATE];
        op_based_latencies.stat[mix+offset] = latencies[STAT];
        op_based_latencies.unlink[mix+offset] = latencies[UNLINK];
    }
}

/*
 Issues mixes (create, stat, unlink) during a predefined time
 
 Params:
  - root_path: Path where the operation will occur
  - user_defined_runtime: Time in which the mixes will be issued
  - thread_id: Thread identification

 Errors: none
 Returns: none
*/
void issue_time_based_mixes(char *root_path, uint64_t user_defined_runtime, int thread_id) {
    uint64_t curr_runtime = 0;
    uint64_t user_defined_runtime_ns = user_defined_runtime * NSEC;

    uint64_t creates = 0, create_latencies = 0;
    uint64_t stats = 0, stat_latencies = 0;
    uint64_t unlinks = 0, unlink_latencies = 0;

    char * filename = (char*) calloc(256, sizeof(char));
    uint64_t * latencies;

    while(curr_runtime < user_defined_runtime_ns) {
        snprintf(filename, sizeof filename, "mix-%d-%ld", thread_id, creates);

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

/*
 Calls the job for the thread

 Params:
  - args: struct of thread load

 Errors: none
 Returns: NULL
*/
static void* thread_init(void* args) {
    thread_load* load = (thread_load*) args;

    if (time_based) {
        issue_time_based_mixes(load->root_path, load->mix_load, load->thread_id);
    } else {
        issue_operation_based_mixes(load->root_path, load->mix_load, load->offset, load->thread_id);
    }

    return NULL;
}

/*
 Prints the latencies collected in nanoseconds.

 The output depends on the latency and time flags being used:
  - Time based execution (FLAG=time-based):
     - Full latency (FLAG=full-lat): average latencies for each thread (create, stat, unlink)
     - Resumed latency (FLAG=res-lat): one-line with the average latencies of all threads (create, stat, unlink)
  - Operation based execution (FLAG=no-time):
     - Full latency (FLAG=full-lat): all latencies of all thread (create, stat, unlink)
     - Resumed latency (FLAG=res-lat): one-line with the average latencies of all threads (create, stat, unlink)

 Params:
  - latencies_out: struct of latencies to be printed
  - size: number of latencies per array in the struct

 Errors: none
 Returns: none
*/
void print_latencies(struct latencies latencies_out, int size) {
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

/*
 Evaluates whether the input is one of the two options given in the params
 
 Params:
  - input: user inputed value
  - first_op: First option to the value
  - second_op: Second option to the value

 Error: It fails and exits the program if the input doesn't corresponds to any option 
 Returns: 1 in case the input it is equal to the first option
          0 otherwise 
*/
int parse_bool_flag(char * input, char * first_op, char * second_op) {
    if (strcmp(input, first_op) == 0) {
        return 1;
    } else if (strcmp(input, second_op) == 0) {
        return 0;
    } else {
        fprintf(stderr, "Invalid parameter %s, must be one of: %s or %s.\n", input, first_op, second_op);
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char* argv[]) {
    if (argc < 6) {
        fprintf(stderr, "Usage: ./mix_metadata <path> <load_per_thread> <num_threads> full-lat|res-lat time-based|no-time\n");
        exit(EXIT_FAILURE);
    }

    // Path in which the operations will take place
    char* path = argv[1];
    // Number of seconds (time-based) or number of mixes (no-time)
    int mix_load = atoi(argv[2]);
    // Number of threads being used
    int num_threads = atoi(argv[3]);
    // Whether the latency will be detailed or not
    detailed_latency = parse_bool_flag(argv[4], "full-lat", "res-lat");
    // Whether the operations will take place in a time defined by the user
    time_based = parse_bool_flag(argv[5], "time-based", "no-time");

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