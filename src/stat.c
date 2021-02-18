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

#define NUM_FILES 100

uint64_t mean(uint64_t * numbers, size_t size) {
    uint64_t sum = 0;
    for (int i = 0; i < size; i++) {
        sum += numbers[i];
    }
    return sum/size;
}

static uint64_t stamp (void) {
   struct timespec tspec;
   if (clock_gettime (CLOCK_MONOTONIC, &tspec)) {
       fprintf (stderr, "Error getting timestamp: %s\n", strerror(errno));
       exit (EXIT_FAILURE);
   }
   return (tspec.tv_sec * 1000000000ULL) + tspec.tv_nsec;
}

void issue_stats(uint64_t * stat_latencies, char *path, int num_ops) {
    
    int i, file_id = 0;
    uint64_t begin, end;
    char pathbuf[256];

    struct stat st;

    srand(time(NULL));

    for (i = 0; i < num_ops; i++) {
        file_id = rand() % NUM_FILES;
        
        memset(pathbuf, 0, sizeof pathbuf);
        snprintf (pathbuf, sizeof pathbuf, "%s/%d", path, file_id);

        begin = stamp();

        if (stat (pathbuf, &st) != 0) {
            printf("Can't make stat to %d", file_id);
        }

        end = stamp();

        // Saving latency
        stat_latencies[i] = (end - begin);
    }
}

// Print latency(ies) in nanoseconds
void print_latencies(int num_ops, uint64_t * stat_latencies, int detailed_latency) {
    if (detailed_latency) {
        for (int i = 0; i < num_ops; i++) {
            printf("%ld\n", stat_latencies[i]);
        }
    } else {
        printf("%ld\n", mean(stat_latencies, num_ops));
    }
}

// To run, type: ./a.out ./test <num_ops> full-lat|res-lat

int main (int argc, char* argv[]) {

    int detailed_latency;
    char* path = argv[1];
    int num_ops = atoi (argv[2]);

    if (strcmp (argv[3], "full-lat") == 0) {
        detailed_latency = 1;
    } else if (strcmp (argv[3], "res-lat") == 0) {
        detailed_latency = 0;
    }

    uint64_t * stat_latencies = (uint64_t*) calloc (num_ops, sizeof(uint64_t));

    issue_stats(stat_latencies, path, num_ops);

    print_latencies(num_ops, stat_latencies, detailed_latency);

    return 0;
}

