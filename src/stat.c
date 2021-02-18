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
#define NUM_FILES 5

long random_offset (long file_length, int blksize) {
    return (long) ((rand() / (double) RAND_MAX) * (file_length - blksize));
}

static uint64_t stamp (void) {
   struct timespec tspec;
   if (clock_gettime (CLOCK_MONOTONIC, &tspec)) {
       fprintf (stderr, "Error getting timestamp: %s\n", strerror(errno));
       exit (EXIT_FAILURE);
   }
   return (tspec.tv_sec * 1000000000ULL) + tspec.tv_nsec;
}

// To run, make: ./a.out ./test <num_ops>

void issue_stats(char *path, int num_ops) {
    
    int i, file_id = 0;
    char pathbuf[256];

    struct stat st;

    srand(time(NULL));

    for (i = 0; i < num_ops; i++) {
        file_id = rand() % (NUM_FILES + 1);
        
        memset(pathbuf, 0, sizeof pathbuf);
        snprintf (pathbuf, sizeof pathbuf, "%s/%d", path, file_id);

        printf("Making stat to %d \n", file_id);

        if (stat (pathbuf, &st) != 0) {
            printf("Can't make stat to %d", file_id);
        }
    }
}

int main (int argc, char* argv[]) {

    char* path = argv[1];
    int num_ops = atoi (argv[2]);

    issue_stats(path, num_ops);

    return 0;
}

