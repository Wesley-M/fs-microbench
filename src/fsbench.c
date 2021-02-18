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

static uint64_t stamp (void) {
   struct timespec tspec;
   if (clock_gettime (CLOCK_MONOTONIC, &tspec)) {
       fprintf (stderr, "Error getting timestamp: %s\n", strerror(errno));
       exit (EXIT_FAILURE);
   }
   return (tspec.tv_sec * 1000000000ULL) + tspec.tv_nsec;
}

void create_directories(char *path, int num_directories) {
    char pathbuf[256];

    for (int i = 0; i < num_directories; i++) {
        // Clean the string
        memset(pathbuf, 0, sizeof pathbuf);

        // build dict name
        snprintf(pathbuf, sizeof pathbuf, "%s/%s%d", path, "bench_dict", i);

        if(mkdir(pathbuf, ACCESS_PERMISSION)){
            fprintf(stderr, "Can't make directory bench_dict%d\n", i);
        }
    }
}

void create_file() {
    // To do
}

int main (int argc, char* argv[]) {

    char* path = argv[1];
    int num_directories = atoi (argv[2]);

    create_directories(path, num_directories);

    return 0;
}

