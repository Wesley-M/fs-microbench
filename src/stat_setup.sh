#!/bin/bash

# Script to create X files and Y files inside them

# Path where the files will be created
PATH_NAME=.

# Number of directories which will be created
NUM_DIRECTORIES=1

# Number of files which will be created
NUM_FILES_PER_DIRECTORY=100

for ((i=0; i<$NUM_DIRECTORIES; i++))
do
  mkdir "$PATH_NAME/bench-dict-$i"
  for ((j=0; j<$NUM_FILES_PER_DIRECTORY; j++))
  do
    touch "$PATH_NAME/bench-dict-$i/$j"
  done
done