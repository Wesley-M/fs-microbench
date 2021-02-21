CC=gcc
CCFLAGS += -g -Wall -Wextra

# created to the list
MAIN = rr rw seqr seqw background stat mix_metadata
all : $(MAIN)

rr.o : rr.c
	$(CC) $(CCFLAGS) -c rr.c

rr : rr.o
	$(CC) $(CCFLAGS) $^ -o $@ -pthread -lrt

rw.o : rw.c
	$(CC) $(CCFLAGS) -c rw.c

rw : rw.o
	$(CC) $(CCFLAGS) $^ -o $@ -pthread -lrt

seqr.o : seqr.c
	$(CC) $(CCFLAGS) -c seqr.c

seqr : seqr.o
	$(CC) $(CCFLAGS) $^ -o $@ -pthread -lrt -lm

seqw.o : seqw.c
	$(CC) $(CCFLAGS) -c seqw.c

seqw : seqw.o
	$(CC) $(CCFLAGS) $^ -o $@ -pthread -lrt -lm

background.o : background.c
	$(CC) $(CCFLAGS) -c background.c

background : background.o
	$(CC) $(CCFLAGS) $^ -o $@

stat.o : stat.c
	$(CC) $(CCFLAGS) -c stat.c

stat : stat.o
	$(CC) $(CCFLAGS) $^ -o $@ -pthread -lrt -lm

mix_metadata : mix_metadata.o
	$(CC) $(CCFLAGS) $^ -o $@ -pthread -lrt -lm

mix_metadata.o : mix_metadata.c
	$(CC) $(CCFLAGS) -c mix_metadata.c

clean :
	rm -rf $(MAIN) *.o
