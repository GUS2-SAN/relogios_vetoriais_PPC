CC = gcc
MPICC = mpicc

PTHREAD_BIN = etapa2_pthreads/pth_produtor_consumidor
MPI_BIN = etapa1_mpi/rvet

.PHONY: all etapa1 etapa2 run-etapa1 run-cheia run-vazia clean

all: etapa1 etapa2

etapa1:
	$(MPICC) -Wall -Wextra -o $(MPI_BIN) etapa1_mpi/rvet.c

etapa2:
	$(CC) -Wall -Wextra -pthread -o $(PTHREAD_BIN) etapa2_pthreads/pth_produtor_consumidor.c

run-etapa1: etapa1
	mpiexec -n 3 ./$(MPI_BIN)

run-cheia: etapa2
	./$(PTHREAD_BIN) cheia

run-vazia: etapa2
	./$(PTHREAD_BIN) vazia

clean:
	rm -f $(MPI_BIN) $(PTHREAD_BIN)
