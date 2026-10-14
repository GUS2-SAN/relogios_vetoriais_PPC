/**
 * Etapa 1 - Relogios Vetoriais com MPI
 */
#include <stdio.h>
#include <mpi.h>

#define PROCESS_COUNT 3
#define CLOCK_TAG 0

typedef struct Clock {
    int p[PROCESS_COUNT];
} Clock;

void PrintClock(const char *acao, const Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    printf("[P%d] %s -> Clock = (%d,%d,%d)\n",
           pid, acao, clock->p[0], clock->p[1], clock->p[2]);
    fflush(stdout);
}

void Event(Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    clock->p[pid]++;
    PrintClock("Evento interno", clock);
}

void Send(int dest, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    clock->p[pid]++;
    MPI_Send(clock->p, PROCESS_COUNT, MPI_INT, dest, CLOCK_TAG, MPI_COMM_WORLD);
    PrintClock("Envio de mensagem", clock);
}

void Receive(int src, Clock *clock) {
    int pid;
    int remote_clock[PROCESS_COUNT];
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    MPI_Recv(remote_clock, PROCESS_COUNT, MPI_INT, src, CLOCK_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    clock->p[pid]++;
    for (int i = 0; i < PROCESS_COUNT; i++) {
        if (remote_clock[i] > clock->p[i]) {
            clock->p[i] = remote_clock[i];
        }
    }
    PrintClock("Recebimento de mensagem", clock);
}

void process0(void) {
    Clock clock = {{0, 0, 0}};
    PrintClock("Estado inicial", &clock);
    Event(&clock);       /* (1,0,0) */
    Send(1, &clock);     /* (2,0,0) */
    Receive(1, &clock);  /* (3,1,0) */
    Send(2, &clock);     /* (4,1,0) */
    Receive(2, &clock);  /* (5,1,2) */
    Send(1, &clock);     /* (6,1,2) */
    Event(&clock);       /* (7,1,2) */
}

void process1(void) {
    Clock clock = {{0, 0, 0}};
    PrintClock("Estado inicial", &clock);
    Send(0, &clock);     /* (0,1,0) */
    Receive(0, &clock);  /* (2,2,0) */
    Receive(0, &clock);  /* (6,3,2) */
}

void process2(void) {
    Clock clock = {{0, 0, 0}};
    PrintClock("Estado inicial", &clock);
    Event(&clock);       /* (0,0,1) */
    Send(0, &clock);     /* (0,0,2) */
    Receive(0, &clock);  /* (4,1,3) */
}

int main(void) {
    int my_rank, comm_size;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    if (comm_size != PROCESS_COUNT) {
        if (my_rank == 0) {
            fprintf(stderr,
                    "Erro: execute com exatamente %d processos.\n"
                    "Use: mpiexec -n %d ./rvet\n",
                    PROCESS_COUNT, PROCESS_COUNT);
        }
        MPI_Finalize();
        return 1;
    }

    if (my_rank == 0) process0();
    else if (my_rank == 1) process1();
    else if (my_rank == 2) process2();

    MPI_Finalize();
    return 0;
}
