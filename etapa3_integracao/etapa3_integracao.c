/**
  Etapa 3 - Relogios Vetoriais + Produtor/Consumidor
 
    Cada processo MPI tem 3 threads:
    central   -> central_thread():  executa EVENT, SEND e RECEIVE
    receptora -> receiver_thread(): MPI_Recv -> produz na fila de recepcao
    emissora  -> sender_thread():   consome a fila de envio -> MPI_Send
 
    Filas (receive_queue e send_queue): independentes, cada uma com
    1 mutex + 2 variaveis de condicao (not_empty e not_full).
 
    Compilar:  mpicc -pthread -o integracao etapa3_integracao.c
    Executar:  mpiexec -n 3 ./integracao
    Forcar fila cheia (teste): adicionar -DBUFFER_SIZE=1 na compilacao
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <mpi.h>

#define PROCESS_COUNT 3
#define VECTOR_SIZE PROCESS_COUNT
#define CLOCK_TAG 0

#ifndef BUFFER_SIZE
#define BUFFER_SIZE 5
#endif

/* Nomes usados nas mensagens de log */
#define ROLE_CENTRAL   "Central"
#define ROLE_RECEIVER  "Receptora"
#define ROLE_SENDER    "Emissora"



/* Tipos*/
typedef struct Clock {
    int p[VECTOR_SIZE];
} Clock;

typedef struct QueueItem {
    Clock clock;
    int peer;               /* destino (fila de envio) ou origem (fila de recepcao) */
} QueueItem;

typedef struct Queue {
    QueueItem items[BUFFER_SIZE];
    int head;
    int tail;
    int count;
    int rank;               /* dono da fila (so para o log) */
    const char *name;       /* "RECEPCAO" ou "ENVIO" (so para o log) */
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} Queue;

typedef struct ProcessContext {
    int rank;
    Clock local_clock;      /* acessado somente pela thread central */
    Queue receive_queue;
    Queue send_queue;
    int expected_receives;  /* quantas mensagens a receptora deve receber */
    int expected_sends;     /* quantas mensagens a emissora deve enviar */
} ProcessContext;

/* Mensagens por processo, conforme o diagrama */
static const int EXPECTED_RECEIVES[PROCESS_COUNT] = {2, 2, 1};
static const int EXPECTED_SENDS[PROCESS_COUNT]    = {3, 1, 1};


/* Impressao*/

/* Escreve o vetor em buf no formato (a,b,c) */
static void format_clock(char *buf, size_t size, const Clock *clock) {
    size_t len = (size_t)snprintf(buf, size, "(");

    for (int i = 0; i < VECTOR_SIZE; i++) {
        len += (size_t)snprintf(buf + len, size - len, "%s%d",
                                i > 0 ? "," : "", clock->p[i]);
    }
    snprintf(buf + len, size - len, ")");
}

static void print_clock(int rank, const char *action, const Clock *clock) {
    char text[64];

    format_clock(text, sizeof(text), clock);
    printf("[P%d] %-28s Clock = %s\n", rank, action, text);
    fflush(stdout);
}


/* Fila produtor/consumidor*/
static void queue_init(Queue *queue, int rank, const char *name) {
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->rank = rank;
    queue->name = name;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    pthread_cond_init(&queue->not_full, NULL);
}

static void queue_destroy(Queue *queue) {
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->not_empty);
    pthread_cond_destroy(&queue->not_full);
}

/* Produtor: bloqueia enquanto a fila estiver cheia. 'actor' so aparece no log. */
static void queue_push(Queue *queue, const QueueItem *item, const char *actor) {
    pthread_mutex_lock(&queue->mutex);

    while (queue->count == BUFFER_SIZE) {
        printf("[P%d][%s] FILA DE %s CHEIA (%d/%d). Aguardando consumidor...\n",
               queue->rank, actor, queue->name, queue->count, BUFFER_SIZE);
        fflush(stdout);
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }

    queue->items[queue->tail] = *item;
    queue->tail = (queue->tail + 1) % BUFFER_SIZE;
    queue->count++;

    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
}

/* Consumidor: bloqueia enquanto a fila estiver vazia. */
static void queue_pop(Queue *queue, QueueItem *item, const char *actor) {
    pthread_mutex_lock(&queue->mutex);

    while (queue->count == 0) {
        printf("[P%d][%s] FILA DE %s VAZIA. Aguardando produtor...\n",
               queue->rank, actor, queue->name);
        fflush(stdout);
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }

    *item = queue->items[queue->head];
    queue->head = (queue->head + 1) % BUFFER_SIZE;
    queue->count--;

    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
}


/* Operacoes do relogio vetorial (chamadas na thread central)*/
static void Event(ProcessContext *ctx) {
    ctx->local_clock.p[ctx->rank]++;
    print_clock(ctx->rank, "EVENT", &ctx->local_clock);
}

/* Incrementa o relogio e poe a copia na fila de envio (sem MPI_Send). */
static void Send(ProcessContext *ctx, int destination) {
    QueueItem item;
    char action[80];

    ctx->local_clock.p[ctx->rank]++;

    item.clock = ctx->local_clock;
    item.peer = destination;
    queue_push(&ctx->send_queue, &item, ROLE_CENTRAL);

    snprintf(action, sizeof(action), "SEND(%d) -> fila de envio", destination);
    print_clock(ctx->rank, action, &ctx->local_clock);
}

/* Retira o proximo relogio da fila de recepcao (sem MPI_Recv, origem livre). */
static void Receive(ProcessContext *ctx) {
    QueueItem item;
    char action[80];

    queue_pop(&ctx->receive_queue, &item, ROLE_CENTRAL);

    ctx->local_clock.p[ctx->rank]++;
    for (int i = 0; i < VECTOR_SIZE; i++) {
        if (item.clock.p[i] > ctx->local_clock.p[i]) {
            ctx->local_clock.p[i] = item.clock.p[i];
        }
    }

    snprintf(action, sizeof(action), "RECEIVE <- P%d", item.peer);
    print_clock(ctx->rank, action, &ctx->local_clock);
}



/* Threads*/

/* Receptora: MPI_Recv de qualquer origem -> produz na fila de recepcao. */
static void *receiver_thread(void *arg) {
    ProcessContext *ctx = (ProcessContext *)arg;

    for (int i = 0; i < ctx->expected_receives; i++) {
        QueueItem item;
        MPI_Status status;
        char text[64];

        MPI_Recv(item.clock.p, VECTOR_SIZE, MPI_INT, MPI_ANY_SOURCE,
                 CLOCK_TAG, MPI_COMM_WORLD, &status);
        item.peer = status.MPI_SOURCE;

        format_clock(text, sizeof(text), &item.clock);
        printf("[P%d][%s] recebeu de P%d: %s\n",
               ctx->rank, ROLE_RECEIVER, item.peer, text);
        fflush(stdout);

        queue_push(&ctx->receive_queue, &item, ROLE_RECEIVER);
    }

    return NULL;
}

/* Emissora: consome a fila de envio -> MPI_Send para o destino. */
static void *sender_thread(void *arg) {
    ProcessContext *ctx = (ProcessContext *)arg;

    for (int i = 0; i < ctx->expected_sends; i++) {
        QueueItem item;
        char text[64];

        queue_pop(&ctx->send_queue, &item, ROLE_SENDER);

        MPI_Send(item.clock.p, VECTOR_SIZE, MPI_INT, item.peer,
                 CLOCK_TAG, MPI_COMM_WORLD);

        format_clock(text, sizeof(text), &item.clock);
        printf("[P%d][%s] enviou para P%d: %s\n",
               ctx->rank, ROLE_SENDER, item.peer, text);
        fflush(stdout);
    }

    return NULL;
}

/* Central: executa o fluxo de cada processo conforme o diagrama. */
static void *central_thread(void *arg) {
    ProcessContext *ctx = (ProcessContext *)arg;

    print_clock(ctx->rank, "Estado inicial", &ctx->local_clock);

    switch (ctx->rank) {
        case 0:
            Event(ctx);
            Send(ctx, 1);
            Receive(ctx);
            Send(ctx, 2);
            Receive(ctx);
            Send(ctx, 1);
            Event(ctx);
            break;

        case 1:
            Send(ctx, 0);
            Receive(ctx);
            Receive(ctx);
            break;

        case 2:
            Event(ctx);
            Send(ctx, 0);
            Receive(ctx);
            break;

        default:
            break;
    }

    print_clock(ctx->rank, "ESTADO FINAL", &ctx->local_clock);
    return NULL;
}


/*Main*/
int main(int argc, char **argv) {
    int rank;
    int comm_size;
    int provided;

    /* Receptora e emissora chamam MPI ao mesmo tempo: exige THREAD_MULTIPLE. */
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    if (comm_size != PROCESS_COUNT) {
        if (rank == 0) {
            fprintf(stderr,
                    "Erro: execute com exatamente %d processos.\n"
                    "Use: mpiexec -n %d ./integracao\n",
                    PROCESS_COUNT, PROCESS_COUNT);
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    if (provided < MPI_THREAD_MULTIPLE) {
        if (rank == 0) {
            fprintf(stderr,
                    "Erro: a implementacao MPI nao fornece "
                    "MPI_THREAD_MULTIPLE.\n");
        }
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        return EXIT_FAILURE;
    }

    ProcessContext ctx = {
        .rank = rank,
        .local_clock = {{0, 0, 0}},
        .expected_receives = EXPECTED_RECEIVES[rank],
        .expected_sends = EXPECTED_SENDS[rank]
    };

    queue_init(&ctx.receive_queue, rank, "RECEPCAO");
    queue_init(&ctx.send_queue, rank, "ENVIO");

    pthread_t receiver;
    pthread_t central;
    pthread_t sender;

    if (pthread_create(&receiver, NULL, receiver_thread, &ctx) != 0 ||
        pthread_create(&sender, NULL, sender_thread, &ctx) != 0 ||
        pthread_create(&central, NULL, central_thread, &ctx) != 0) {
        fprintf(stderr, "[P%d] Erro ao criar threads.\n", rank);
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        return EXIT_FAILURE;
    }

    pthread_join(central, NULL);
    pthread_join(sender, NULL);
    pthread_join(receiver, NULL);

    queue_destroy(&ctx.receive_queue);
    queue_destroy(&ctx.send_queue);

    MPI_Finalize();
    return EXIT_SUCCESS;
}
