// etapa 3 juntando relogios vetoriais com produtor/consumidor
// cada processo mpi vai ter 3 threads: uma central, uma que recebe e uma que envia
// tem duas filas, uma para o que chega e outra para o que vai ser enviado
// para compilar: mpicc -pthread -o integracao integracao.c
// para rodar: mpiexec -n 3 ./integracao


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

// nomes que aparecem no terminal para saber qual thread esta trabalhando
#define ROLE_CENTRAL   "Central"
#define ROLE_RECEIVER  "Receptora"
#define ROLE_SENDER    "Emissora"

typedef struct Clock {
    int p[VECTOR_SIZE];
} Clock;

typedef struct QueueItem {
    Clock clock;
    int peer;               // guarda para quem vai ou de quem veio
} QueueItem;

typedef struct Queue {
    QueueItem items[BUFFER_SIZE];
    int head;
    int tail;
    int count;
    int rank;               // processo dono dessa fila
    const char *name;       // nome da fila para aparecer no terminal
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} Queue;

typedef struct ProcessContext {
    int rank;
    Clock local_clock;      // relogio principal desse processo
    Queue receive_queue;
    Queue send_queue;
    int expected_receives;  // quantas mensagens esse processo vai receber
    int expected_sends;     // quantas mensagens esse processo vai enviar
} ProcessContext;

// quantidade de mensagens que cada processo recebe e envia no diagrama
static const int EXPECTED_RECEIVES[PROCESS_COUNT] = {2, 2, 1};
static const int EXPECTED_SENDS[PROCESS_COUNT]    = {3, 1, 1};

// transforma o relogio em texto tipo (1,0,2) para conseguir mostrar no printf
static void format_clock(char *buf, size_t size, const Clock *clock) {
    size_t len = (size_t)snprintf(buf, size, "(");

    for (int i = 0; i < VECTOR_SIZE; i++) {
        len += (size_t)snprintf(buf + len, size - len, "%s%d",
                                i > 0 ? "," : "", clock->p[i]);
    }
    snprintf(buf + len, size - len, ")");
}

// usa a funcao acima e mostra no terminal o processo, a acao e o relogio atual
static void print_clock(int rank, const char *action, const Clock *clock) {
    char text[64];

    format_clock(text, sizeof(text), clock);
    printf("[P%d] %-28s Clock = %s\n", rank, action, text);
    fflush(stdout);
}

// inicia uma fila vazia e tambem cria o mutex e as duas condicoes dela
// essa funcao e usada nas filas de recepcao e de envio
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

// no fim do programa libera o mutex e as condicoes que a fila usou
static void queue_destroy(Queue *queue) {
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->not_empty);
    pthread_cond_destroy(&queue->not_full);
}

// coloca um item na fila
// se a fila estiver cheia espera alguem consumir antes de continuar
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

// tira um item da fila
// se estiver vazia espera ate alguma outra thread colocar algo
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

//---- EVENTO 
// do relogio vetorial
// aumenta a posicao do proprio processo
static void Event(ProcessContext *ctx) {
    ctx->local_clock.p[ctx->rank]++;
    print_clock(ctx->rank, "EVENT", &ctx->local_clock);
}

//---- SEND 
// envia uma mensagem
// aumenta o proprio relogio e coloca uma copia na fila de envio
// o MPI_Send vai ser feito depois pela thread emissora
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


// quando a central precisa receber ela pega o proximo item da fila de recepcao
// depois aumenta seu proprio contador e compara os dois relogios pegando os maiores valores
// o MPI_Recv ja foi feito antes pela thread receptora
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


// ---- THREAD RECEPTORA 
// essa e a thread que fica esperando e recebendo mensagens dos outros processos
// ela usa MPI_Recv e depois coloca o que chegou na fila de recepcao
static void *receiver_thread(void *arg) {
    ProcessContext *ctx = (ProcessContext *)arg;

    for (int i = 0; i < ctx->expected_receives; i++) {
        QueueItem item;
        MPI_Status status;
        char text[64];
        //MPI_Recv recebe uma mensagem de qualquer processo
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


// ---- THREAD EMISSORA 
// essa thread fica esperando aparecer algo na fila de envio
// quando aparece ela tira da fila e manda de verdade usando MPI_Send
static void *sender_thread(void *arg) {
    ProcessContext *ctx = (ProcessContext *)arg;

    for (int i = 0; i < ctx->expected_sends; i++) {
        QueueItem item;
        char text[64];

        queue_pop(&ctx->send_queue, &item, ROLE_SENDER);

        //MPI_Send envia a mensagem para o processo destino
        MPI_Send(item.clock.p, VECTOR_SIZE, MPI_INT, item.peer,
                 CLOCK_TAG, MPI_COMM_WORLD);

        format_clock(text, sizeof(text), &item.clock);
        printf("[P%d][%s] enviou para P%d: %s\n",
               ctx->rank, ROLE_SENDER, item.peer, text);
        fflush(stdout);
    }

    return NULL;
}

// essa e a thread principal de cada processo
// aqui esta a ordem dos EVENT, SEND e RECEIVE que aparece no diagrama da atividade
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

// ---- MAIN
// inicia o MPI, monta as filas, cria as 3 threads e espera tudo terminar
int main(int argc, char **argv) {
    int rank;
    int comm_size;
    int provided;

    // como a thread receptora e a emissora podem usar MPI ao mesmo tempo
    // inicializa pedindo suporte para varias threads
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    // esse trabalho foi feito para rodar exatamente com P0, P1 e P2
    if (comm_size != PROCESS_COUNT) {
        if (rank == 0) {
            fprintf(stderr,
                    "Erro: execute com exatamente %d processos.\n"
                    "Use: mpiexec -n %d ./integracao\n",
                    PROCESS_COUNT, PROCESS_COUNT);
        }
        // encerra o MPI
    MPI_Finalize();
        return EXIT_FAILURE;
    }

    // se o MPI instalado nao aceitar varias threads usando MPI ao mesmo tempo, para aqui
    if (provided < MPI_THREAD_MULTIPLE) {
        if (rank == 0) {
            fprintf(stderr,
                    "Erro: a implementacao MPI nao fornece "
                    "MPI_THREAD_MULTIPLE.\n");
        }
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        return EXIT_FAILURE;
    }

    // aqui ficam os dados que pertencem a este processo
    ProcessContext ctx = {
        .rank = rank,
        .local_clock = {{0, 0, 0}},
        .expected_receives = EXPECTED_RECEIVES[rank],
        .expected_sends = EXPECTED_SENDS[rank]
    };

    // cria as duas filas separadas desse processo
    queue_init(&ctx.receive_queue, rank, "RECEPCAO");
    queue_init(&ctx.send_queue, rank, "ENVIO");

    // ids das 3 threads que vao existir em cada processo
    pthread_t receiver;
    pthread_t central;
    pthread_t sender;

    // CRIA AS THREADS
    // cria primeiro as threads que recebem, enviam e controlam o relogio
    if (pthread_create(&receiver, NULL, receiver_thread, &ctx) != 0 ||
        pthread_create(&sender, NULL, sender_thread, &ctx) != 0 ||
        pthread_create(&central, NULL, central_thread, &ctx) != 0) {
        fprintf(stderr, "[P%d] Erro ao criar threads.\n", rank);
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        return EXIT_FAILURE;
    }

    // espera as 3 threads terminarem antes de finalizar o processo MPI
    pthread_join(central, NULL);
    pthread_join(sender, NULL);
    pthread_join(receiver, NULL);

    // depois que terminou tudo, limpa as filas
    queue_destroy(&ctx.receive_queue);
    queue_destroy(&ctx.send_queue);

    // encerra o MPI
    MPI_Finalize();
    return EXIT_SUCCESS;
}
