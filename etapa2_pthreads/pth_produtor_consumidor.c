/**
 * Etapa 2 - Produtor/Consumidor com Pthreads
 *
 * 3 produtoras geram relogios vetoriais e 3 consumidoras os retiram
 * de uma fila circular controlada por mutex e variaveis de condicao.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define VECTOR_SIZE 3
#define PRODUCER_COUNT 3
#define CONSUMER_COUNT 3
#define BUFFER_SIZE 5
#define CLOCKS_PER_PRODUCER 6

typedef struct Clock {
    int p[VECTOR_SIZE];
} Clock;

typedef struct QueueItem {
    Clock clock;
    int producer_id;
    int sequence;
} QueueItem;

typedef struct Queue {
    QueueItem items[BUFFER_SIZE];
    int head;
    int tail;
    int count;
} Queue;

typedef struct Scenario {
    unsigned int producer_delay;
    unsigned int consumer_delay;
    const char *name;
} Scenario;

Queue queue_data = {.head = 0, .tail = 0, .count = 0};
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;

int producers_finished = 0;
Scenario scenario;

static void print_clock(const Clock *clock) {
    printf("(%d,%d,%d)", clock->p[0], clock->p[1], clock->p[2]);
}

static void enqueue_item(const QueueItem *item, long producer_id) {
    pthread_mutex_lock(&queue_mutex);

    while (queue_data.count == BUFFER_SIZE) {
        printf("[Produtor %ld] FILA CHEIA (%d/%d). Aguardando consumidor...\n",
               producer_id, queue_data.count, BUFFER_SIZE);
        fflush(stdout);
        pthread_cond_wait(&not_full, &queue_mutex);
    }

    queue_data.items[queue_data.tail] = *item;
    queue_data.tail = (queue_data.tail + 1) % BUFFER_SIZE;
    queue_data.count++;

    printf("[Produtor %ld] produziu ", producer_id);
    print_clock(&item->clock);
    printf(" | fila: %d/%d\n", queue_data.count, BUFFER_SIZE);
    fflush(stdout);

    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&queue_mutex);
}

static int dequeue_item(QueueItem *item, long consumer_id) {
    pthread_mutex_lock(&queue_mutex);

    while (queue_data.count == 0 && producers_finished < PRODUCER_COUNT) {
        printf("[Consumidor %ld] FILA VAZIA. Aguardando produtor...\n",
               consumer_id);
        fflush(stdout);
        pthread_cond_wait(&not_empty, &queue_mutex);
    }

    if (queue_data.count == 0 && producers_finished == PRODUCER_COUNT) {
        pthread_mutex_unlock(&queue_mutex);
        return 0;
    }

    *item = queue_data.items[queue_data.head];
    queue_data.head = (queue_data.head + 1) % BUFFER_SIZE;
    queue_data.count--;

    pthread_cond_signal(&not_full);
    pthread_mutex_unlock(&queue_mutex);
    return 1;
}

static void *producer(void *arg) {
    long producer_id = (long)arg;
    Clock clock = {{0, 0, 0}};

    for (int i = 1; i <= CLOCKS_PER_PRODUCER; i++) {
        QueueItem item;

        clock.p[producer_id]++;
        item.clock = clock;
        item.producer_id = (int)producer_id;
        item.sequence = i;

        enqueue_item(&item, producer_id);
        sleep(scenario.producer_delay);
    }

    pthread_mutex_lock(&queue_mutex);
    producers_finished++;
    printf("[Produtor %ld] finalizado. Produtores finalizados: %d/%d\n",
           producer_id, producers_finished, PRODUCER_COUNT);
    fflush(stdout);

    if (producers_finished == PRODUCER_COUNT) {
        pthread_cond_broadcast(&not_empty);
    }

    pthread_mutex_unlock(&queue_mutex);
    return NULL;
}

static void *consumer(void *arg) {
    long consumer_id = (long)arg;
    QueueItem item;

    while (dequeue_item(&item, consumer_id)) {
        printf("[Consumidor %ld] consumiu ", consumer_id);
        print_clock(&item.clock);
        printf(" <- Produtor %d, item %d\n", item.producer_id, item.sequence);
        fflush(stdout);
        sleep(scenario.consumer_delay);
    }

    printf("[Consumidor %ld] finalizado: nao ha mais relogios na fila.\n",
           consumer_id);
    fflush(stdout);
    return NULL;
}

static int configure_scenario(const char *arg) {
    if (strcmp(arg, "cheia") == 0) {
        scenario.name = "Fila tende a ficar CHEIA";
        scenario.producer_delay = 1;
        scenario.consumer_delay = 2;
        return 1;
    }

    if (strcmp(arg, "vazia") == 0) {
        scenario.name = "Fila tende a ficar VAZIA";
        scenario.producer_delay = 2;
        scenario.consumer_delay = 1;
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    pthread_t producers[PRODUCER_COUNT];
    pthread_t consumers[CONSUMER_COUNT];

    if (argc != 2 || !configure_scenario(argv[1])) {
        fprintf(stderr,
                "Uso: %s <cheia|vazia>\n\n"
                "  cheia : produtores a cada 1s; consumidores a cada 2s\n"
                "  vazia : produtores a cada 2s; consumidores a cada 1s\n",
                argv[0]);
        return 1;
    }

    printf("=============================================\n");
    printf("Etapa 2 - Produtor/Consumidor com Pthreads\n");
    printf("Cenario: %s\n", scenario.name);
    printf("Produtores: %d | Consumidores: %d\n", PRODUCER_COUNT, CONSUMER_COUNT);
    printf("Capacidade da fila: %d\n", BUFFER_SIZE);
    printf("Relogios por produtor: %d\n", CLOCKS_PER_PRODUCER);
    printf("Intervalo produtor: %us | consumidor: %us\n",
           scenario.producer_delay, scenario.consumer_delay);
    printf("=============================================\n\n");

    for (long i = 0; i < CONSUMER_COUNT; i++) {
        if (pthread_create(&consumers[i], NULL, consumer, (void *)i) != 0) {
            perror("Erro ao criar thread consumidora");
            return 1;
        }
    }

    for (long i = 0; i < PRODUCER_COUNT; i++) {
        if (pthread_create(&producers[i], NULL, producer, (void *)i) != 0) {
            perror("Erro ao criar thread produtora");
            return 1;
        }
    }

    for (int i = 0; i < PRODUCER_COUNT; i++) {
        pthread_join(producers[i], NULL);
    }

    for (int i = 0; i < CONSUMER_COUNT; i++) {
        pthread_join(consumers[i], NULL);
    }

    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&not_empty);
    pthread_cond_destroy(&not_full);

    printf("\nTodos os relogios foram produzidos e consumidos.\n");
    printf("Programa finalizado corretamente.\n");
    return 0;
}
