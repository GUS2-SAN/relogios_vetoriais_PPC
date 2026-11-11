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

// main: inicia o MPI, monta as filas, cria as 3 threads e espera tudo terminar
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
