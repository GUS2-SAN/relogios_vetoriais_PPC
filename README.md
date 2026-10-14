# Projeto de Programação Paralela — Relógios Vetoriais

Projeto desenvolvido em C para a disciplina PPC, dividido em duas etapas.

## Integrantes

- Augusto Henrique da Silva Santana
- Carlos Henrico Fontes Cabral

## Estrutura

```text
relogios_vetoriais/
├── README.md
├── Makefile
├── .gitignore
├── etapa1_mpi/
│   └── rvet.c
└── etapa2_pthreads/
    └── pth_produtor_consumidor.c
```

## Etapa 1 — Relógios Vetoriais com MPI

Implementa o exemplo de relógios vetoriais com três processos MPI (`P0`, `P1` e `P2`).

### Compilar

```bash
cd etapa1_mpi
mpicc -Wall -Wextra -o rvet rvet.c
```

### Executar

```bash
mpiexec -n 3 ./rvet
```

Estados finais esperados:

- `P0`: `(7,1,2)`
- `P1`: `(6,3,2)`
- `P2`: `(4,1,3)`

A ordem das linhas pode variar devido à execução concorrente.

## Etapa 2 — Produtor/Consumidor com Pthreads

Implementa uma fila compartilhada entre:

- 3 threads produtoras;
- 3 threads consumidoras.

A sincronização é feita com:

- `pthread_mutex_t`;
- variável de condição `not_empty`;
- variável de condição `not_full`.

Cada produtora gera relógios vetoriais e os coloca na fila. As consumidoras retiram os relógios e os imprimem.

### Compilar

```bash
cd etapa2_pthreads
gcc -Wall -Wextra -pthread -o pth_produtor_consumidor pth_produtor_consumidor.c
```

### Cenário 1 — fila cheia

Produtores produzem a cada 1 segundo e consumidores consomem a cada 2 segundos:

```bash
./pth_produtor_consumidor cheia
```

Durante o teste devem aparecer mensagens de fila cheia e produtores aguardando.

### Cenário 2 — fila vazia

Produtores produzem a cada 2 segundos e consumidores consomem a cada 1 segundo:

```bash
./pth_produtor_consumidor vazia
```

Durante o teste devem aparecer mensagens de fila vazia e consumidores aguardando.

## Makefile

Na raiz do projeto:

```bash
make
```

Executar etapa 1:

```bash
make run-etapa1
```

Executar cenário de fila cheia:

```bash
make run-cheia
```

Executar cenário de fila vazia:

```bash
make run-vazia
```

Limpar executáveis:

```bash
make clean
```
