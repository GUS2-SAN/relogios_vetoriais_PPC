# Projeto Programação Paralela e Concorrente — Relógios Vetoriais

Projeto desenvolvido em C para a disciplina de Programação Paralela e Concorrente (PPC).

## Integrante

- Augusto Henrique da Silva Santana

## Estrutura

```text
relogios_vetoriais_PPC/
├── README.md
├── Makefile
├── .gitignore
├── etapa1_mpi/
│   └── rvet.c
├── etapa2_pthreads/
│   └── pth_produtor_consumidor.c
└── etapa3_integracao/
    └── integracao.c
```

## Etapa 1 — Relógios Vetoriais com MPI

Implementa relógios vetoriais utilizando três processos MPI (`P0`, `P1` e `P2`).

Cada processo mantém seu próprio relógio vetorial e realiza eventos internos, envio e recebimento de mensagens.

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

A ordem das mensagens exibidas no terminal pode variar devido à execução concorrente.

---

## Etapa 2 — Produtor/Consumidor com Pthreads

Implementa o modelo Produtor/Consumidor utilizando:

- 3 threads produtoras;
- 3 threads consumidoras;
- uma fila circular compartilhada.

A sincronização é feita utilizando:

- `pthread_mutex_t`;
- variável de condição `not_empty`;
- variável de condição `not_full`.

Cada produtora gera relógios vetoriais e os coloca na fila. As consumidoras retiram os relógios da fila e os imprimem.

### Compilar

```bash
cd etapa2_pthreads
gcc -Wall -Wextra -pthread -o pth_produtor_consumidor pth_produtor_consumidor.c
```

### Cenário 1 — Fila cheia

Os produtores produzem a cada 1 segundo e os consumidores consomem a cada 2 segundos.

```bash
./pth_produtor_consumidor cheia
```

Durante o teste devem aparecer mensagens indicando fila cheia e produtores aguardando espaço.

### Cenário 2 — Fila vazia

Os produtores produzem a cada 2 segundos e os consumidores consomem a cada 1 segundo.

```bash
./pth_produtor_consumidor vazia
```

Durante o teste devem aparecer mensagens indicando fila vazia e consumidores aguardando novos itens.

---

## Etapa 3 — Integração Produtor/Consumidor com Relógios Vetoriais

Integra as duas etapas anteriores utilizando MPI e Pthreads.

São executados três processos MPI (`P0`, `P1` e `P2`). Cada processo possui três threads:

- **Central:** executa as operações `EVENT`, `SEND` e `RECEIVE` do relógio vetorial;
- **Receptora:** recebe mensagens de outros processos através de `MPI_Recv`;
- **Emissora:** envia mensagens através de `MPI_Send`.

Cada processo possui duas filas independentes:

- fila de recepção;
- fila de envio.

Cada fila possui seu próprio mutex e suas próprias variáveis de condição `not_empty` e `not_full`.

### Funcionamento

Na fila de recepção:

```text
Thread Receptora
       ↓
   MPI_Recv
       ↓
Fila de Recepção
       ↓
 Thread Central
```

Na fila de envio:

```text
 Thread Central
       ↓
 Fila de Envio
       ↓
Thread Emissora
       ↓
   MPI_Send
```

A thread central não chama diretamente `MPI_Send` ou `MPI_Recv`.

No `SEND`, o relógio é atualizado e colocado na fila de envio. A thread emissora retira esse item da fila e realiza o envio através do MPI.

No `RECEIVE`, a thread central retira da fila de recepção a próxima mensagem disponível e atualiza seu relógio vetorial.

A thread receptora utiliza `MPI_ANY_SOURCE`, portanto a ordem de recebimento das mensagens pode variar entre execuções.

### Compilar

```bash
cd etapa3_integracao
mpicc -Wall -Wextra -pthread -o integracao integracao.c
```

### Executar

```bash
mpiexec -n 3 ./integracao
```

O programa deve ser executado com exatamente três processos.

Durante a execução podem aparecer mensagens como:

```text
FILA DE ENVIO VAZIA. Aguardando produtor...
FILA DE RECEPCAO VAZIA. Aguardando produtor...
```

Essas mensagens são esperadas e indicam o funcionamento do modelo Produtor/Consumidor.

Uma execução possível termina com:

```text
P0 = (7,1,2)
P1 = (6,3,2)
P2 = (4,1,3)
```

Como a recepção utiliza `MPI_ANY_SOURCE`, a ordem das mensagens pode variar. Em algumas execuções o estado final de `P2` também pode ser:

```text
P2 = (4,0,3)
```

Essa variação é consequência da ordem concorrente de chegada das mensagens.

### Teste com fila pequena

Também é possível compilar utilizando uma fila com capacidade igual a 1:

```bash
mpicc -Wall -Wextra -pthread -DBUFFER_SIZE=1 -o integracao integracao.c
```

Executar:

```bash
mpiexec -n 3 ./integracao
```

Esse teste aumenta a disputa entre produtores e consumidores e ajuda a observar o comportamento das filas.

Para voltar ao tamanho padrão da fila:

```bash
mpicc -Wall -Wextra -pthread -o integracao integracao.c
```

---

## Makefile

Na raiz do projeto:

```bash
make
```

### Executar Etapa 1

```bash
make run-etapa1
```

### Executar Etapa 2 — fila cheia

```bash
make run-cheia
```

### Executar Etapa 2 — fila vazia

```bash
make run-vazia
```

### Executar Etapa 3

```bash
make run-etapa3
```

### Limpar executáveis

```bash
make clean
```

## Tecnologias utilizadas

- Linguagem C
- MPI / OpenMPI
- Pthreads
- Mutex
- Variáveis de condição
- Relógios Vetoriais
- Modelo Produtor/Consumidor
