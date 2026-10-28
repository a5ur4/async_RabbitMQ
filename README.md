# Aula 05 — Mensageria, Filas e Publish-Subscribe

**Mini laboratório: pedido assíncrono com RabbitMQ**
Disciplina: Sistemas Distribuídos e Computação Paralela

---

## 1. Ambiente e arquivos

| Item | Versão / descrição |
|---|---|
| Broker | RabbitMQ 4.4.0 (`rabbitmq:4-management`) via Docker Compose |
| Linguagem | Python 3 + biblioteca `pika` 1.4.4 (cliente AMQP 0-9-1) |
| Painel web | http://localhost:15672 (usuário/senha `guest`/`guest`) |

| Arquivo | Função |
|---|---|
| `docker-compose.yml` | Sobe o RabbitMQ com volume persistente e healthcheck (passo 1) |
| `comum.py` | Conexão, logs e declaração da topologia (fila, DLX/DLQ, exchange fanout) |
| `producer.py` | Publica pedidos JSON `{pedido_id, cliente, valor, criado_em}` (passo 3) |
| `consumer.py` | Consome a fila `notificacoes` e registra o processamento; simula falhas (passos 4, 7, 8) |
| `assinante.py` | Assinantes Pub/Sub `notificacoes` e `analytics` (desafio adicional) |
| `inspecionar.py` | Mostra mensagens pendentes/consumidores e o conteúdo da DLQ |
| `executar_lab.sh` | Roda todos os passos em sequência e grava as evidências em `evidencias/` |

### Topologia criada

```
producer ──(exchange padrão, routing_key=notificacoes)──► fila "notificacoes" (durável)
                                                              │ nack(requeue=False) após 3 tentativas
                                                              ▼
                                   exchange "notificacoes.dlx" ──► fila "notificacoes.dlq"

producer --destino fanout ──► exchange fanout "pedidos.eventos" ─┬─► fila "evento.notificacoes" ─► assinante notificacoes
                                                                 └─► fila "evento.analytics"    ─► assinante analytics
```

### Como executar

```bash
cd aula_5
python3 -m venv .venv && .venv/bin/pip install pika
./executar_lab.sh            # executa todos os passos e salva os logs em evidencias/
```

Ou manualmente, cada um em um terminal:

```bash
docker compose up -d --wait
.venv/bin/python consumer.py --nome C1
.venv/bin/python producer.py --quantidade 5 --inicio 1
.venv/bin/python inspecionar.py --dlq
```

### Decisões de implementação

- **Fila e mensagens duráveis**: a fila é `durable=True` e cada mensagem é publicada com `delivery_mode=Persistent`, então sobrevivem até a um restart do broker.
- **Publisher confirms** (`confirm_delivery`): o producer espera apenas o broker confirmar que *guardou* a mensagem (~1 ms), nunca o processamento.
- **Ack manual**: o consumer só faz `basic_ack` *depois* de processar. Se cair antes, a mensagem volta para a fila.
- **`prefetch_count=1`**: o broker só entrega a próxima mensagem após o ack da anterior, o que distribui melhor o trabalho entre consumidores.
- **Correlação**: o `pedido_id` vai no corpo, em `message_id` e em `correlation_id`, e aparece em **todas** as linhas de log (`pedido_id=PED-XXXX`).

---

## 2. Execução passo a passo (evidências)

Todos os trechos abaixo são saídas reais da execução de `./executar_lab.sh` (arquivos completos em `evidencias/`).

### Passo 1 — RabbitMQ via Docker

```
NAME          IMAGE                   STATUS                   PORTS
sd-rabbitmq   rabbitmq:4-management   Up 6 seconds (healthy)   0.0.0.0:5672->5672/tcp, 0.0.0.0:15672->15672/tcp
```

### Passos 2, 3 e 4 — fila `notificacoes`, producer e consumer

```
23:09:58.125 [producer] pedido_id=PED-0001 publicado em notificacoes cliente=Elisa valor=378.43 (confirmado pelo broker em 1.4 ms)
23:09:58.126 [producer] pedido_id=PED-0002 publicado em notificacoes cliente=Bruno valor=54.03 (confirmado pelo broker em 1.1 ms)
23:09:58.127 [producer] pedido_id=PED-0003 publicado em notificacoes cliente=Gabriela valor=399.16 (confirmado pelo broker em 1.0 ms)
23:09:58.127 [producer] 3 pedido(s) publicados em 4.0 ms - producer encerrado sem esperar processamento

23:09:58.125 [consumer-C1] pedido_id=PED-0001 recebido tentativa=1 redelivered=False cliente=Elisa valor=378.43
23:09:58.625 [consumer-C1] pedido_id=PED-0001 processado: notificacao enviada para Elisa (ack)
23:09:58.626 [consumer-C1] pedido_id=PED-0002 recebido tentativa=1 redelivered=False cliente=Bruno valor=54.03
23:09:59.127 [consumer-C1] pedido_id=PED-0002 processado: notificacao enviada para Bruno (ack)
23:09:59.128 [consumer-C1] pedido_id=PED-0003 recebido tentativa=1 redelivered=False cliente=Gabriela valor=399.16
23:09:59.628 [consumer-C1] pedido_id=PED-0003 processado: notificacao enviada para Gabriela (ack)
```

**Observação:** o producer terminou em **4 ms**, enquanto o consumer levou **~1,5 s** para processar os três pedidos (0,5 s cada). O producer não esperou o processamento final.

### Passo 5 — consumer parado: mensagens permanecem pendentes

```
23:10:01.441 [producer] 5 pedido(s) publicados em 5.6 ms - producer encerrado sem esperar processamento
23:10:01.606 [inspecionar] fila=notificacoes         mensagens_prontas=5   consumidores=0
-- reiniciando o BROKER para mostrar que a mensagem e persistente --
23:10:08.559 [inspecionar] fila=notificacoes         mensagens_prontas=5   consumidores=0
```

**Observação:** com **0 consumidores** a publicação funcionou normalmente e as 5 mensagens ficaram armazenadas. Como extra, o próprio RabbitMQ foi reiniciado (`docker compose restart`) e as 5 mensagens continuaram lá, graças à fila durável + mensagem persistente.

### Passo 6 — consumer reiniciado processa o acumulado

```
23:10:08.778 [consumer-C1] aguardando mensagens na fila 'notificacoes' (falhar_em=-, modo_falha=dlq)
23:10:08.778 [consumer-C1] pedido_id=PED-0004 recebido tentativa=1 redelivered=False cliente=Elisa valor=386.16
23:10:09.279 [consumer-C1] pedido_id=PED-0004 processado: notificacao enviada para Elisa (ack)
...
23:10:10.785 [consumer-C1] pedido_id=PED-0008 recebido tentativa=1 redelivered=False cliente=Fabio valor=459.57
23:10:11.285 [consumer-C1] pedido_id=PED-0008 processado: notificacao enviada para Fabio (ack)
23:10:13.826 [inspecionar] fila=notificacoes         mensagens_prontas=0   consumidores=0
```

**Observação:** assim que conectou, o consumer recebeu o *backlog* (PED-0004 a PED-0008) na ordem de chegada e esvaziou a fila.

### Passo 7 — falha proposital e redelivery (`nack(requeue=True)`)

O consumer foi iniciado com `--falhar-em PED-0010 --modo-falha requeue`:

```
23:10:16.554 [consumer-C1] pedido_id=PED-0009 processado: notificacao enviada para Heitor (ack)
23:10:16.555 [consumer-C1] pedido_id=PED-0010 recebido tentativa=1 redelivered=False cliente=Carla valor=484.91
23:10:17.055 [consumer-C1] pedido_id=PED-0010 FALHOU (falha simulada no envio da notificacao) -> nack requeue=True
23:10:17.057 [consumer-C1] pedido_id=PED-0010 recebido tentativa=1 redelivered=True cliente=Carla valor=484.91
23:10:17.557 [consumer-C1] pedido_id=PED-0010 FALHOU (falha simulada no envio da notificacao) -> nack requeue=True
23:10:17.590 [consumer-C1] pedido_id=PED-0010 recebido tentativa=1 redelivered=True cliente=Carla valor=484.91
...  (repete até o consumer ser parado)
-- PED-0010 entregue 7 vezes; continua na fila --
23:10:20.297 [inspecionar] fila=notificacoes         mensagens_prontas=2   consumidores=0
```

**Observações:**
- Após o `nack`, o broker **reentregou a mesma mensagem imediatamente**, agora com a flag `redelivered=True`.
- Como a mensagem volta para a **frente** da fila, ela é reentregue sem parar e **bloqueia** a fila: o PED-0011 nunca foi processado enquanto o PED-0010 falhava. É a chamada *poison message*.
- Num primeiro teste deixamos a falha instantânea (sem tempo de processamento) e o consumer gerou **3.444 linhas de log em 4 segundos**: um loop quente de redelivery.
- Detalhe encontrado: na versão 4.x do RabbitMQ, `nack(requeue=True)` **não** incrementa o contador `x-delivery-count` das quorum queues, então o `x-delivery-limit` não interrompeu o loop. Por isso o limite de tentativas do passo 8 foi implementado na aplicação.

### Passo 8 — limite de tentativas + Dead Letter Queue

A fila `notificacoes` foi declarada com `x-dead-letter-exchange=notificacoes.dlx`. O consumer (modo padrão) conta as tentativas no header `x-tentativa`: até 3 tentativas ele republica a mensagem no fim da fila; na terceira falha faz `nack(requeue=False)` e o **broker** move a mensagem para a DLQ.

```
23:10:20.508 [consumer-C1] pedido_id=PED-0010 recebido tentativa=1 redelivered=True cliente=Carla valor=484.91
23:10:21.008 [consumer-C1] pedido_id=PED-0010 FALHOU (falha simulada no envio da notificacao) -> reenfileirado para tentativa 2/3
23:10:21.009 [consumer-C1] pedido_id=PED-0011 recebido tentativa=1 redelivered=False cliente=Fabio valor=369.00
23:10:21.510 [consumer-C1] pedido_id=PED-0011 processado: notificacao enviada para Fabio (ack)
23:10:21.511 [consumer-C1] pedido_id=PED-0010 recebido tentativa=2 redelivered=False cliente=Carla valor=484.91
23:10:22.012 [consumer-C1] pedido_id=PED-0010 FALHOU (falha simulada no envio da notificacao) -> reenfileirado para tentativa 3/3
23:10:22.014 [consumer-C1] pedido_id=PED-0010 recebido tentativa=3 redelivered=False cliente=Carla valor=484.91
23:10:22.514 [consumer-C1] pedido_id=PED-0010 FALHOU 3 vezes -> nack requeue=False -> enviado para a DLQ

23:10:23.516 [inspecionar] fila=notificacoes         mensagens_prontas=0   consumidores=0
23:10:23.517 [inspecionar] fila=notificacoes.dlq     mensagens_prontas=1   consumidores=0
23:10:23.521 [inspecionar] DLQ -> {'pedido_id': 'PED-0010', 'cliente': 'Carla', 'valor': 484.91, ...} | motivo=rejected fila_origem=notificacoes
```

**Observações:**
- Como a retentativa vai para o **fim** da fila, o PED-0011 (que estava travado no passo 7) foi processado entre as tentativas: a mensagem problemática deixou de bloquear as outras.
- Depois de 3 falhas, o PED-0010 foi **isolado na DLQ**. O header `x-death` adicionado pelo broker registra o motivo (`rejected`) e a fila de origem, o que permite análise e reprocessamento manual depois.

### Desafio 1 — dois consumidores concorrentes

Dois consumers (`C1` e `C2`) na mesma fila, 10 pedidos publicados:

```
23:10:25.701 [consumer-C1] pedido_id=PED-0020 recebido ...
23:10:25.702 [consumer-C2] pedido_id=PED-0021 recebido ...
23:10:26.202 [consumer-C1] pedido_id=PED-0022 recebido ...
23:10:26.203 [consumer-C2] pedido_id=PED-0023 recebido ...
...
23:10:27.707 [consumer-C1] pedido_id=PED-0028 recebido ...
23:10:27.707 [consumer-C2] pedido_id=PED-0029 recebido ...
-- distribuicao --
C1: 5 pedido(s)
C2: 5 pedido(s)
```

**Observação:** cada mensagem foi entregue a **um único** consumidor, alternando entre eles (*competing consumers*). Os 10 pedidos foram processados em ~2,5 s em vez de ~5 s com um consumidor só: a fila **distribui trabalho** e escala horizontalmente.

### Desafio 2 — exchange fanout (Notificações + Analytics)

```
23:10:33.021 [producer] 4 pedido(s) publicados em 7.4 ms - producer encerrado sem esperar processamento
23:10:33.015 [assinante-analytics] pedido_id=PED-0050 contabilizado: pedidos=1 faturamento=372.23 ticket_medio=372.23
23:10:33.015 [assinante-notificacoes] pedido_id=PED-0050 e-mail de confirmacao enviado para Diego
23:10:33.018 [assinante-analytics] pedido_id=PED-0051 contabilizado: pedidos=2 faturamento=836.52 ticket_medio=418.26
23:10:33.018 [assinante-notificacoes] pedido_id=PED-0051 e-mail de confirmacao enviado para Heitor
23:10:33.019 [assinante-analytics] pedido_id=PED-0052 contabilizado: pedidos=3 faturamento=1110.87 ticket_medio=370.29
23:10:33.019 [assinante-notificacoes] pedido_id=PED-0052 e-mail de confirmacao enviado para Elisa
23:10:33.021 [assinante-analytics] pedido_id=PED-0053 contabilizado: pedidos=4 faturamento=1431.63 ticket_medio=357.91
23:10:33.021 [assinante-notificacoes] pedido_id=PED-0053 e-mail de confirmacao enviado para Bruno
```

**Observação:** diferente da fila de trabalho, **cada** assinante recebeu **todos** os 4 eventos, cada um na sua própria fila. O producer não conhece os assinantes: para adicionar um terceiro serviço bastaria criar outra fila ligada à exchange, sem mudar o producer. Pub/Sub **distribui eventos**.

---

## 3. O que observar — resultado

| O que observar | Comprovado? | Evidência |
|---|---|---|
| A mensagem pode existir independentemente do consumer | ✅ | Passo 5: 5 mensagens pendentes com 0 consumidores, inclusive após reiniciar o broker |
| O producer não precisa esperar o processamento final | ✅ | Producer termina em 4–11 ms; consumer leva 0,5 s por pedido |
| Mensagens podem ser reenviadas | ✅ | Passo 7: PED-0010 reentregue 7× com `redelivered=True`; passo 8: 3 tentativas e depois a DLQ |
| Logs contêm um identificador do pedido para correlação | ✅ | Todas as linhas trazem `pedido_id=PED-XXXX` (também em `message_id`/`correlation_id`) |

---

## 4. Questões de revisão

**1. Quando uma fila é mais adequada que uma chamada HTTP direta?**
Quando quem chama **não precisa da resposta na hora** para continuar (enviar e-mail, gerar nota fiscal, atualizar analytics); quando o destino pode estar **fora do ar ou lento** e não queremos que essa falha se propague para quem chamou (desacoplamento temporal: o passo 5 mostrou pedidos aceitos com o consumer parado); quando há **picos de carga** que precisam ser absorvidos e processados no ritmo do consumidor; e quando queremos **distribuir trabalho** entre vários workers (desafio 1). HTTP direto continua melhor quando o cliente precisa do resultado imediatamente (ex.: consultar estoque antes de confirmar a compra, como na Aula 3).

**2. Qual a diferença entre Queue e Publish/Subscribe?**
Na **Queue (ponto a ponto / competing consumers)** cada mensagem é entregue a **um único** consumidor; vários consumidores dividem o trabalho (desafio 1: 5 pedidos para C1 e 5 para C2). No **Publish/Subscribe** o publicador emite um **evento** e **cada assinante** recebe a sua cópia (desafio 2: Notificações e Analytics receberam os 4 eventos). No RabbitMQ, o Pub/Sub é feito com uma exchange (ex.: fanout) ligada a uma fila por assinante. Resumindo: fila distribui **trabalho**; pub/sub distribui **eventos**.

**3. Por que acknowledgement pode levar a redelivery?**
Porque o broker só apaga a mensagem quando recebe o **ack**. Se o consumer manda `nack`/`reject` com requeue, cai ou perde a conexão **antes** do ack, o broker não tem como saber se o processamento aconteceu e entrega a mensagem de novo (com `redelivered=True`, como no passo 7). O caso crítico é quando o consumer **processou** (ex.: enviou o e-mail) mas morreu antes de o ack chegar ao broker: a mensagem é reentregue e o trabalho é feito **duas vezes**. Ou seja, o ack evita perder mensagens, mas em troca pode gerar duplicidade.

**4. Qual a relação entre at-least-once e idempotência?**
*At-least-once* garante que a mensagem será processada **pelo menos uma vez**, mas permite que seja processada **mais de uma** (pela redelivery da questão 3). Para o resultado ficar correto, o consumer precisa ser **idempotente**: processar a mesma mensagem duas vezes deve ter o mesmo efeito que processar uma vez. Na prática isso se faz guardando os `pedido_id`/`message_id` já processados (tabela de deduplicação, chave única no banco) e ignorando repetições, ou usando operações naturalmente idempotentes (`UPDATE status='PAGO'` em vez de `saldo = saldo - valor`). At-least-once + idempotência resulta em um efeito *exactly-once* do ponto de vista do negócio. O laboratório também mostrou esse risco: no passo 8 a retentativa é feita com "republicar e depois dar ack"; se o consumer cair entre as duas operações, a mensagem aparece duplicada.

**5. Qual a finalidade de uma Dead Letter Queue?**
Isolar mensagens que **não podem ser processadas** (dados inválidos, bug, dependência permanentemente fora do ar) depois de um número limite de tentativas, para que elas **não bloqueiem a fila nem fiquem em loop infinito** consumindo recursos (passo 7) e **não sejam perdidas**. Na DLQ elas ficam guardadas com metadados (`x-death`: motivo, fila de origem, quantidade), permitindo alertas, investigação e reprocessamento manual depois da correção (passo 8).

---

## 5. Problemas encontrados

1. **Loop infinito de redelivery.** A primeira versão usava quorum queue com `x-delivery-limit=2`, esperando que o RabbitMQ mandasse a mensagem para a DLQ após 3 entregas. Na versão 4.4, `basic.nack(requeue=True)` **não incrementa** o `x-delivery-count`, então o limite nunca foi atingido e o consumer gerou 3.444 linhas de log em 4 s. **Solução:** fila clássica com DLX e contagem de tentativas na aplicação (header `x-tentativa`), finalizando com `nack(requeue=False)`.
2. **Logs internos do `pika`** poluíam a saída (nível INFO). **Solução:** logger `pika` configurado para nível ERROR.
3. **Argumentos de fila são imutáveis.** Mudar o tipo da fila ou o `x-dead-letter-exchange` de uma fila já existente gera erro `PRECONDITION_FAILED`. Por isso o script começa com `docker compose down -v` (estado limpo) e a fila já é criada com a DLX desde o passo 2.

---

## 6. Conclusão

O laboratório mostrou na prática que a mensageria **desacopla** produtor e consumidor no tempo: pedidos foram aceitos em milissegundos mesmo com o consumer desligado, ficaram guardados de forma durável (inclusive com restart do broker) e foram processados quando o consumer voltou. Também ficou claro que a mensageria **não elimina falhas, apenas muda onde elas aparecem**: o mecanismo de ack, que impede a perda de mensagens, é o mesmo que causa redelivery e, portanto, duplicidade (exige consumers idempotentes). Uma única mensagem com problema conseguiu travar a fila inteira até que um limite de tentativas e uma **Dead Letter Queue** fossem configurados. Por fim, os desafios mostraram as duas formas de distribuição: a **fila** dividiu o trabalho entre consumidores concorrentes (5/5) e a **exchange fanout** entregou cada evento a todos os serviços interessados (Notificações e Analytics), sem que o producer precisasse conhecê-los.
