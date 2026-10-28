#!/usr/bin/env bash
# Executa o mini laboratorio da Aula 05 de ponta a ponta e salva as evidencias em evidencias/.
# Uso: ./executar_lab.sh
set -euo pipefail
cd "$(dirname "$0")"

PY=".venv/bin/python -u"
EV=evidencias
rm -rf "$EV" && mkdir -p "$EV"

titulo() { printf '\n========== %s ==========\n' "$1"; }
# inicia um processo em background gravando log; devolve o PID em $!
bg() { local log=$1; shift; $PY "$@" >>"$EV/$log" 2>&1 & }
parar() { kill -TERM "$@" 2>/dev/null || true; wait "$@" 2>/dev/null || true; }

titulo "Passo 1 - subindo RabbitMQ (docker compose, estado limpo)"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --wait
docker compose ps | tee "$EV/01_docker.log"

titulo "Passos 2, 3 e 4 - fila notificacoes, producer e consumer"
bg 02_consumer.log consumer.py --nome C1; C1=$!
sleep 2
$PY producer.py --quantidade 3 --inicio 1 | tee "$EV/02_producer.log"
sleep 3
parar $C1
cat "$EV/02_consumer.log"

titulo "Passo 5 - consumer parado: mensagens ficam pendentes"
$PY producer.py --quantidade 5 --inicio 4 | tee "$EV/05_producer.log"
$PY inspecionar.py | tee "$EV/05_fila_pendente.log"
echo "-- reiniciando o BROKER para mostrar que a mensagem e persistente --" | tee -a "$EV/05_fila_pendente.log"
docker compose restart rabbitmq >/dev/null && docker compose up -d --wait >/dev/null
$PY inspecionar.py | tee -a "$EV/05_fila_pendente.log"

titulo "Passo 6 - consumer reiniciado processa o acumulado"
bg 06_consumer.log consumer.py --nome C1; C1=$!
sleep 5
parar $C1
cat "$EV/06_consumer.log"
$PY inspecionar.py | tee "$EV/06_fila_vazia.log"

titulo "Passo 7 - falha proposital com nack(requeue=True): redelivery"
bg 07_consumer.log consumer.py --nome C1 --falhar-em PED-0010 --modo-falha requeue; C1=$!
sleep 2
$PY producer.py --quantidade 3 --inicio 9 | tee "$EV/07_producer.log"
sleep 4
parar $C1
cat "$EV/07_consumer.log"
echo "-- PED-0010 entregue $(grep -c 'PED-0010 recebido' "$EV/07_consumer.log") vezes; continua na fila --" | tee -a "$EV/07_consumer.log"
$PY inspecionar.py | tee "$EV/07_fila.log"

titulo "Passo 8 - limite de tentativas + dead letter queue"
bg 08_consumer.log consumer.py --nome C1 --falhar-em PED-0010; C1=$!
sleep 3
parar $C1
cat "$EV/08_consumer.log"
$PY inspecionar.py --dlq | tee "$EV/08_dlq.log"

titulo "Desafio 1 - dois consumidores concorrentes"
bg 09_consumidores.log consumer.py --nome C1; C1=$!
bg 09_consumidores.log consumer.py --nome C2; C2=$!
sleep 2
$PY producer.py --quantidade 10 --inicio 20 | tee "$EV/09_producer.log"
sleep 5
parar $C1 $C2
sort "$EV/09_consumidores.log" | tee "$EV/09_consumidores_ordenado.log"
echo "-- distribuicao --" | tee -a "$EV/09_consumidores_ordenado.log"
for c in C1 C2; do
  echo "$c: $(grep -c "consumer-$c\] pedido_id=.* processado" "$EV/09_consumidores.log") pedido(s)"
done | tee -a "$EV/09_consumidores_ordenado.log"

titulo "Desafio 2 - exchange fanout: Notificacoes e Analytics"
bg 10_assinantes.log assinante.py --servico notificacoes; A1=$!
bg 10_assinantes.log assinante.py --servico analytics; A2=$!
sleep 2
$PY producer.py --quantidade 4 --inicio 50 --destino fanout | tee "$EV/10_producer.log"
sleep 2
parar $A1 $A2
sort "$EV/10_assinantes.log" | tee "$EV/10_assinantes_ordenado.log"

titulo "Estado final das filas"
$PY inspecionar.py | tee "$EV/11_estado_final.log"
