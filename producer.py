"""
Producer de pedidos - Aula 05

Passo 3: publica um JSON com id do pedido, cliente e valor.

  python producer.py --quantidade 5 --inicio 1                 # fila "notificacoes"
  python producer.py --quantidade 4 --inicio 50 --destino fanout  # exchange fanout

O producer so espera a CONFIRMACAO DO BROKER (publisher confirm: "a mensagem esta
guardada"), nunca o processamento final feito pelo consumer.
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

import pika

from comum import EXCHANGE_FANOUT, FILA, conectar, configurar_log, declarar_fanout, declarar_fila_trabalho

CLIENTES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fabio", "Gabriela", "Heitor"]

log = configurar_log("producer")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quantidade", type=int, default=5)
    parser.add_argument("--inicio", type=int, default=1, help="numero do primeiro pedido")
    parser.add_argument("--destino", choices=["fila", "fanout"], default="fila")
    args = parser.parse_args()

    conexao = conectar()
    canal = conexao.channel()
    if args.destino == "fila":
        declarar_fila_trabalho(canal)
        exchange, routing_key = "", FILA
    else:
        declarar_fanout(canal)
        exchange, routing_key = EXCHANGE_FANOUT, ""
    canal.confirm_delivery()  # basic_publish passa a bloquear ate o broker confirmar

    inicio_total = time.perf_counter()
    for n in range(args.inicio, args.inicio + args.quantidade):
        pedido = {
            "pedido_id": f"PED-{n:04d}",
            "cliente": random.choice(CLIENTES),
            "valor": round(random.uniform(10, 500), 2),
            "criado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        propriedades = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,  # sobrevive a restart do broker
            message_id=pedido["pedido_id"],
            correlation_id=pedido["pedido_id"],  # id de correlacao usado nos logs
        )
        t0 = time.perf_counter()
        canal.basic_publish(exchange, routing_key, json.dumps(pedido), propriedades, mandatory=True)
        log.info(
            "pedido_id=%s publicado em %s cliente=%s valor=%.2f (confirmado pelo broker em %.1f ms)",
            pedido["pedido_id"], exchange or FILA, pedido["cliente"], pedido["valor"],
            (time.perf_counter() - t0) * 1000,
        )

    log.info(
        "%d pedido(s) publicados em %.1f ms - producer encerrado sem esperar processamento",
        args.quantidade, (time.perf_counter() - inicio_total) * 1000,
    )
    conexao.close()


if __name__ == "__main__":
    main()
