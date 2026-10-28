"""
Mostra o estado das filas (mensagens prontas e consumidores conectados).

  python inspecionar.py            # contadores
  python inspecionar.py --dlq      # tambem lista o conteudo da DLQ, sem remover nada
"""
import argparse
import json

import pika

from comum import DLQ, FILA, FILAS_ASSINANTES, conectar, configurar_log

log = configurar_log("inspecionar")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dlq", action="store_true")
    args = parser.parse_args()

    conexao = conectar()
    canal = conexao.channel()
    for fila in [FILA, DLQ, *FILAS_ASSINANTES.values()]:
        try:
            r = canal.queue_declare(fila, passive=True)  # passive: so consulta, nao cria
            log.info("fila=%-20s mensagens_prontas=%-3d consumidores=%d",
                     fila, r.method.message_count, r.method.consumer_count)
        except pika.exceptions.ChannelClosedByBroker:
            canal = conexao.channel()  # fila ainda nao existe

    if args.dlq:
        # basic_get sem ack: ao fechar a conexao as mensagens voltam para a DLQ.
        while True:
            method, props, corpo = canal.basic_get(DLQ, auto_ack=False)
            if method is None:
                break
            morte = (props.headers or {}).get("x-death", [{}])[0]
            log.info("DLQ -> %s | motivo=%s fila_origem=%s",
                     json.loads(corpo), morte.get("reason"), morte.get("queue"))
    conexao.close()


if __name__ == "__main__":
    main()
