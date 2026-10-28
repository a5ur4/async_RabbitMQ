"""
Configuracao compartilhada - Aula 05 (Mensageria, Filas e Publish-Subscribe)

Topologia criada no RabbitMQ:

  [producer] --(default exchange, routing_key=notificacoes)--> fila "notificacoes" (duravel)
                                                                  |  falhou MAX_TENTATIVAS vezes (nack requeue=False)
                                                                  v
                                       exchange "notificacoes.dlx" --> fila "notificacoes.dlq"

  [producer --destino fanout] --> exchange fanout "pedidos.eventos" --+--> fila "evento.notificacoes"
                                                                      +--> fila "evento.analytics"
"""
import logging
import os
import signal
import sys
import time

import pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

FILA = "notificacoes"
DLX = "notificacoes.dlx"
DLQ = "notificacoes.dlq"
EXCHANGE_FANOUT = "pedidos.eventos"
FILAS_ASSINANTES = {"notificacoes": "evento.notificacoes", "analytics": "evento.analytics"}

# Passo 8: quantas vezes o consumer tenta processar um pedido antes de manda-lo para a DLQ.
MAX_TENTATIVAS = 3


def configurar_log(nome):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("pika").setLevel(logging.ERROR)  # esconde logs internos da biblioteca
    return logging.getLogger(nome)


def conectar(tentativas=30):
    """Abre conexao AMQP, tentando de novo enquanto o broker ainda esta subindo."""
    for i in range(tentativas):
        try:
            return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        except pika.exceptions.AMQPConnectionError:
            if i == tentativas - 1:
                raise
            time.sleep(1)


def declarar_fila_trabalho(canal):
    """Passos 2 e 8: fila 'notificacoes' duravel com dead letter configurada.

    Mensagens rejeitadas com nack(requeue=False) sao enviadas pelo broker para a DLX.
    """
    canal.exchange_declare(DLX, exchange_type="direct", durable=True)
    canal.queue_declare(DLQ, durable=True)
    canal.queue_bind(DLQ, DLX, routing_key=FILA)
    canal.queue_declare(
        FILA,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": FILA,
        },
    )


def declarar_fanout(canal):
    """Desafio adicional: exchange fanout com uma fila por servico assinante."""
    canal.exchange_declare(EXCHANGE_FANOUT, exchange_type="fanout", durable=True)
    for fila in FILAS_ASSINANTES.values():
        canal.queue_declare(fila, durable=True)
        canal.queue_bind(fila, EXCHANGE_FANOUT)


def parar_com_sigterm():
    """Faz 'kill' (SIGTERM) se comportar como Ctrl+C, para o consumer fechar limpo."""
    def handler(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, handler)
