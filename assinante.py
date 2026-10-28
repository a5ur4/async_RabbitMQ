"""
Assinante Pub/Sub - Aula 05 (desafio adicional)

Cada servico tem a SUA fila ligada a exchange fanout "pedidos.eventos",
entao todos os servicos recebem uma copia de cada evento.

  python assinante.py --servico notificacoes
  python assinante.py --servico analytics
"""
import argparse
import json

from comum import FILAS_ASSINANTES, conectar, configurar_log, declarar_fanout, parar_com_sigterm


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--servico", choices=list(FILAS_ASSINANTES), required=True)
    args = parser.parse_args()

    log = configurar_log(f"assinante-{args.servico}")
    parar_com_sigterm()

    conexao = conectar()
    canal = conexao.channel()
    declarar_fanout(canal)
    fila = FILAS_ASSINANTES[args.servico]
    estatisticas = {"pedidos": 0, "total": 0.0}

    def processar(ch, method, props, corpo):
        pedido = json.loads(corpo)
        if args.servico == "notificacoes":
            log.info("pedido_id=%s e-mail de confirmacao enviado para %s", pedido["pedido_id"], pedido["cliente"])
        else:
            estatisticas["pedidos"] += 1
            estatisticas["total"] += pedido["valor"]
            log.info(
                "pedido_id=%s contabilizado: pedidos=%d faturamento=%.2f ticket_medio=%.2f",
                pedido["pedido_id"], estatisticas["pedidos"], estatisticas["total"],
                estatisticas["total"] / estatisticas["pedidos"],
            )
        ch.basic_ack(method.delivery_tag)

    canal.basic_consume(fila, processar)
    log.info("assinando exchange fanout via fila '%s'", fila)
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        log.info("assinante parado")
    finally:
        if conexao.is_open:
            conexao.close()


if __name__ == "__main__":
    main()
