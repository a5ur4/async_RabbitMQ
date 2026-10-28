"""
Consumer de notificacoes - Aula 05

Passo 4: le a mensagem da fila "notificacoes" e registra um log de processamento.
Passo 7: --falhar-em PED-XXXX --modo-falha requeue
         falha de proposito e devolve com nack(requeue=True): o broker REENTREGA
         a mesma mensagem imediatamente, sem limite (mensagem "envenenada").
Passo 8: --falhar-em PED-XXXX (modo padrao "dlq")
         conta as tentativas no header x-tentativa; depois de MAX_TENTATIVAS
         rejeita com nack(requeue=False) e o broker manda para a DLQ.
Desafio: rode duas instancias (--nome C1 / --nome C2) para ver consumidores concorrentes.

  python consumer.py --nome C1
  python consumer.py --nome C1 --falhar-em PED-0010 --modo-falha requeue
  python consumer.py --nome C1 --falhar-em PED-0010
"""
import argparse
import json
import time

import pika

from comum import FILA, MAX_TENTATIVAS, conectar, configurar_log, declarar_fila_trabalho, parar_com_sigterm


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nome", default="C1")
    parser.add_argument("--falhar-em", nargs="*", default=[], help="pedido_id(s) que devem falhar")
    parser.add_argument("--modo-falha", choices=["requeue", "dlq"], default="dlq")
    parser.add_argument("--tempo-processamento", type=float, default=0.5, help="segundos por mensagem")
    parser.add_argument("--prefetch", type=int, default=1)
    args = parser.parse_args()

    log = configurar_log(f"consumer-{args.nome}")
    parar_com_sigterm()

    conexao = conectar()
    canal = conexao.channel()
    declarar_fila_trabalho(canal)
    # prefetch=1: o broker so manda a proxima mensagem depois do ack da anterior
    # (distribui melhor o trabalho entre consumidores concorrentes).
    canal.basic_qos(prefetch_count=args.prefetch)

    def processar(ch, method, props, corpo):
        pedido = json.loads(corpo)
        pid = pedido["pedido_id"]
        tentativa = (props.headers or {}).get("x-tentativa", 1)
        log.info(
            "pedido_id=%s recebido tentativa=%d redelivered=%s cliente=%s valor=%.2f",
            pid, tentativa, method.redelivered, pedido["cliente"], pedido["valor"],
        )
        try:
            time.sleep(args.tempo_processamento)  # simula envio de e-mail/SMS
            if pid in args.falhar_em:
                raise RuntimeError("falha simulada no envio da notificacao")
            ch.basic_ack(method.delivery_tag)  # so confirma DEPOIS de processar
            log.info("pedido_id=%s processado: notificacao enviada para %s (ack)", pid, pedido["cliente"])
        except Exception as erro:
            if args.modo_falha == "requeue":
                log.warning("pedido_id=%s FALHOU (%s) -> nack requeue=True", pid, erro)
                ch.basic_nack(method.delivery_tag, requeue=True)
            elif tentativa < MAX_TENTATIVAS:
                # republica uma copia com o contador incrementado e confirma a original
                novas_props = pika.BasicProperties(
                    content_type=props.content_type,
                    delivery_mode=props.delivery_mode,
                    message_id=props.message_id,
                    correlation_id=props.correlation_id,
                    headers={**(props.headers or {}), "x-tentativa": tentativa + 1},
                )
                ch.basic_publish("", FILA, corpo, novas_props)
                ch.basic_ack(method.delivery_tag)
                log.warning("pedido_id=%s FALHOU (%s) -> reenfileirado para tentativa %d/%d",
                            pid, erro, tentativa + 1, MAX_TENTATIVAS)
            else:
                ch.basic_nack(method.delivery_tag, requeue=False)
                log.error("pedido_id=%s FALHOU %d vezes -> nack requeue=False -> enviado para a DLQ",
                          pid, tentativa)

    canal.basic_consume(FILA, processar)
    log.info("aguardando mensagens na fila '%s' (falhar_em=%s, modo_falha=%s)",
             FILA, args.falhar_em or "-", args.modo_falha)
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        log.info("consumer parado")
    finally:
        if conexao.is_open:
            conexao.close()


if __name__ == "__main__":
    main()
