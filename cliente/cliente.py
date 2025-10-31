import argparse, json, sys, zmq, time
from common.config import BROKER_XSUB, BROKER_XPUB, BROKER_HEALTH
from common.topics import TOP_SOL, TOP_ACK, topic_resp
from common.utils import make_pub, make_sub, AliveFlag, start_health_monitor, safe_publish, flush_backlog, gen_id

def leer_arreglo(nombre: str, n: int):
    print(f"Ingrese {n} enteros para {nombre}, separados por espacios:")
    parts = (sys.stdin.readline() or "").strip().split()
    if len(parts) != n:
        print(f"Se esperaban {n} números; recibidos {len(parts)}"); sys.exit(1)
    try:
        return [int(x) for x in parts]
    except:
        print("Entrada inválida"); sys.exit(1)

def main():
    ap = argparse.ArgumentParser(description="Cliente Pub/Sub: solicita suma A[]+B[]")
    ap.add_argument("--xsub", default=BROKER_XSUB)
    ap.add_argument("--xpub", default=BROKER_XPUB)
    ap.add_argument("--hc",   default=BROKER_HEALTH)
    ap.add_argument("--n", type=int, default=6)
    ap.add_argument("--client_id", default=None)
    ap.add_argument("--ack_timeout", type=float, default=2.0, help="segundos para reintentar si no hay ACK")
    args = ap.parse_args()

    a = leer_arreglo("A", args.n)
    b = leer_arreglo("B", args.n)

    client_id = args.client_id or gen_id("cli")
    corr_id = gen_id("req")
    reply_t = topic_resp(client_id)

    ctx = zmq.Context.instance()
    broker_alive = AliveFlag()
    # Silencioso para el usuario:
    start_health_monitor(args.hc, broker_alive, label="broker", interval=1.0, timeout_ms=700, quiet=True)

    pub = make_pub(ctx, args.xsub)
    backlog: list[tuple[str,dict]] = []
    sub = make_sub(ctx, args.xpub, [reply_t, TOP_ACK])
    print(f"[cliente] Esperando resultado… (id={client_id}, corr={corr_id})")

    # Publica solicitud (si broker está caído, se encola)
    msg = {"a": a, "b": b, "corr_id": corr_id, "reply_to": reply_t}
    sent = safe_publish(pub, backlog, broker_alive, TOP_SOL, msg)

    acked = False
    last_send = time.time()
    resend_attempts = 0
    t0 = time.time()

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    try:
        while True:
            # flush periódico si el broker volvió
            if broker_alive.get() and backlog:
                flush_backlog(pub, backlog)

            # reintento si no llega ACK
            if not acked and (time.time() - last_send) >= args.ack_timeout and broker_alive.get():
                safe_publish(pub, backlog, broker_alive, TOP_SOL, msg)
                last_send = time.time()
                resend_attempts += 1

            events = dict(poller.poll(250))  # 250ms
            if events.get(sub) == zmq.POLLIN:
                t, p = sub.recv_multipart()
                topic = t.decode(); data = json.loads(p.decode())
                if topic == TOP_ACK and data.get("corr_id") == corr_id:
                    acked = True
                elif topic == reply_t and data.get("corr_id") == corr_id:
                    ok = data.get("ok"); res = data.get("result")
                    print(f"[cliente] OK={ok} tiempo={time.time()-t0:.3f}s result={res}")
                    break
    except KeyboardInterrupt:
        print("\n[cliente] cancelado")
    finally:
        try: sub.close(0); pub.close(0); ctx.term()
        except: pass

if __name__ == "__main__":
    main()
