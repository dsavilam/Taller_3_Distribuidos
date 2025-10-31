import argparse, json, time, threading, signal, zmq
from common.config import BROKER_XSUB, BROKER_XPUB, BROKER_HEALTH
from common.config import BROKER_CLAIM_CONNECT


from common.topics import TOP_RES_CHUNK, topic_hb
from common.utils import make_pub, make_sub, AliveFlag, start_health_monitor, safe_publish, flush_backlog

def heartbeat(pub, name, flag: AliveFlag, backlog):
    """Emite latidos periódicos (publicador) para observabilidad."""
    t = topic_hb(name)
    while not threading.current_thread().daemon is False:  # solo para evitar warnings
        pass
    while True:
        safe_publish(pub, backlog, flag, t, {"op": "hb", "ts": time.time(), "name": name})
        time.sleep(1.5)

def main():
    ap = argparse.ArgumentParser(description="Operador que procesa órdenes 'orden.chunk.*' y publica 'resultado.chunk'")
    ap.add_argument("--name", default="operador-2")
    ap.add_argument("--xsub", default=BROKER_XSUB, help="CONNECT al XSUB del broker (publicar)")
    ap.add_argument("--xpub", default=BROKER_XPUB, help="CONNECT al XPUB del broker (suscribirse)")
    ap.add_argument("--hc",   default=BROKER_HEALTH, help="REP health del broker")
    ap.add_argument("--claim", default=BROKER_CLAIM_CONNECT, help="REP claim/done del broker (CONNECT)")
    ap.add_argument("--subscribe_prefix", default="orden.chunk.", help="prefijo de tópicos a suscribir")
    ap.add_argument("--hc_interval", type=float, default=1.5)
    ap.add_argument("--hc_verbose", action="store_true", default=True)
    args = ap.parse_args()

    ctx = zmq.Context.instance()

    broker_alive = AliveFlag(False)
    start_health_monitor(
        args.hc,
        broker_alive,
        label="broker",
        interval=args.hc_interval,
        timeout_ms=800,
        quiet=not args.hc_verbose,
        log_mode="always",
        log_every=args.hc_interval
    )

    # PUB (resultados + heartbeat) con backlog para resiliencia
    pub = make_pub(ctx, args.xsub)
    backlog: list[tuple[str, dict]] = []

    # SUB: suscríbete al prefijo general para habilitar failover activo-activo
    sub = make_sub(ctx, args.xpub, [args.subscribe_prefix])
    print(f"[{args.name}] SUB a prefijo: {args.subscribe_prefix}")

    # REQ claim/done para single-consumer
    claim = ctx.socket(zmq.REQ)
    claim.setsockopt(zmq.LINGER, 0)
    claim.connect(args.claim)
    print(f"[{args.name}] CLAIM REQ -> {args.claim}")

    # Heartbeat en hilo
    hb_thread = threading.Thread(target=heartbeat, args=(pub, args.name, broker_alive, backlog), daemon=True)
    hb_thread.start()

    stop_event = threading.Event()
    def on_stop(*_):
        stop_event.set()
    signal.signal(signal.SIGINT, on_stop)
    signal.signal(signal.SIGTERM, on_stop)

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    try:
        while not stop_event.is_set():
            socks = dict(poller.poll(200))
            if socks.get(sub) == zmq.POLLIN:
                topic, payload = sub.recv_multipart(flags=0)
                data = json.loads(payload.decode("utf-8"))
                # Esperamos al menos corr_id + idx para formar una clave única
                corr_id = data.get("corr_id")
                idx = data.get("idx")
                if corr_id is None or idx is None:
                    # Mensaje mal formado; ignora
                    continue
                task_id = f"{corr_id}#{idx}"

                # CLAIM: si otro operador ya lo tomó, no lo procesamos
                try:
                    claim.send_json({"type": "claim", "task_id": task_id})
                    rep = claim.recv_json()
                except Exception as e:
                    print(f"[{args.name}] CLAIM error: {e}")
                    continue

                if not rep.get("ok", False):
                    # Otro operador lo está atendiendo
                    continue

                # --- Procesamiento ---
                a = data.get("a_chunk", [])
                b = data.get("b_chunk", [])
                print(f"[{args.name}] resuelve idx={idx} A={a} B={b}")

                if len(a) != len(b):
                    res = {"corr_id": corr_id, "idx": idx, "ok": False, "error": "len mismatch"}
                else:
                    try:
                        result = [int(x) + int(y) for x, y in zip(a, b)]
                        res = {"corr_id": corr_id, "idx": idx, "ok": True, "result": result, "by": args.name}
                    except Exception as e:
                        res = {"corr_id": corr_id, "idx": idx, "ok": False, "error": str(e)}

                # Publica resultado con resiliencia
                safe_publish(pub, backlog, broker_alive, TOP_RES_CHUNK, res)
                if broker_alive.get() and backlog:
                    flush_backlog(pub, backlog)

                # DONE: libera el claim
                try:
                    claim.send_json({"type": "done", "task_id": task_id})
                    _ = claim.recv_json()
                except Exception as e:
                    print(f"[{args.name}] DONE error: {e}")

    finally:
        try: sub.close(0)
        except: pass
        try: claim.close(0)
        except: pass
        try: pub.close(0)
        except: pass
        # No hacemos ctx.term() si es Context global compartido por otros procesos.
        print(f"[{args.name}] Apagado")

if __name__ == "__main__":
    main()
