import argparse, zmq, threading, time, signal, json
from common.config import BROKER_CLAIM_BIND

def serve_health(bind_hc: str, ctx: zmq.Context, stop_event: threading.Event):
    rep = ctx.socket(zmq.REP)
    rep.setsockopt(zmq.LINGER, 0)
    rep.bind(bind_hc)
    print(f"[BROKER] Health REP en {bind_hc}")
    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    try:
        while not stop_event.is_set():
            socks = dict(poller.poll(200))
            if socks.get(rep) == zmq.POLLIN:
                try:
                    req = rep.recv_json(flags=0)
                    if req.get("type") == "health":
                        rep.send_json({"type": "health_ok", "role": "broker"})
                    else:
                        rep.send_json({"type": "error", "error": "unknown"})
                except Exception as e:
                    try:
                        rep.send_json({"type": "error", "error": str(e)})
                    except Exception:
                        pass
    finally:
        try: rep.close(0)
        except: pass

def start_claim_server(ctx: zmq.Context, stop_event: threading.Event):
    """REP simple para 'claim' y 'done' con set en memoria."""
    rep = ctx.socket(zmq.REP)
    rep.setsockopt(zmq.LINGER, 0)
    rep.bind(BROKER_CLAIM_BIND)

    claimed = set()
    print(f"[BROKER] CLAIM REP en {BROKER_CLAIM_BIND}")

    def loop():
        poller = zmq.Poller()
        poller.register(rep, zmq.POLLIN)
        while not stop_event.is_set():
            socks = dict(poller.poll(200))
            if socks.get(rep) == zmq.POLLIN:
                try:
                    msg = rep.recv_json(flags=0)
                    t = msg.get("type")
                    tid = msg.get("task_id")
                    if not tid:
                        rep.send_json({"ok": False, "error": "missing_task_id"})
                        continue

                    if t == "claim":
                        if tid not in claimed:
                            claimed.add(tid)
                            rep.send_json({"ok": True})
                        else:
                            rep.send_json({"ok": False})
                    elif t == "done":
                        if tid in claimed:
                            claimed.remove(tid)
                        rep.send_json({"ok": True})
                    else:
                        rep.send_json({"ok": False, "error": "bad_type"})
                except Exception as e:
                    try:
                        rep.send_json({"ok": False, "error": str(e)})
                    except Exception:
                        pass
        try:
            rep.close(0)
        except Exception:
            pass

    th = threading.Thread(target=loop, name="CLAIM-REP", daemon=True)
    th.start()
    return th

def main():
    ap = argparse.ArgumentParser(description="Broker ZMQ (XSUB<->XPUB) con health y claim/done")
    ap.add_argument("--xsub", default="tcp://*:7000", help="BIND XSUB (recibe de publicadores)")
    ap.add_argument("--xpub", default="tcp://*:7001", help="BIND XPUB (entrega a suscriptores)")
    ap.add_argument("--hc",   default="tcp://*:7002", help="BIND REP health")
    ap.add_argument("--ctl",  default="inproc://broker-ctl", help="PAIR para terminar proxy")
    args = ap.parse_args()

    stop_event = threading.Event()

    ctx = zmq.Context.instance()
    xsub = ctx.socket(zmq.XSUB); xsub.setsockopt(zmq.LINGER, 0); xsub.bind(args.xsub)
    xpub = ctx.socket(zmq.XPUB); xpub.setsockopt(zmq.LINGER, 0); xpub.bind(args.xpub)
    control = ctx.socket(zmq.PAIR); control.setsockopt(zmq.LINGER, 0); control.bind(args.ctl)

    print(f"[BROKER] XSUB en {args.xsub}")
    print(f"[BROKER] XPUB en {args.xpub}")

    # Health en hilo aparte
    th_health = threading.Thread(target=serve_health, args=(args.hc, ctx, stop_event), daemon=True)
    th_health.start()

    # Claim/Done REP en hilo aparte
    th_claim = start_claim_server(ctx, stop_event)

    # Proxy steerable en hilo para poder mandar TERMINATE al capturar Ctrl+C
    def proxy_thread():
        try:
            zmq.proxy_steerable(xsub, xpub, None, control)
        except zmq.ContextTerminated:
            pass
        except Exception as e:
            print(f"[BROKER] proxy error: {e}")

    tproxy = threading.Thread(target=proxy_thread, name="XPUB-XSUB-PROXY", daemon=True)
    tproxy.start()

    def shutdown(*_):
        # Señal de parada idempotente
        if not stop_event.is_set():
            stop_event.set()
            try:
                ctl = ctx.socket(zmq.PAIR)
                ctl.setsockopt(zmq.LINGER, 0)
                ctl.connect(args.ctl)
                ctl.send(b"TERMINATE")
                time.sleep(0.1)
                ctl.close(0)
            except Exception:
                pass

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while tproxy.is_alive():
            time.sleep(0.2)
    finally:
        try: xsub.close(0); xpub.close(0); control.close(0)
        except: pass
        # Espera breve a los hilos auxiliares
        time.sleep(0.2)
        try: ctx.term()
        except: pass
        print("[BROKER] Apagado")

if __name__ == "__main__":
    main()
