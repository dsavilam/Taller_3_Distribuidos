import json
import threading
import time
import uuid
import zmq

#  ID helper
def gen_id(prefix: str = "id") -> str:
    """
    Genera un ID corto con prefijo, timestamp en ms y sufijo aleatorio.
    Ej: req-1730312345123-a1b2c3
    """
    return f"{prefix}-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}"

#  Flag thread-safe para compartir estado "vivo" / "down"
class AliveFlag:
    def __init__(self, initial: bool = False):
        self._val = initial
        self._lock = threading.Lock()

    def get(self) -> bool:
        with self._lock:
            return self._val

    def set(self, v: bool):
        """Devuelve el valor anterior para poder detectar cambios."""
        with self._lock:
            old = self._val
            self._val = v
            return old

#  Helpers para PUB / SUB
def make_pub(ctx: zmq.Context, xsub_connect: str) -> zmq.Socket:
    """
    Crea un PUB que se CONECTA al XSUB del broker.
    """
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.LINGER, 0)
    pub.setsockopt(zmq.SNDHWM, 10000)
    pub.connect(xsub_connect)
    return pub

def make_sub(ctx: zmq.Context, xpub_connect: str, topics):
    """
    Crea un SUB que se CONECTA al XPUB del broker y se suscribe a una lista de topics/prefijos.
    """
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, 0)
    sub.setsockopt(zmq.RCVHWM, 10000)
    sub.connect(xpub_connect)
    if isinstance(topics, (list, tuple, set)):
        for t in topics:
            sub.setsockopt_string(zmq.SUBSCRIBE, t)
    elif isinstance(topics, str):
        sub.setsockopt_string(zmq.SUBSCRIBE, topics)
    else:
        raise ValueError("topics debe ser str o lista/tupla/set de str")
    return sub


#  Health monitor (REQ->REP) con logging configurable Usa el contrato del broker: {"type":"health"} -> {"type":"health_ok"}
def start_health_monitor(endpoint: str,
                         alive_flag: AliveFlag,
                         label: str = "broker",
                         interval: float = 1.0,
                         timeout_ms: int = 700,
                         quiet: bool = False,
                         log_mode: str = "changes",   # "changes" | "always"
                         log_every: float = None):
    """
    Lanza un hilo que hace ping periódico y actualiza 'alive_flag'.
    - log_mode="changes": imprime sólo cuando hay transición VIVO/DOWN.
    - log_mode="always":  imprime cada 'log_every' segundos (o cada 'interval' si log_every es None).
    Devuelve (stop_event, thread).
    """
    stop_event = threading.Event()

    def _worker():
        ctx = zmq.Context.instance()
        req = None
        last_log = 0.0

        def _ensure_socket():
            nonlocal req
            if req is None:
                req = ctx.socket(zmq.REQ)
                req.setsockopt(zmq.LINGER, 0)
                req.setsockopt(zmq.RCVTIMEO, timeout_ms)
                req.setsockopt(zmq.SNDTIMEO, timeout_ms)
                req.connect(endpoint)

        while not stop_event.is_set():
            _ensure_socket()
            ok = False
            try:
                req.send_json({"type": "health"})
                rep = req.recv_json()
                ok = (rep.get("type") == "health_ok")
            except Exception:
                ok = False
                # reset socket si hubo error
                try:
                    if req is not None:
                        req.close(0)
                except Exception:
                    pass
                req = None

            prev = alive_flag.set(ok)
            now = time.time()

            should_log = False
            if not quiet:
                if log_mode == "always":
                    period = (log_every if log_every is not None else interval)
                    if now - last_log >= period:
                        should_log = True
                else:  # "changes"
                    if prev is None or ok != prev:
                        should_log = True

            if should_log:
                print(f"[HC] {label}: {'VIVO' if ok else 'DOWN'}")
                last_log = now

            stop_event.wait(interval)

        try:
            if req is not None:
                req.close(0)
        except Exception:
            pass

    t = threading.Thread(target=_worker, name=f"HC-{label}", daemon=True)
    t.start()
    return stop_event, t

#  Publicación resiliente con backlog
def safe_publish(pub: zmq.Socket,
                 backlog,
                 alive_flag: AliveFlag,
                 topic: str,
                 payload: dict):
    """
    Publica (topic, payload) si el broker está vivo; si no, lo guarda en backlog.
    """
    if alive_flag.get():
        try:
            pub.send_multipart(
                [topic.encode("utf-8"), json.dumps(payload).encode("utf-8")],
                flags=zmq.DONTWAIT
            )
            return True
        except Exception:
            backlog.append((topic, payload))
            return False
    else:
        backlog.append((topic, payload))
        return False

def flush_backlog(pub: zmq.Socket, backlog):
    """
    Intenta vaciar el backlog cuando el broker vuelve a estar VIVO.
    """
    if not backlog:
        return
    remaining = []
    for topic, payload in backlog:
        try:
            pub.send_multipart(
                [topic.encode("utf-8"), json.dumps(payload).encode("utf-8")],
                flags=zmq.DONTWAIT
            )
        except Exception:
            remaining.append((topic, payload))
    backlog.clear()
    if remaining:
        backlog.extend(remaining)
