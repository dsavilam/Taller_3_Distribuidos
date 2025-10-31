import argparse, json, time, threading, queue, zmq, collections
from typing import Dict, List
from common.config import BROKER_XSUB, BROKER_XPUB, BROKER_HEALTH
from common.topics import (TOP_SOL, TOP_ACK, TOP_RES_CHUNK, topic_resp, topic_orden_chunk, TOP_HB_OPER_PREFIX)
from common.utils import make_pub, make_sub, AliveFlag, start_health_monitor, safe_publish, flush_backlog

class PendingReq:
    def __init__(self, corr_id: str, reply_to: str, halves: List[tuple]):
        self.corr_id = corr_id
        self.reply_to = reply_to
        self.halves = {h[0]: {"a":h[1], "b":h[2], "assigned":h[3], "done":False, "result":None, "attempts":0, "last_send":0.0}
                       for h in halves}
        self.created = time.time()

class GC:
    def __init__(self, xsub, xpub, hc, operators: List[str]):
        self.ctx = zmq.Context.instance()
        self.broker_alive = AliveFlag()
        start_health_monitor(hc, self.broker_alive, label="broker", interval=1.0, timeout_ms=700, quiet=False,  log_mode="always",  
                             log_every=3.0)       

        self.pub = make_pub(self.ctx, xsub)
        self.backlog: List[tuple[str,dict]] = []

        topics = [TOP_SOL, TOP_RES_CHUNK, TOP_HB_OPER_PREFIX]
        self.sub = make_sub(self.ctx, xpub, topics)
        print(f"[GC] SUB a {topics}")

        self.inbox: "queue.Queue[tuple[str,dict]]" = queue.Queue()
        threading.Thread(target=self._sub_loop, daemon=True).start()

        self.pending: Dict[str, PendingReq] = {}
        self.recent_done: Dict[str, dict] = {}   # corr_id -> {"reply_to": str, "result": [...], "ts": float}
        self.ops = operators
        self.last_hb = collections.defaultdict(lambda: 0.0)
        threading.Thread(target=self._watchdog, daemon=True).start()

    def _sub_loop(self):
        while True:
            t, p = self.sub.recv_multipart()
            self.inbox.put((t.decode(), json.loads(p.decode())))

    def _assign_ops(self):
        if not self.ops: return [None, None]
        if len(self.ops) == 1: return [self.ops[0], self.ops[0]]
        return [self.ops[0], self.ops[1]]

    def _watchdog(self):
        TIMEOUT = 3.0
        while True:
            now = time.time()
            # limpiar recent_done (TTL 60s)
            for k in list(self.recent_done.keys()):
                if now - self.recent_done[k]["ts"] > 60:
                    del self.recent_done[k]
            # reintentos de halves
            for corr_id, pend in list(self.pending.items()):
                for idx, ch in pend.halves.items():
                    if ch["done"]: continue
                    if now - ch["last_send"] > TIMEOUT:
                        current = ch["assigned"]
                        candidates = [op for op in self.ops if op != current] or [current]
                        chosen = None
                        for op in candidates:
                            if (now - self.last_hb[op]) < 5.0:
                                chosen = op; break
                        if not chosen: chosen = candidates[0]
                        ch["assigned"] = chosen
                        self._send_half(corr_id, idx, pend)
            time.sleep(0.4)

    def _send_half(self, corr_id: str, idx: int, pend: PendingReq):
        ch = pend.halves[idx]
        out_t = topic_orden_chunk(ch["assigned"])
        msg = {"corr_id": corr_id, "idx": idx, "a_chunk": ch["a"], "b_chunk": ch["b"]}
        ok = safe_publish(self.pub, self.backlog, self.broker_alive, out_t, msg)
        if ok:
            print(f"[GC] → {out_t} corr={corr_id} idx={idx} A={ch['a']} B={ch['b']}")
        else:
            print(f"[GC] broker DOWN, encolo {out_t} corr={corr_id} idx={idx}")
        ch["last_send"] = time.time(); ch["attempts"] += 1

    def _handle_solicitud(self, data: dict):
        a = data.get("a") or []; b = data.get("b") or []
        corr_id = data.get("corr_id"); reply_to = data.get("reply_to")

        # ACK inmediato siempre (idempotente)
        if not isinstance(a, list) or not isinstance(b, list) or len(a)!=len(b):
            safe_publish(self.pub, self.backlog, self.broker_alive, TOP_ACK,
                         {"ok": False, "msg":"len mismatch", "corr_id": corr_id})
            return
        safe_publish(self.pub, self.backlog, self.broker_alive, TOP_ACK,
                     {"ok": True, "msg":"recibido", "corr_id": corr_id})

        # Si ya lo resolvimos, re-publica la respuesta y listo
        done = self.recent_done.get(corr_id)
        if done:
            safe_publish(self.pub, self.backlog, self.broker_alive, done["reply_to"],
                         {"corr_id": corr_id, "ok": True, "result": done["result"], "elapsed": 0.0})
            return

        # Si está pendiente, no dupliques halves; el watchdog sigue su curso
        if corr_id in self.pending:
            return

        n = len(a); mitad = n//2
        op_pref = self._assign_ops()
        halves = [
            (0, a[:mitad], b[:mitad], op_pref[0] or self.ops[0]),
            (1, a[mitad:], b[mitad:], op_pref[1] or self.ops[-1]),
        ]
        self.pending[corr_id] = PendingReq(corr_id, reply_to, halves)
        print(f"[GC] nueva solicitud corr={corr_id} n={n} → mitades={len(halves)}")
        for idx, *_ in halves:
            self._send_half(corr_id, idx, self.pending[corr_id])

    def _handle_resultado(self, data: dict):
        corr_id = data.get("corr_id")
        if corr_id not in self.pending: 
            return
        pend = self.pending[corr_id]
        idx = data.get("idx")
        if not data.get("ok"):
            pend.halves[idx]["last_send"] = 0
            return
        pend.halves[idx]["done"] = True
        pend.halves[idx]["result"] = data.get("result") or []
        if all(ch["done"] for ch in pend.halves.values()):
            res = (pend.halves[0]["result"] or []) + (pend.halves[1]["result"] or [])
            safe_publish(self.pub, self.backlog, self.broker_alive, pend.reply_to,
                         {"corr_id": corr_id, "ok": True, "result": res, "elapsed": time.time()-pend.created})
            self.recent_done[corr_id] = {"reply_to": pend.reply_to, "result": res, "ts": time.time()}
            print(f"[GC] corr={corr_id} COMPLETO → result={res}")
            del self.pending[corr_id]

    def _handle_hb(self, topic: str, data: dict):
        name = topic.split(".")[-1]
        self.last_hb[name] = time.time()

    def run(self):
        try:
            while True:
                if self.broker_alive.get() and self.backlog:
                    flush_backlog(self.pub, self.backlog)
                topic, data = self.inbox.get()
                if topic == TOP_SOL:
                    self._handle_solicitud(data)
                elif topic == TOP_RES_CHUNK:
                    self._handle_resultado(data)
                elif topic.startswith(TOP_HB_OPER_PREFIX):
                    self._handle_hb(topic, data)
        except KeyboardInterrupt:
            print("\n[GC] Saliendo…")
        finally:
            try: self.sub.close(0); self.pub.close(0); self.ctx.term()
            except: pass

def main():
    ap = argparse.ArgumentParser(description="Gestor/Coordinador con Broker Pub/Sub (suma por mitades)")
    ap.add_argument("--xsub", default=BROKER_XSUB)
    ap.add_argument("--xpub", default=BROKER_XPUB)
    ap.add_argument("--hc",   default=BROKER_HEALTH)
    ap.add_argument("--operadores", nargs="+", default=["operador-1","operador-2"])
    args = ap.parse_args()
    GC(args.xsub, args.xpub, args.hc, args.operadores).run()

if __name__ == "__main__":
    main()
