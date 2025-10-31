# Topics:
# - solicitud.sumarrays               (cliente → GC)
# - ack.solicitud                    (GC → cliente)
# - orden.chunk.<operador>           (GC → operador-*)
# - resultado.chunk                  (operador-* → GC)
# - respuesta.sumarrays.<clientId>   (GC → cliente)
# - hb.operador.<operador>           (operador-* → GC) heartbeat

TOP_SOL = "solicitud.sumarrays"
TOP_ACK = "ack.solicitud"
TOP_RES_CHUNK = "resultado.chunk"
TOP_RESP_PREFIX = "respuesta.sumarrays"
TOP_ORD_CHUNK_PREFIX = "orden.chunk"
TOP_HB_OPER_PREFIX = "hb.operador"

def topic_resp(client_id: str) -> str:
    return f"{TOP_RESP_PREFIX}.{client_id}"

def topic_orden_chunk(op_name: str) -> str:
    return f"{TOP_ORD_CHUNK_PREFIX}.{op_name}"

def topic_hb(op_name: str) -> str:
    return f"{TOP_HB_OPER_PREFIX}.{op_name}"

