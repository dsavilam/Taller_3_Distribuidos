# Puertos / endpoints por defecto 
BROKER_XSUB   = "tcp://127.0.0.1:7000"  # Publishers aquí (los PUB se conectan a este XSUB del broker)
BROKER_XPUB   = "tcp://127.0.0.1:7001"  # Subscribers aquí (los SUB se conectan a este XPUB del broker)
BROKER_HEALTH = "tcp://127.0.0.1:7002"  # Health REP del broker

# Para el mecanismo de 'claim/done' (single-consumer o coordinación sencilla entre operadores)
# El broker hace bind en BROKER_CLAIM_BIND; los operadores hacen connect a BROKER_CLAIM_CONNECT.
BROKER_CLAIM_BIND    = "tcp://*:5561"
BROKER_CLAIM_CONNECT = "tcp://127.0.0.1:5561"  # Cambiar IP al de la VM del broker en despliegues
