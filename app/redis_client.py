# redis_client.py
import json
import logging
from typing import Optional, Dict, Any

from upstash_redis import Redis
from app.config import UPSTASH_REDIS_URL, UPSTASH_REDIS_TOKEN

logger = logging.getLogger("redis")

redis: Optional[Redis] = None


# ==========================================================
# Inicialización segura
# ==========================================================

if UPSTASH_REDIS_URL and UPSTASH_REDIS_TOKEN:
    try:
        redis = Redis(
            url=UPSTASH_REDIS_URL,
            token=UPSTASH_REDIS_TOKEN,
        )
        logger.info("Cliente Redis inicializado correctamente.")
    except Exception as e:
        logger.error(f"Error al inicializar Redis: {e}")
        redis = None
else:
    logger.warning(
        "Redis no configurado: faltan variables UPSTASH_REDIS_URL o UPSTASH_REDIS_TOKEN."
    )


# ==========================================================
# SET
# ==========================================================

def kv_set(
    key: str,
    value: Dict[str, Any],
    ttl_seconds: Optional[int] = None
) -> bool:
    """
    Guarda un valor en Redis serializado como JSON.
    """
    if not redis:
        logger.debug("Intento de SET ignorado: Redis no está disponible.")
        return False

    try:
        serialized_value = json.dumps(value)

        if ttl_seconds and ttl_seconds > 0:
            redis.set(key, serialized_value, ex=ttl_seconds)
            logger.debug(
                f"Clave almacenada en Redis: key={key}, TTL={ttl_seconds} segundos."
            )
        else:
            redis.set(key, serialized_value)
            logger.debug(
                f"Clave almacenada en Redis: key={key}, sin expiración."
            )

        return True

    except Exception as e:
        logger.error(f"Error en KV SET para key={key}: {e}")
        return False


# ==========================================================
# GET
# ==========================================================

def kv_get(key: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene un valor desde Redis y lo deserializa.
    """
    if not redis:
        logger.debug("Intento de GET ignorado: Redis no está disponible.")
        return None

    try:
        result = redis.get(key)

        if not result:
            logger.debug(f"Redis GET key={key} → SIN RESULTADO (MISS).")
            return None

        logger.debug(f"Redis GET key={key} → ENCONTRADO (HIT).")
        return json.loads(result)

    except Exception as e:
        logger.error(f"Error en KV GET para key={key}: {e}")
        return None


# ==========================================================
# DELETE
# ==========================================================

def kv_delete(key: str) -> bool:
    """
    Elimina una clave de Redis.
    """
    if not redis:
        logger.debug("Intento de DELETE ignorado: Redis no está disponible.")
        return False

    try:
        redis.delete(key)
        logger.debug(f"Clave eliminada de Redis: key={key}.")
        return True
    except Exception as e:
        logger.error(f"Error en KV DELETE para key={key}: {e}")
        return False