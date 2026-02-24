import json
import logging
from typing import Optional, Dict, Any

from upstash_redis import Redis
from app.config import UPSTASH_REDIS_URL, UPSTASH_REDIS_TOKEN

logger = logging.getLogger("redis")

redis: Optional[Redis] = None

# Inicialización segura
if UPSTASH_REDIS_URL and UPSTASH_REDIS_TOKEN:
    try:
        redis = Redis(
            url=UPSTASH_REDIS_URL,
            token=UPSTASH_REDIS_TOKEN,
        )
        logger.info("Redis initialized successfully")
    except Exception as e:
        logger.error(f"Redis initialization error: {e}")
        redis = None
else:
    logger.warning("Redis not configured (missing URL or TOKEN)")


# ==============================
# SET
# ==============================
def kv_set(key: str, value: Dict[str, Any], ttl_seconds: Optional[int] = None) -> bool:
    """
    Guarda un valor en Redis serializado como JSON.
    :param key: Clave Redis
    :param value: Diccionario a almacenar
    :param ttl_seconds: Tiempo de expiración en segundos (opcional)
    :return: True si fue exitoso
    """
    if not redis:
        logger.warning("Redis not available - SET skipped")
        return False

    try:
        serialized_value = json.dumps(value)

        if ttl_seconds and ttl_seconds > 0:
            redis.set(key, serialized_value, ex=ttl_seconds)
            logger.info(f"Redis SET key={key} ttl={ttl_seconds}s")
        else:
            redis.set(key, serialized_value)
            logger.info(f"Redis SET key={key} (no ttl)")

        return True

    except Exception as e:
        logger.error(f"KV SET ERROR key={key}: {e}")
        return False


# ==============================
# GET
# ==============================
def kv_get(key: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene un valor desde Redis y lo deserializa.
    :param key: Clave Redis
    :return: Diccionario o None si no existe
    """
    if not redis:
        logger.warning("Redis not available - GET skipped")
        return None

    try:
        result = redis.get(key)

        if not result:
            logger.info(f"Redis GET key={key} -> MISS")
            return None

        logger.info(f"Redis GET key={key} -> HIT")
        return json.loads(result)

    except Exception as e:
        logger.error(f"KV GET ERROR key={key}: {e}")
        return None


# ==============================
# DELETE
# ==============================
def kv_delete(key: str) -> bool:
    """
    Elimina una clave de Redis.
    """
    if not redis:
        logger.warning("Redis not available - DELETE skipped")
        return False

    try:
        redis.delete(key)
        logger.info(f"Redis DELETE key={key}")
        return True
    except Exception as e:
        logger.error(f"KV DELETE ERROR key={key}: {e}")
        return False


# ==============================
# DEBUG (opcional)
# ==============================
def kv_exists(key: str) -> bool:
    """
    Verifica si una clave existe.
    """
    if not redis:
        return False

    try:
        return bool(redis.exists(key))
    except Exception:
        return False
