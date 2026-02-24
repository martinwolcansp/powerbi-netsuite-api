#redis_client.py
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
        logger.info("Redis initialized successfully")
    except Exception as e:
        logger.error(f"Redis initialization error: {e}")
        redis = None
else:
    logger.warning("Redis not configured (missing URL or TOKEN)")


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
        return False

    try:
        serialized_value = json.dumps(value)

        if ttl_seconds and ttl_seconds > 0:
            redis.set(key, serialized_value, ex=ttl_seconds)
        else:
            redis.set(key, serialized_value)

        logger.debug(f"Redis SET key={key} ttl={ttl_seconds}")
        return True

    except Exception as e:
        logger.error(f"KV SET ERROR key={key}: {e}")
        return False


# ==========================================================
# GET
# ==========================================================

def kv_get(key: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene un valor desde Redis y lo deserializa.
    """
    if not redis:
        return None

    try:
        result = redis.get(key)

        if not result:
            logger.debug(f"Redis GET key={key} -> MISS")
            return None

        logger.debug(f"Redis GET key={key} -> HIT")
        return json.loads(result)

    except Exception as e:
        logger.error(f"KV GET ERROR key={key}: {e}")
        return None


# ==========================================================
# DELETE
# ==========================================================

def kv_delete(key: str) -> bool:
    """
    Elimina una clave de Redis.
    """
    if not redis:
        return False

    try:
        redis.delete(key)
        logger.debug(f"Redis DELETE key={key}")
        return True
    except Exception as e:
        logger.error(f"KV DELETE ERROR key={key}: {e}")
        return False