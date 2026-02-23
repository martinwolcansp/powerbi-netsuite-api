import json
import logging
from upstash_redis import Redis
from app.config import UPSTASH_REDIS_URL, UPSTASH_REDIS_TOKEN

logger = logging.getLogger("redis")

redis = None

if UPSTASH_REDIS_URL and UPSTASH_REDIS_TOKEN:
    redis = Redis(url=UPSTASH_REDIS_URL, token=UPSTASH_REDIS_TOKEN)
    logger.info("Redis initialized")
else:
    logger.warning("Redis not configured")


def kv_set(key: str, value: dict, ttl_seconds: int = None):
    if not redis:
        return
    try:
        redis.set(key, json.dumps(value), ex=ttl_seconds)
    except Exception as e:
        logger.error(f"KV SET ERROR: {e}")


def kv_get(key: str):
    if not redis:
        return None
    try:
        result = redis.get(key)
        return json.loads(result) if result else None
    except Exception as e:
        logger.error(f"KV GET ERROR: {e}")
        return None