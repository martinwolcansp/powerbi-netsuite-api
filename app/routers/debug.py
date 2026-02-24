from fastapi import APIRouter
import time
from app.redis_client import redis, kv_set, kv_get

router = APIRouter(prefix="/debug")

@router.get("/redis-test")
def redis_test():

    if not redis:
        return {"error": "Redis not configured"}

    test_key = "debug:test_key"

    payload = {
        "message": "redis funcionando",
        "timestamp": time.time()
    }

    kv_set(test_key, payload, ttl_seconds=60)

    value = kv_get(test_key)
    ttl = redis.ttl(test_key)

    return {
        "saved": payload,
        "read": value,
        "ttl_seconds_remaining": ttl
    }