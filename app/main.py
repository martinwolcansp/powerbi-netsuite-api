from fastapi import FastAPI
import logging
import time

from app.routers import netsuite, powerbi
from app.redis_client import redis, kv_set, kv_get

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

app = FastAPI(
    title="NetSuite → Power BI API",
    version="3.0.0"
)

# Routers principales
app.include_router(netsuite.router)
app.include_router(powerbi.router)


@app.get("/")
def healthcheck():
    return {"status": "ok", "version": "3.0.0"}


# ==========================================
# DEBUG REDIS TEST
# ==========================================

@app.get("/debug/redis-test")
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