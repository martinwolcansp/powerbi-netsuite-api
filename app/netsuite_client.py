import time
import base64
import requests
import logging
from fastapi import HTTPException
from app.config import (
    NETSUITE_ACCOUNT_ID,
    NETSUITE_CLIENT_ID,
    NETSUITE_CLIENT_SECRET,
    NETSUITE_REFRESH_TOKEN,
)
from app.redis_client import redis, kv_get, kv_set

logger = logging.getLogger("netsuite")

TOKEN_KEY = "netsuite_oauth_token"


# ==========================================
# OAuth
# ==========================================

def _refresh_access_token():
    logger.info("Refreshing NetSuite access token")

    if not redis:
        logger.warning("Redis not available, refreshing without lock")

    TOKEN_LOCK_KEY = "lock:oauth_refresh"

    # Re-check
    cached = kv_get(TOKEN_KEY)
    if cached and cached.get("expires_at", 0) > time.time():
        logger.info("Token already refreshed by another instance")
        return cached["access_token"]

    # Lock distribuido (solo si redis está activo)
    lock_acquired = None
    if redis:
        lock_acquired = redis.set(TOKEN_LOCK_KEY, "1", nx=True, ex=30)

    if redis and lock_acquired != "OK":
        logger.info("Another instance is refreshing token. Waiting...")

        for _ in range(30):
            time.sleep(0.3)
            cached = kv_get(TOKEN_KEY)
            if cached and cached.get("expires_at", 0) > time.time():
                logger.info("Token obtained after waiting")
                return cached["access_token"]

        logger.warning("Lock released but token not found, continuing refresh")

    try:
        token_url = (
            f"https://{NETSUITE_ACCOUNT_ID}.suitetalk.api.netsuite.com/"
            "services/rest/auth/oauth2/v2/token"
        )

        basic_auth = base64.b64encode(
            f"{NETSUITE_CLIENT_ID}:{NETSUITE_CLIENT_SECRET}".encode()
        ).decode()

        headers = {
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": NETSUITE_REFRESH_TOKEN,
        }

        response = requests.post(
            token_url,
            headers=headers,
            data=payload,
            timeout=30
        )

        if response.status_code >= 400:
            logger.error(f"OAuth error {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=502,
                detail={"oauth_error": response.text}
            )

        data = response.json()
        expires_in = int(data.get("expires_in", 1800))

        token_data = {
            "access_token": data["access_token"],
            "expires_at": time.time() + expires_in - 60
        }

        kv_set(TOKEN_KEY, token_data, ttl_seconds=expires_in)
        logger.info("New access token cached")

        return token_data["access_token"]

    finally:
        if redis:
            redis.delete(TOKEN_LOCK_KEY)
            logger.info("OAuth refresh lock released")


def get_access_token():
    cached = kv_get(TOKEN_KEY)

    if cached and cached.get("expires_at", 0) > time.time():
        return cached["access_token"]

    return _refresh_access_token()


# ==========================================
# Restlet Caller
# ==========================================

def call_restlet(script_id: str, deploy_id: str = "1"):
    url = (
        f"https://{NETSUITE_ACCOUNT_ID}"
        ".restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    )

    params = {
        "script": script_id,
        "deploy": deploy_id
    }

    for attempt in range(2):

        access_token = get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        start = time.time()

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=60
        )

        duration = round(time.time() - start, 2)

        logger.info(
            f"NetSuite call script={script_id} "
            f"status={response.status_code} "
            f"duration={duration}s "
            f"attempt={attempt+1}"
        )

        if response.status_code == 401 and attempt == 0:
            logger.warning("401 received, refreshing token and retrying")
            _refresh_access_token()
            continue

        if response.status_code >= 400:
            logger.error(f"NetSuite error: {response.text}")
            raise HTTPException(
                status_code=502,
                detail={
                    "netsuite_status": response.status_code,
                    "netsuite_error": response.text
                }
            )

        return response.json()

    raise HTTPException(status_code=502, detail="NetSuite call failed")


# ==========================================
# Distributed Cache + Lock (Single Flight)
# ==========================================

def call_restlet_with_cache(script_id: str, ttl: int = 300):

    cache_key = f"cache:{script_id}"
    lock_key = f"lock:{script_id}"

    # 1️⃣ Intentar cache primero
    cached = kv_get(cache_key)
    if cached:
        logger.info(f"Cache hit for script {script_id}")
        return cached

    logger.info(f"Cache miss for script {script_id}")

    # Si Redis no está disponible
    if not redis:
        logger.warning("Redis not available, skipping distributed cache")
        return call_restlet(script_id)

    # 2️⃣ Intentar adquirir lock distribuido
    lock_acquired = redis.set(lock_key, "1", nx=True, ex=120)

    # 🔥 CORRECCIÓN CLAVE: Upstash devuelve True/False
    if lock_acquired:
        logger.info(f"Lock acquired for script {script_id}")

        try:
            data = call_restlet(script_id)

            logger.info(f"Saving cache for {cache_key}")
            kv_set(cache_key, data, ttl_seconds=ttl)

            # Verificación adicional
            exists = redis.exists(cache_key)
            logger.info(f"Cache saved? exists={exists}")

            return data

        finally:
            redis.delete(lock_key)
            logger.info(f"Lock released for script {script_id}")

    else:
        logger.info("Another request is active. Waiting...")

        # Esperar hasta que desaparezca el lock
        for _ in range(400):  # máx 120s
            if not redis.exists(lock_key):
                break
            time.sleep(0.3)

        # Intentar leer cache nuevamente
        cached = kv_get(cache_key)
        if cached:
            logger.info(f"Cache available after wait for script {script_id}")
            return cached

        logger.warning("Lock released but no cache found, calling directly")
        return call_restlet(script_id)