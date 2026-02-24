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
TOKEN_LOCK_KEY = "lock:oauth_refresh"


# ==========================================================
# Utils
# ==========================================================

def _wait_for_cache_with_backoff(key: str, timeout: int = 120):
    """
    Espera progresiva hasta que exista el key en Redis.
    Reduce el número de requests innecesarios.
    """
    start = time.time()
    delay = 0.2  # 200ms inicial

    while time.time() - start < timeout:
        value = kv_get(key)
        if value:
            return value

        time.sleep(delay)
        delay = min(delay * 1.5, 1.0)  # máximo 1 segundo

    return None


# ==========================================================
# OAuth
# ==========================================================

def _refresh_access_token():
    logger.info("Refreshing NetSuite access token")

    # Re-check por si otra instancia ya lo refrescó
    cached = kv_get(TOKEN_KEY)
    if cached and cached.get("expires_at", 0) > time.time():
        logger.info("Token already refreshed by another instance")
        return cached["access_token"]

    # Si no hay Redis, refrescar sin lock distribuido
    if not redis:
        logger.warning("Redis not available, refreshing without lock")
        return _request_new_token()

    # Intentar lock distribuido
    lock_acquired = redis.set(TOKEN_LOCK_KEY, "1", nx=True, ex=30)

    if not lock_acquired:
        logger.info("Another instance is refreshing token. Waiting with backoff...")

        token_data = _wait_for_cache_with_backoff(TOKEN_KEY, timeout=30)

        if token_data and token_data.get("expires_at", 0) > time.time():
            logger.info("Token obtained after waiting")
            return token_data["access_token"]

        logger.warning("Token not found after waiting, continuing refresh")

    try:
        return _request_new_token()
    finally:
        redis.delete(TOKEN_LOCK_KEY)
        logger.info("OAuth refresh lock released")


def _request_new_token():
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


def get_access_token():
    cached = kv_get(TOKEN_KEY)

    if cached and cached.get("expires_at", 0) > time.time():
        return cached["access_token"]

    return _refresh_access_token()


# ==========================================================
# Restlet Caller
# ==========================================================

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
            f"attempt={attempt + 1}"
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


# ==========================================================
# Distributed Cache + Lock (Single Flight)
# ==========================================================

def call_restlet_with_cache(script_id: str, ttl: int = 300):

    cache_key = f"cache:{script_id}"
    lock_key = f"lock:{script_id}"

    # 1️⃣ Intentar cache
    cached = kv_get(cache_key)
    if cached:
        logger.info(f"Cache hit for script {script_id}")
        return cached

    logger.info(f"Cache miss for script {script_id}")

    # Si no hay Redis → fallback directo
    if not redis:
        logger.warning("Redis not available, skipping distributed cache")
        return call_restlet(script_id)

    # 2️⃣ Intentar lock distribuido
    lock_acquired = redis.set(lock_key, "1", nx=True, ex=120)

    if lock_acquired:
        logger.info(f"Lock acquired for script {script_id}")

        try:
            data = call_restlet(script_id)
            kv_set(cache_key, data, ttl_seconds=ttl)
            logger.info(f"Cache saved for script {script_id}")
            return data

        finally:
            redis.delete(lock_key)
            logger.info(f"Lock released for script {script_id}")

    # 3️⃣ Si otro proceso tiene el lock → esperar cache con backoff
    logger.info("Another request is active. Waiting with backoff...")

    cached = _wait_for_cache_with_backoff(cache_key, timeout=120)

    if cached:
        logger.info(f"Cache available after wait for script {script_id}")
        return cached

    logger.warning("Timeout waiting for cache. Calling directly.")
    return call_restlet(script_id)