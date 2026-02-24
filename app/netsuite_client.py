# netsuite_client.py
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
import threading

logger = logging.getLogger("netsuite")

TOKEN_KEY = "netsuite_oauth_token"
TOKEN_LOCK_KEY = "lock:oauth_refresh"

locks = {}  # Locks por script_id para llamadas concurrentes


# ==========================================================
# Utils
# ==========================================================

def _wait_for_cache_with_backoff(key: str, timeout: int = 120):
    """Espera progresiva hasta que exista el key en Redis"""
    start = time.time()
    delay = 0.2
    while time.time() - start < timeout:
        value = kv_get(key)
        if value:
            return value
        time.sleep(delay)
        delay = min(delay * 1.5, 1.0)
    return None


# ==========================================================
# OAuth
# ==========================================================

def _request_new_token():
    """Pide un nuevo token OAuth a NetSuite"""
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

    response = requests.post(token_url, headers=headers, data=payload, timeout=30)

    if response.status_code >= 400:
        logger.error(f"OAuth error {response.status_code}: {response.text}")
        raise HTTPException(status_code=502, detail={"oauth_error": response.text})

    data = response.json()
    expires_in = int(data.get("expires_in", 1800))

    token_data = {
        "access_token": data["access_token"],
        "expires_at": time.time() + expires_in - 60
    }

    kv_set(TOKEN_KEY, token_data, ttl_seconds=expires_in)
    logger.info("New access token cached")
    return token_data["access_token"]


def _refresh_access_token():
    """Refresca token usando lock distribuido"""
    cached = kv_get(TOKEN_KEY)
    if cached and cached.get("expires_at", 0) > time.time():
        return cached["access_token"]

    if not redis:
        logger.warning("Redis no disponible, refrescando sin lock")
        return _request_new_token()

    lock_acquired = redis.set(TOKEN_LOCK_KEY, "1", nx=True, ex=30)
    if not lock_acquired:
        logger.info("Otra instancia refresca token, esperando...")
        token_data = _wait_for_cache_with_backoff(TOKEN_KEY, timeout=30)
        if token_data and token_data.get("expires_at", 0) > time.time():
            return token_data["access_token"]
        logger.warning("Token no disponible después de esperar, refrescando")

    try:
        return _request_new_token()
    finally:
        redis.delete(TOKEN_LOCK_KEY)
        logger.info("Lock de OAuth liberado")


def get_access_token():
    cached = kv_get(TOKEN_KEY)
    if cached and cached.get("expires_at", 0) > time.time():
        return cached["access_token"]
    return _refresh_access_token()


# ==========================================================
# Restlet Sync Caller
# ==========================================================

def _call_restlet_sync(script_id: str, deploy_id: str = "1"):
    """Llamada síncrona a NetSuite"""
    url = (
        f"https://{NETSUITE_ACCOUNT_ID}"
        ".restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    )

    params = {"script": script_id, "deploy": deploy_id}

    for attempt in range(2):
        access_token = get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        start = time.time()

        response = requests.get(url, headers=headers, params=params, timeout=60)

        duration = round(time.time() - start, 2)
        logger.debug(
            f"NetSuite call script={script_id} status={response.status_code} "
            f"duration={duration}s attempt={attempt+1}"
        )

        if response.status_code == 401 and attempt == 0:
            logger.warning("401 recibido, refrescando token y reintentando")
            _refresh_access_token()
            continue

        if response.status_code >= 400:
            logger.error(f"NetSuite error: {response.text}")
            raise HTTPException(
                status_code=502,
                detail={"netsuite_status": response.status_code, "netsuite_error": response.text}
            )

        return response.json()

    raise HTTPException(status_code=502, detail="NetSuite call failed")


# ==========================================================
# Cache Distribuido + Lock
# ==========================================================

def call_restlet_with_cache(script_id: str, ttl: int = 300):
    """Cache Redis + lock threading"""
    cache_key = f"cache:{script_id}"
    lock_key = f"lock:{script_id}"

    cached = kv_get(cache_key)
    if cached:
        logger.info(f"Cache hit for script {script_id}")
        return cached

    logger.info(f"Cache miss for script {script_id}")

    # Lock threading local por script_id
    if script_id not in locks:
        locks[script_id] = threading.Lock()

    lock = locks[script_id]

    with lock:
        # Verificar cache nuevamente dentro del lock
        cached = kv_get(cache_key)
        if cached:
            logger.info(f"Cache filled during wait for script {script_id}")
            return cached

        # Llamada real
        data = _call_restlet_sync(script_id)
        kv_set(cache_key, data, ttl_seconds=ttl)
        logger.info(f"Cache saved for script {script_id}")
        return data