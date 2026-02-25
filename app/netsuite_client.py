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

locks = {}  # Locks locales por script_id


# ==========================================================
# Utils
# ==========================================================

def _wait_for_cache_with_backoff(key: str, timeout: int = 120):
    inicio = time.time()
    delay = 0.2

    while time.time() - inicio < timeout:
        value = kv_get(key)
        if value:
            return value
        time.sleep(delay)
        delay = min(delay * 1.5, 1.0)

    return None


def _formatear_tiempo_restante(segundos: int) -> str:
    dias = segundos // 86400
    horas = (segundos % 86400) // 3600
    minutos = (segundos % 3600) // 60
    segs = segundos % 60
    return f"{dias}d {horas}h {minutos}m {segs}s"


# ==========================================================
# OAuth
# ==========================================================

def _request_new_token():
    logger.info("Solicitando NUEVO token OAuth a NetSuite")

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
        logger.error(f"Error OAuth {response.status_code}: {response.text}")
        raise HTTPException(status_code=502, detail={"oauth_error": response.text})

    data = response.json()
    expires_in = int(data.get("expires_in", 1800))

    token_data = {
        "access_token": data["access_token"],
        "expires_at": time.time() + expires_in - 60
    }

    kv_set(TOKEN_KEY, token_data, ttl_seconds=expires_in)

    logger.info(
        "Nuevo token OAuth almacenado en cache. "
        f"Validez aproximada: {_formatear_tiempo_restante(expires_in)} "
        "(se renueva 60s antes de expirar)."
    )

    return token_data["access_token"]


def _refresh_access_token():

    cached = kv_get(TOKEN_KEY)

    if cached and cached.get("expires_at", 0) > time.time():
        restante = round(cached["expires_at"] - time.time())
        logger.info(
            "Token OAuth aún válido. "
            f"Tiempo restante: {_formatear_tiempo_restante(restante)}"
        )
        return cached["access_token"]

    logger.info("Token OAuth vencido o inexistente. Iniciando refresh.")

    if not redis:
        logger.warning("Redis no disponible. Refrescando sin lock distribuido.")
        return _request_new_token()

    lock_acquired = redis.set(TOKEN_LOCK_KEY, "1", nx=True, ex=30)

    if not lock_acquired:
        logger.info("Otra instancia está refrescando el token. Esperando...")

        token_data = _wait_for_cache_with_backoff(TOKEN_KEY, timeout=30)

        if token_data and token_data.get("expires_at", 0) > time.time():
            restante = round(token_data["expires_at"] - time.time())
            logger.info(
                "Token obtenido tras espera. "
                f"Tiempo restante: {_formatear_tiempo_restante(restante)}"
            )
            return token_data["access_token"]

        logger.warning("No se obtuvo token tras esperar. Forzando refresh.")

    try:
        return _request_new_token()
    finally:
        redis.delete(TOKEN_LOCK_KEY)
        logger.info("Lock distribuido de OAuth liberado.")


def get_access_token():

    cached = kv_get(TOKEN_KEY)

    if cached and cached.get("expires_at", 0) > time.time():
        restante = round(cached["expires_at"] - time.time())
        logger.info(
            "Reutilizando token OAuth en cache. "
            f"Tiempo restante: {_formatear_tiempo_restante(restante)}"
        )
        return cached["access_token"]

    logger.info("Token no disponible o expirado. Se procederá a refrescar.")
    return _refresh_access_token()


# ==========================================================
# Restlet Sync Caller
# ==========================================================

def _call_restlet_sync(script_id: str, deploy_id: str = "1"):

    url = (
        f"https://{NETSUITE_ACCOUNT_ID}"
        ".restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    )

    params = {"script": script_id, "deploy": deploy_id}

    for attempt in range(2):

        access_token = get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        inicio = time.time()

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=60
        )

        duracion = round(time.time() - inicio, 2)

        logger.info(
            f"Llamada a NetSuite script={script_id} "
            f"status={response.status_code} "
            f"duración={duracion}s "
            f"intento={attempt+1}"
        )

        if response.status_code == 401 and attempt == 0:
            logger.warning("401 recibido. Refrescando token y reintentando.")
            _refresh_access_token()
            continue

        if response.status_code >= 400:
            logger.error(f"Error en NetSuite: {response.text}")
            raise HTTPException(
                status_code=502,
                detail={
                    "netsuite_status": response.status_code,
                    "netsuite_error": response.text
                }
            )

        return response.json()

    raise HTTPException(status_code=502, detail="Fallo definitivo al invocar NetSuite")


# ==========================================================
# Cache Distribuido + Lock Local
# ==========================================================

def call_restlet_with_cache(script_id: str, ttl: int = 300):

    cache_key = f"cache:{script_id}"

    cached = kv_get(cache_key)

    if cached:
        logger.info(f"Cache HIT para script {script_id}")
        return cached

    logger.info(f"Cache MISS para script {script_id}")

    if script_id not in locks:
        locks[script_id] = threading.Lock()

    lock = locks[script_id]

    with lock:

        cached = kv_get(cache_key)
        if cached:
            logger.info(
                f"Cache completada mientras se esperaba lock para script {script_id}"
            )
            return cached

        logger.info(f"Invocando NetSuite para script {script_id}")

        data = _call_restlet_sync(script_id)

        kv_set(cache_key, data, ttl_seconds=ttl)

        logger.info(
            f"Datos almacenados en cache para script {script_id}. "
            f"TTL configurado: {ttl} segundos."
        )

        return data