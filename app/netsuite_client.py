# netsuite_client.py

# ==========================================================
# Importaciones estándar y dependencias
# ==========================================================
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

# Logger específico del módulo para trazabilidad de OAuth,
# llamadas a NetSuite y comportamiento de cache.
logger = logging.getLogger("netsuite")

# Claves utilizadas en Redis / KV Store
TOKEN_KEY = "netsuite_oauth_token"       # Donde se almacena el token OAuth
TOKEN_LOCK_KEY = "lock:oauth_refresh"    # Lock distribuido para evitar refresh concurrente

# Locks en memoria (por proceso) para evitar llamadas duplicadas
# a un mismo script_id dentro de la misma instancia.
locks = {}  # Locks locales por script_id


# ==========================================================
# Utils
# ==========================================================

def _wait_for_cache_with_backoff(key: str, timeout: int = 120):
    """
    Espera activa con backoff exponencial hasta que una clave
    esté disponible en cache (usado principalmente durante el
    refresh distribuido de OAuth).

    - timeout: tiempo máximo total de espera.
    - delay inicial: 200ms.
    - delay aumenta progresivamente hasta 1 segundo.
    
    Este mecanismo evita:
    - Saturar Redis con consultas constantes.
    - Que múltiples instancias refresquen el token al mismo tiempo.
    """
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
    """
    Utilidad para logging legible.
    Convierte segundos en formato días/horas/minutos/segundos.
    Solo impacta en trazabilidad, no en lógica funcional.
    """
    dias = segundos // 86400
    horas = (segundos % 86400) // 3600
    minutos = (segundos % 3600) // 60
    segs = segundos % 60
    return f"{dias}d {horas}h {minutos}m {segs}s"


# ==========================================================
# OAuth
# ==========================================================

def _request_new_token():
    """
    Solicita un nuevo access_token a NetSuite usando el flujo
    OAuth2 con grant_type=refresh_token.

    Flujo técnico:
    1. Construye Authorization header en Basic Auth (client_id:client_secret).
    2. Envía refresh_token.
    3. Recibe access_token + expires_in.
    4. Guarda token en Redis con:
        - expires_at (timestamp interno)
        - TTL real en Redis
        - margen de seguridad de 60s antes del vencimiento.
    
    El margen de 60 segundos evita errores por desincronización
    de reloj entre servidores.
    """

    logger.info("Solicitando NUEVO token OAuth a NetSuite")

    token_url = (
        f"https://{NETSUITE_ACCOUNT_ID}.suitetalk.api.netsuite.com/"
        "services/rest/auth/oauth2/v2/token"
    )

    # Codificación Base64 requerida por OAuth2 Basic Auth
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

    # Cualquier error OAuth se traduce a 502 para que la API
    # actúe como gateway hacia NetSuite.
    if response.status_code >= 400:
        logger.error(f"Error OAuth {response.status_code}: {response.text}")
        raise HTTPException(status_code=502, detail={"oauth_error": response.text})

    data = response.json()
    expires_in = int(data.get("expires_in", 1800))

    # expires_at se guarda con margen de seguridad
    token_data = {
        "access_token": data["access_token"],
        "expires_at": time.time() + expires_in - 60
    }

    # Persistencia en cache distribuido
    kv_set(TOKEN_KEY, token_data, ttl_seconds=expires_in)

    logger.info(
        "Nuevo token OAuth almacenado en cache. "
        f"Validez aproximada: {_formatear_tiempo_restante(expires_in)} "
        "(se renueva 60s antes de expirar)."
    )

    return token_data["access_token"]


def _refresh_access_token():
    """
    Gestiona la renovación del token considerando:
    - Cache existente
    - Lock distribuido en Redis
    - Espera con backoff si otra instancia está refrescando

    Previene:
    - Thundering herd problem
    - Múltiples refresh simultáneos
    - Sobrecarga innecesaria del endpoint OAuth
    """

    cached = kv_get(TOKEN_KEY)

    # Si el token aún es válido, se reutiliza
    if cached and cached.get("expires_at", 0) > time.time():
        restante = round(cached["expires_at"] - time.time())
        logger.info(
            "Token OAuth aún válido. "
            f"Tiempo restante: {_formatear_tiempo_restante(restante)}"
        )
        return cached["access_token"]

    logger.info("Token OAuth vencido o inexistente. Iniciando refresh.")

    # Si Redis no está disponible, no hay lock distribuido
    # (posible entorno local o fallback).
    if not redis:
        logger.warning("Redis no disponible. Refrescando sin lock distribuido.")
        return _request_new_token()

    # Intento de adquirir lock distribuido
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
        # Liberación del lock distribuido
        redis.delete(TOKEN_LOCK_KEY)
        logger.info("Lock distribuido de OAuth liberado.")


def get_access_token():
    """
    Punto de entrada público para obtener access_token.

    Estrategia:
    1. Reutiliza cache si es válido.
    2. Si no, delega a _refresh_access_token().
    """

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
    """
    Invoca un Restlet de NetSuite de forma sincrónica.

    Características importantes:
    - Usa Bearer Token OAuth.
    - Retry automático UNA vez si recibe 401.
    - Logging de duración y status.
    - Traduce errores >=400 en HTTPException 502.
    """

    url = (
        f"https://{NETSUITE_ACCOUNT_ID}"
        ".restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    )

    params = {"script": script_id, "deploy": deploy_id}

    # Se permiten hasta 2 intentos:
    # - Intento 1 normal
    # - Intento 2 solo si el primero fue 401
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

        # Si el token expiró entre validación y request
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

    # Si ambos intentos fallan
    raise HTTPException(status_code=502, detail="Fallo definitivo al invocar NetSuite")


# ==========================================================
# Cache Distribuido + Lock Local
# ==========================================================

def call_restlet_with_cache(script_id: str, ttl: int = 300):
    """
    Wrapper que agrega:
    - Cache distribuido (Redis)
    - Lock local por proceso
    - Prevención de llamadas duplicadas simultáneas

    Flujo:
    1. Busca en cache distribuido.
    2. Si MISS → adquiere lock local.
    3. Revalida cache (doble chequeo).
    4. Llama a NetSuite.
    5. Guarda resultado en cache con TTL configurable.
    """

    cache_key = f"cache:{script_id}"

    cached = kv_get(cache_key)

    if cached:
        logger.info(f"Cache HIT para script {script_id}")
        return cached

    logger.info(f"Cache MISS para script {script_id}")

    # Inicializa lock local si no existe
    if script_id not in locks:
        locks[script_id] = threading.Lock()

    lock = locks[script_id]

    with lock:

        # Doble verificación luego de adquirir lock
        cached = kv_get(cache_key)
        if cached:
            logger.info(
                f"Cache completada mientras se esperaba lock para script {script_id}"
            )
            return cached

        logger.info(f"Invocando NetSuite para script {script_id}")

        data = _call_restlet_sync(script_id)

        # Persistencia en cache distribuido
        kv_set(cache_key, data, ttl_seconds=ttl)

        logger.info(
            f"Datos almacenados en cache para script {script_id}. "
            f"TTL configurado: {ttl} segundos."
        )

        return data