from fastapi import FastAPI, HTTPException, Request
from upstash_redis import Redis
from app.powerbi import router as powerbi_router
import os
import requests
import time
import base64
import json
import logging

# =====================================================
# 🔧 Logging
# =====================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("netsuite-api")

# =====================================================
# 🚀 FastAPI App
# =====================================================
app = FastAPI(
    title="NetSuite → Power BI API",
    version="2.0.0"
)

app.include_router(powerbi_router)


@app.get("/")
def healthcheck():
    return {"status": "ok"}


# =====================================================
# 🌐 Upstash Redis
# =====================================================
UPSTASH_REDIS_URL = os.getenv("UPSTASH_REDIS_URL")
UPSTASH_REDIS_TOKEN = os.getenv("UPSTASH_REDIS_TOKEN")

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
        if ttl_seconds:
            redis.set(key, json.dumps(value), ex=ttl_seconds)
        else:
            redis.set(key, json.dumps(value))
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


# =====================================================
# 🔐 OAuth Cache (Ahora en Redis)
# =====================================================
def get_access_token():
    redis_key = "netsuite_oauth_token"

    # 1️⃣ Intentar desde Redis
    cached = kv_get(redis_key)
    if cached and cached.get("access_token") and cached.get("expires_at") > time.time():
        return cached["access_token"]

    # 2️⃣ Si no existe o expiró → renovar
    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    client_id = os.getenv("NETSUITE_CLIENT_ID")
    client_secret = os.getenv("NETSUITE_CLIENT_SECRET")
    refresh_token = os.getenv("NETSUITE_REFRESH_TOKEN")

    if not all([account_id, client_id, client_secret, refresh_token]):
        raise RuntimeError("Faltan variables de entorno de NetSuite")

    token_url = (
        f"https://{account_id}.suitetalk.api.netsuite.com/"
        "services/rest/auth/oauth2/v2/token"
    )

    basic_auth = base64.b64encode(
        f"{client_id}:{client_secret}".encode()
    ).decode()

    headers = {
        "Authorization": f"Basic {basic_auth}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    response = requests.post(token_url, headers=headers, data=payload, timeout=30)

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "netsuite_oauth_status": response.status_code,
                "netsuite_oauth_error": response.text
            }
        )

    data = response.json()

    access_token = data["access_token"]
    expires_in = int(data.get("expires_in", 1800))

    token_data = {
        "access_token": access_token,
        "expires_at": time.time() + expires_in - 60
    }

    kv_set(redis_key, token_data, ttl_seconds=expires_in)

    return access_token


# =====================================================
# 🔧 Cliente Restlet
# =====================================================
def call_restlet(script_id: str):

    def _call_once():
        access_token = get_access_token()
        account_id = os.getenv("NETSUITE_ACCOUNT_ID")

        url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
        params = {"script": script_id, "deploy": "1"}

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code >= 400:
            try:
                error_data = response.json()
            except json.JSONDecodeError:
                error_data = response.text

            raise HTTPException(
                status_code=502,
                detail={
                    "netsuite_status": response.status_code,
                    "netsuite_error": error_data
                }
            )

        try:
            return response.json()
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=502,
                detail="Respuesta no JSON de NetSuite"
            )

    try:
        return _call_once()
    except HTTPException as e:
        if e.status_code == 502:
            time.sleep(1.5)
            return _call_once()
        raise


# =====================================================
# 📊 Endpoints NetSuite
# =====================================================
@app.get("/netsuite/instalaciones")
def netsuite_instalaciones():
    data = call_restlet("2089")
    return {
        "total_inst_caso": data.get("total_inst_caso", []),
        "relevamiento_posventa": data.get("relevamiento_posventa", []),
        "dias_reales_trabajo": data.get("dias_reales_trabajo", [])
    }


@app.get("/netsuite/facturacion_areas_tecnicas")
def netsuite_facturacion_areas_tecnicas():
    data = call_restlet("2092")
    return {
        "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", [])
    }


@app.get("/netsuite/comercial")
def netsuite_comercial():
    data = call_restlet("2091")
    return {
        "clientes_potenciales": data.get("clientes_potenciales", []),
        "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
    }


# =====================================================
# 🔔 Webhook protegido
# =====================================================
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")


@app.post("/webhook/test")
async def webhook_test(request: Request):

    if WEBHOOK_SECRET:
        if request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()
    kv_set("last_webhook_payload", payload)

    return {"status": "ok", "received": payload}


@app.get("/webhook/test")
def webhook_get():
    stored = kv_get("last_webhook_payload")
    return {"status": "ok", "stored_payload": stored}


# =====================================================
# 🩺 Redis Healthcheck
# =====================================================
@app.get("/health/redis")
def redis_health():
    if not redis:
        return {"redis": "not_configured"}

    try:
        redis.set("healthcheck", "ok", ex=10)
        return {"redis": "ok"}
    except Exception:
        return {"redis": "error"}
