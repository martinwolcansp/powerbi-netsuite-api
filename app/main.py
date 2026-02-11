from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from app.powerbi import router as powerbi_router
import os
import requests
import time
import base64
import json

# =====================================================
# 🚀 FastAPI App
# =====================================================
app = FastAPI(
    title="NetSuite → Power BI API",
    version="1.2.0"
)

app.include_router(powerbi_router)


@app.get("/")
def healthcheck():
    return {"status": "ok"}


# =====================================================
# 🌐 Upstash Redis (KV externo opcional)
# =====================================================
UPSTASH_REDIS_URL = os.getenv("UPSTASH_REDIS_URL")
UPSTASH_REDIS_TOKEN = os.getenv("UPSTASH_REDIS_TOKEN")


def kv_set(key: str, value: dict, ttl_seconds: int = 3600):
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return

    url = f"{UPSTASH_REDIS_URL}/set/{key}"
    headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
    data = {
        "value": json.dumps(value),
        "ttl": ttl_seconds
    }

    try:
        requests.post(url, headers=headers, json=data, timeout=5)
    except Exception as e:
        print("KV SET ERROR:", e)


def kv_get(key: str):
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None

    url = f"{UPSTASH_REDIS_URL}/get/{key}"
    headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}

    try:
        r = requests.get(url, headers=headers, timeout=5)

        if r.status_code != 200:
            return None

        result = r.json().get("result")
        return json.loads(result) if result else None

    except Exception as e:
        print("KV GET ERROR:", e)
        return None


# =====================================================
# 🔐 OAuth Cache en memoria
# =====================================================
_token_cache = {
    "access_token": None,
    "expires_at": 0
}


def get_access_token():
    now = time.time()

    # Token válido en memoria
    if _token_cache["access_token"] and now < _token_cache["expires_at"]:
        return _token_cache["access_token"]

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

    response = requests.post(
        token_url,
        headers=headers,
        data=payload,
        timeout=30
    )

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

    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = now + expires_in - 60

    return access_token


# =====================================================
# 🔧 Cliente Restlet NetSuite
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

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )

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

    # Retry controlado
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
# 🔔 Webhook Test con Redis
# =====================================================
@app.api_route("/webhook/test", methods=["POST", "GET"])
async def webhook_test(request: Request):

    if request.method == "GET":
        stored = kv_get("last_webhook_payload")
        return {
            "status": "ok",
            "stored_payload": stored
        }

    payload = await request.json()

    print("WEBHOOK RECEIVED >>>")
    print(payload)

    kv_set("last_webhook_payload", payload)

    return {
        "status": "ok",
        "received": payload
    }
