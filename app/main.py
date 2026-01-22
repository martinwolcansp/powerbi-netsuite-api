# /app/main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from app.powerbi import router as powerbi_router
import os
import requests
import time
import base64
import json
import threading

# ==============================
# 1️⃣ Crear la app FastAPI
# ==============================
app = FastAPI(
    title="NetSuite → Power BI API",
    version="1.1.0"
)

app.include_router(powerbi_router)

@app.get("/")
def healthcheck():
    return {"status": "ok"}

# ==============================
# 🔐 OAuth cache en memoria
# ==============================
_token_cache = {
    "access_token": None,
    "expires_at": 0
}

_token_lock = threading.Lock()
_netsuite_lock = threading.Lock()

def get_access_token():
    with _token_lock:
        now = time.time()

        # ✅ Token válido en cache
        if _token_cache["access_token"] and now < _token_cache["expires_at"]:
            return _token_cache["access_token"]

        account_id = os.getenv("NETSUITE_ACCOUNT_ID")
        client_id = os.getenv("NETSUITE_CLIENT_ID")
        client_secret = os.getenv("NETSUITE_CLIENT_SECRET")
        refresh_token = os.getenv("NETSUITE_REFRESH_TOKEN")

        if not all([account_id, client_id, client_secret, refresh_token]):
            raise RuntimeError("Faltan variables de entorno de NetSuite")

        token_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v2/token"
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
        response.raise_for_status()

        data = response.json()

        access_token = data["access_token"]
        expires_in = data.get("expires_in", 1800)  # default 30 min

        # 🧠 guardamos token con margen de seguridad
        _token_cache["access_token"] = access_token
        _token_cache["expires_at"] = now + expires_in - 60

        return access_token

# ==============================
# 🔧 Helper robusto para Restlets
# ==============================
def call_restlet(script_id: str):
    with _netsuite_lock:  # 🚦 evita concurrencia
        access_token = get_access_token()
        account_id = os.getenv("NETSUITE_ACCOUNT_ID")

        url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
        params = {"script": script_id, "deploy": "1"}
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)

        print("NETSUITE STATUS:", response.status_code)
        print("NETSUITE RAW RESPONSE:")
        print(response.text)

        # 🚨 Manejo explícito de límite NetSuite
        if response.status_code == 400:
            try:
                data = response.json()
                if data.get("error", {}).get("code") == "SSS_REQUEST_LIMIT_EXCEEDED":
                    raise HTTPException(
                        status_code=429,
                        detail="NetSuite request limit exceeded"
                    )
            except json.JSONDecodeError:
                pass

        response.raise_for_status()

       raw = response.text.strip()

        # 🔒 FIX INMEDIATO: parseo robusto
        if not raw:
            raise HTTPException(
                status_code=502,
                detail="Respuesta vacía de NetSuite"
            )

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=502,
                detail=f"Respuesta no JSON de NetSuite: {raw[:200]}"
            )

        if not isinstance(data, dict):
            raise HTTPException(
                status_code=502,
                detail="Respuesta inesperada de NetSuite (no es un objeto JSON)"
            )

        return data


# ==============================
# 5️⃣ Endpoint Instalaciones
# ==============================
@app.get("/netsuite/instalaciones")
def netsuite_instalaciones():
    try:
        data = call_restlet("2089")
        return {
            "total_inst_caso": data.get("total_inst_caso", []),
            "relevamiento_posventa": data.get("relevamiento_posventa", []),
            "dias_reales_trabajo": data.get("dias_reales_trabajo", [])
        }

    except HTTPException as e:
        raise e

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==============================
# 6️⃣ Facturación Áreas Técnicas
# ==============================
@app.get("/netsuite/facturacion_areas_tecnicas")
def netsuite_facturacion_areas_tecnicas():
    try:
        data = call_restlet("2092")
        return {
            "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", [])
        }

    except HTTPException as e:
        raise e

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==============================
# 7️⃣ Comercial
# ==============================
@app.get("/netsuite/comercial")
def netsuite_comercial():
    try:
        data = call_restlet("2091")
        return {
            "clientes_potenciales": data.get("clientes_potenciales", []),
            "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
        }

    except HTTPException as e:
        raise e

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
