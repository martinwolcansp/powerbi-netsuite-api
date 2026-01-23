from fastapi import FastAPI, HTTPException
from app.powerbi import router as powerbi_router
import os
import requests
import time
import base64
import json

# ==============================
# 1️⃣ Crear la app FastAPI
# ==============================
app = FastAPI(
    title="NetSuite → Power BI API",
    version="1.1.2"
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

def get_access_token():
    now = time.time()

    # Token válido en cache
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
    expires_in = int(data.get("expires_in", 1800))  # soporta str o int

    # Cache con margen de seguridad
    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = now + expires_in - 60

    return access_token

    # ==============================
    # 🔧 Cliente Restlet NetSuite
    # ==============================
    def call_restlet(script_id: str):
    """
    Llama a un Restlet de NetSuite con:
    - manejo explícito de errores NetSuite
    - 1 retry con backoff leve para fallos transitorios
    """

    def _call_once():
        access_token = get_access_token()
        account_id = os.getenv("NETSUITE_ACCOUNT_ID")

        url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
        params = {
            "script": script_id,
            "deploy": "1"
        }

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

        # Caso especial: rate limit NetSuite
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

        # Cualquier otro error HTTP de NetSuite
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

        if not response.text:
            raise HTTPException(
                status_code=502,
                detail="Respuesta vacía de NetSuite"
            )

        try:
            data = response.json()
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=502,
                detail=f"Respuesta no JSON de NetSuite: {response.text[:200]}"
            )

        if not isinstance(data, dict):
            raise HTTPException(
                status_code=502,
                detail="Respuesta inesperada de NetSuite"
            )

        return data

        # ==============================
        # Retry controlado (1 intento extra)
        # ==============================
        try:
            return _call_once()
        except HTTPException as e:
            if e.status_code == 502:
                time.sleep(1.5)  # backoff leve
                return _call_once()
            raise

    # ==============================
    # Retry controlado (1 intento extra)
    # ==============================
    try:
        return _call_once()
    except HTTPException as e:
        # Solo reintentamos errores técnicos transitorios
        if e.status_code == 502:
            time.sleep(1.5)  # backoff leve
            return _call_once()
        raise


# ==============================
# 5️⃣ Endpoint Instalaciones
# ==============================
@app.get("/netsuite/instalaciones")
def netsuite_instalaciones():
    data = call_restlet("2089")
    return {
        "total_inst_caso": data.get("total_inst_caso", []),
        "relevamiento_posventa": data.get("relevamiento_posventa", []),
        "dias_reales_trabajo": data.get("dias_reales_trabajo", [])
    }

# ==============================
# 6️⃣ Facturación Áreas Técnicas
# ==============================
@app.get("/netsuite/facturacion_areas_tecnicas")
def netsuite_facturacion_areas_tecnicas():
    data = call_restlet("2092")
    return {
        "facturacion_areas_tecnicas": data.get(
            "facturacion_areas_tecnicas", []
        )
    }

# ==============================
# 7️⃣ Comercial
# ==============================
@app.get("/netsuite/comercial")
def netsuite_comercial():
    data = call_restlet("2091")
    return {
        "clientes_potenciales": data.get("clientes_potenciales", []),
        "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
    }
