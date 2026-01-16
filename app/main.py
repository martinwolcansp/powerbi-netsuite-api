# /app/main.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from app.powerbi import router as powerbi_router
import os
import requests
import time
import base64

# ==============================
# 1️⃣ Crear la app FastAPI
# ==============================
app = FastAPI(
    title="NetSuite → Power BI API",
    version="1.0.0"
)

# ==============================
# 2️⃣ Incluir router de Power BI
# ==============================
app.include_router(powerbi_router)

# ==============================
# 3️⃣ Healthcheck básico
# ==============================
@app.get("/")
def healthcheck():
    return {"status": "ok"}

# ==============================
# 4️⃣ OAuth2 – Obtener access_token
# ==============================
def get_access_token():
    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    client_id = os.getenv("NETSUITE_CLIENT_ID")
    client_secret = os.getenv("NETSUITE_CLIENT_SECRET")
    refresh_token = os.getenv("NETSUITE_REFRESH_TOKEN")

    if not all([account_id, client_id, client_secret, refresh_token]):
        raise RuntimeError("Faltan variables de entorno de NetSuite")

    token_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v2/token"
    basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

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
    if "access_token" not in data:
        raise RuntimeError(f"No se recibió access_token: {data}")

    return data["access_token"]

# ==============================
# Helper común para llamar a Restlet
# ==============================
def call_restlet(script_id: str):
    access_token = get_access_token()
    account_id = os.getenv("NETSUITE_ACCOUNT_ID")

    url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    params = {"script": script_id, "deploy": "1"}
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, params=params, timeout=120)

    # 🔎 LOG CRÍTICO
    print("NETSUITE STATUS:", response.status_code)
    print("NETSUITE HEADERS:", response.headers)
    print("NETSUITE RAW RESPONSE:")
    print(response.text)

    response.raise_for_status()

    try:
        return response.json()
    except ValueError:
        raise RuntimeError("NetSuite no devolvió JSON válido")

# ==============================
# 5️⃣ Endpoint Instalaciones
# ==============================
@app.get("/netsuite/instalaciones")
def netsuite_instalaciones():
    try:
        data = call_restlet("2089")

        return JSONResponse(
            content={
                "total_inst_caso": data.get("total_inst_caso", []),
                "relevamiento_posventa": data.get("relevamiento_posventa", []),
                "dias_reales_trabajo": data.get("dias_reales_trabajo", [])
            },
            media_type="application/json"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

# ==============================
# 6️⃣ Endpoint Facturación Áreas Técnicas
# ==============================
@app.get("/netsuite/facturacion_areas_tecnicas")
def netsuite_facturacion_areas_tecnicas():
    try:
        data = call_restlet("2092")

        return JSONResponse(
            content={
                "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", [])
            },
            media_type="application/json"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

# ==============================
# 7️⃣ Endpoint Comercial
# ==============================
@app.get("/netsuite/comercial")
def netsuite_comercial():
    try:
        data = call_restlet("2091")

        return JSONResponse(
            content={
                "clientes_potenciales": data.get("clientes_potenciales", []),
                "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
            },
            media_type="application/json"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
