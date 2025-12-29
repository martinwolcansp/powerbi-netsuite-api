# /app/main.py
from fastapi import FastAPI
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
# 4️⃣ Función para obtener access_token vía refresh_token (OAuth2 v2)
# ==============================
def get_access_token():
    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    client_id = os.getenv("NETSUITE_CLIENT_ID")
    client_secret = os.getenv("NETSUITE_CLIENT_SECRET")
    refresh_token = os.getenv("NETSUITE_REFRESH_TOKEN")

    if not all([account_id, client_id, client_secret, refresh_token]):
        raise RuntimeError("Faltan variables NETSUITE_ACCOUNT_ID, CLIENT_ID, CLIENT_SECRET o REFRESH_TOKEN")

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
# 5️⃣ Healthcheck NetSuite (prueba rápida)
# ==============================
@app.get("/health/netsuite")
def health_netsuite():
    try:
        access_token = get_access_token()
    except Exception as e:
        return {"error": str(e)}

    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    params = {"script": "2089", "deploy": "1"}
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    start = time.time()
    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        return {
            "http_status": response.status_code,
            "elapsed_seconds": round(time.time() - start, 2),
            "response_size": len(response.text),
            "preview": response.text[:300]
        }
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

# ==============================
# 6️⃣ Endpoint Instalaciones
# ==============================
@app.get("/netsuite/instalaciones")
def netsuite_data():
    try:
        access_token = get_access_token()
    except Exception as e:
        return {"error": str(e)}

    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    params = {"script": "2089", "deploy": "1"}
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()  # JSON completo del RESTlet
        return {
            "total_inst_caso": data.get("total_inst_caso", []),
            "relevamiento_posventa": data.get("relevamiento_posventa", []),
            "dias_reales_trabajo": data.get("dias_reales_trabajo", [])
        }
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

# ==============================
# 6️⃣ Endpoint Facturacion Areas Técnicas
# ==============================
@app.get("/netsuite/facturacion_areas_tecnicas")
def netsuite_data():
    try:
        access_token = get_access_token()
    except Exception as e:
        return {"error": str(e)}

    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    params = {"script": "2092", "deploy": "1"}
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()  # JSON completo del RESTlet
        return {
            "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", []),
        }
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


# ==============================
# 6️⃣ Endpoint Comercial
# ==============================
@app.get("/netsuite/comercial")
def netsuite_data():
    try:
        access_token = get_access_token()
    except Exception as e:
        return {"error": str(e)}

    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    params = {"script": "2091", "deploy": "1"}
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()  # JSON completo del RESTlet
        return {
            "clientes_potenciales": data.get("clientes_potenciales", []),
            "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
        }
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}