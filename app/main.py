# /app/main.py
from fastapi import FastAPI
from app.powerbi import router as powerbi_router
import os
import requests
import time

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
# 4️⃣ Healthcheck NetSuite (prueba rápida)
# ==============================
@app.get("/health/netsuite")
def health_netsuite():
    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    access_token = os.getenv("NETSUITE_ACCESS_TOKEN")

    if not account_id or not access_token:
        return {
            "error": "Variables de entorno faltantes",
            "account_id": bool(account_id),
            "access_token": bool(access_token)
        }

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
# 5️⃣ Endpoint principal: tres listas separadas
# ==============================
@app.get("/netsuite/data")
def netsuite_data():
    account_id = os.getenv("NETSUITE_ACCOUNT_ID")
    access_token = os.getenv("NETSUITE_ACCESS_TOKEN")

    if not account_id or not access_token:
        return {"error": "Variables de entorno faltantes"}

    url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    params = {"script": "2089", "deploy": "1"}
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()  # JSON completo del RESTlet
        # Retornar solo las tres listas separadas
        return {
            "total_inst_caso": data.get("total_inst_caso", []),
            "relevamiento_posventa": data.get("relevamiento_posventa", []),
            "dias_reales_trabajo": data.get("dias_reales_trabajo", [])
        }
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}
