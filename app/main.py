import os
import requests
import time

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
    params = {
        "script": "2089",
        "deploy": "1"
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    start = time.time()
    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=120
    )

    return {
        "http_status": response.status_code,
        "elapsed_seconds": round(time.time() - start, 2),
        "response_size": len(response.text),
        "preview": response.text[:300]
    }
