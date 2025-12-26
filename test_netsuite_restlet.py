import os
import time
import requests

ACCOUNT_ID = os.getenv("NETSUITE_ACCOUNT_ID")
ACCESS_TOKEN = os.getenv("NETSUITE_ACCESS_TOKEN")

if not ACCOUNT_ID or not ACCESS_TOKEN:
    raise RuntimeError(
        "Faltan variables NETSUITE_ACCOUNT_ID o NETSUITE_ACCESS_TOKEN"
    )

RESTLET_URL = (
    f"https://{ACCOUNT_ID}.restlets.api.netsuite.com/"
    "app/site/hosting/restlet.nl"
)

params = {
    "script": "2089",
    "deploy": "1"
}

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

print("🔄 Probando RESTlet con OAuth 2.0...")

start = time.time()

try:
    response = requests.get(
        RESTLET_URL,
        headers=headers,
        params=params,
        timeout=120
    )

    elapsed = round(time.time() - start, 2)

    print(f"⏱ Tiempo: {elapsed}s")
    print(f"📡 Status: {response.status_code}")
    print(response.text[:500])

except requests.exceptions.Timeout:
    print("⏰ Timeout: NetSuite no respondió")
