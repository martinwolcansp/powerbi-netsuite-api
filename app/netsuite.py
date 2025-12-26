import time
import requests
from app.config import (
    NETSUITE_ACCOUNT_ID,
    CLIENT_ID,
    CLIENT_SECRET,
    REFRESH_TOKEN,
)

# Cache simple en memoria
_access_token = None
_expires_at = 0


def get_access_token():
    global _access_token, _expires_at

    # Reutilizar token si sigue válido
    if _access_token and time.time() < _expires_at - 60:
        return _access_token

    token_url = (
        f"https://{NETSUITE_ACCOUNT_ID}"
        ".suitetalk.api.netsuite.com/services/rest/auth/oauth2/v2/token"
    )

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    _access_token = data["access_token"]
    _expires_at = time.time() + data["expires_in"]

    return _access_token


def call_restlet(script_id, deploy_id, method="GET", body=None, params=None):
    token = get_access_token()

    url = (
        f"https://{NETSUITE_ACCOUNT_ID}"
        ".restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    query = {
        "script": script_id,
        "deploy": deploy_id,
    }

    if params:
        query.update(params)

    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        params=query,
        json=body,
        timeout=60,
    )

    response.raise_for_status()

    return response.json() if response.text else None
