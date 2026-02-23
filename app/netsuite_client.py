import time
import base64
import requests
import logging
from fastapi import HTTPException
from app.config import (
    NETSUITE_ACCOUNT_ID,
    NETSUITE_CLIENT_ID,
    NETSUITE_CLIENT_SECRET,
    NETSUITE_REFRESH_TOKEN,
)
from app.redis_client import kv_get, kv_set

logger = logging.getLogger("netsuite")

TOKEN_KEY = "netsuite_oauth_token"

# ==========================================
# OAuth
# ==========================================

def _refresh_access_token():
    logger.info("Refreshing NetSuite access token")

    token_url = (
        f"https://{NETSUITE_ACCOUNT_ID}.suitetalk.api.netsuite.com/"
        "services/rest/auth/oauth2/v2/token"
    )

    basic_auth = base64.b64encode(
        f"{NETSUITE_CLIENT_ID}:{NETSUITE_CLIENT_SECRET}".encode()
    ).decode()

    headers = {
        "Authorization": f"Basic {basic_auth}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": NETSUITE_REFRESH_TOKEN,
    }

    response = requests.post(token_url, headers=headers, data=payload, timeout=30)

    if response.status_code >= 400:
        logger.error(f"OAuth error {response.status_code}: {response.text}")
        raise HTTPException(
            status_code=502,
            detail={"oauth_error": response.text}
        )

    data = response.json()

    token_data = {
        "access_token": data["access_token"],
        "expires_at": time.time() + int(data.get("expires_in", 1800)) - 60
    }

    kv_set(TOKEN_KEY, token_data, ttl_seconds=int(data.get("expires_in", 1800)))

    logger.info("New access token cached")

    return token_data["access_token"]


def get_access_token():
    cached = kv_get(TOKEN_KEY)

    if cached and cached.get("expires_at") > time.time():
        return cached["access_token"]

    return _refresh_access_token()


# ==========================================
# Restlet Caller
# ==========================================

def call_restlet(script_id: str, deploy_id: str = "1"):
    url = (
        f"https://{NETSUITE_ACCOUNT_ID}"
        ".restlets.api.netsuite.com/app/site/hosting/restlet.nl"
    )

    params = {
        "script": script_id,
        "deploy": deploy_id
    }

    for attempt in range(2):

        access_token = get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        start = time.time()

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=60
        )

        duration = round(time.time() - start, 2)

        logger.info(
            f"NetSuite call script={script_id} "
            f"status={response.status_code} "
            f"duration={duration}s "
            f"attempt={attempt+1}"
        )

        # Token vencido
        if response.status_code == 401 and attempt == 0:
            logger.warning("401 received, refreshing token and retrying")
            _refresh_access_token()
            continue

        if response.status_code >= 400:
            logger.error(f"NetSuite error: {response.text}")
            raise HTTPException(
                status_code=502,
                detail={
                    "netsuite_status": response.status_code,
                    "netsuite_error": response.text
                }
            )

        return response.json()

    raise HTTPException(status_code=502, detail="NetSuite call failed")