# app/routers/powerbi.py
from fastapi import APIRouter, Header, HTTPException
from app.config import POWERBI_API_KEY
from app.netsuite_client import call_restlet_with_cache
import logging

router = APIRouter(prefix="/powerbi")
logger = logging.getLogger("powerbi")
logging.basicConfig(level=logging.INFO)


@router.get("/instalaciones")
def instalaciones(x_api_key: str = Header(...)):
    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    data = call_restlet_with_cache("2089", ttl=300)

    logger.info(f"/powerbi/instalaciones called | rows={len(data.get('total_inst_caso', []))}")

    return {
        "rows": data
    }


@router.get("/webhook/test")
def webhook_test(x_api_key: str = Header(...), test: bool = False):
    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    payload = {"status": "ok"}
    if test:
        payload["stored_payload"] = {"event": "upstash_funciona"}

    logger.info(f"/powerbi/webhook/test called | payload={payload}")
    return payload