# powerbi.py
from fastapi import APIRouter, Header, HTTPException
from app.config import POWERBI_API_KEY
from app.netsuite_client import call_restlet_with_cache
import logging

router = APIRouter(prefix="/powerbi")
logger = logging.getLogger("powerbi")


@router.get("/instalaciones")
def instalaciones(x_api_key: str = Header(...)):
    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    data = call_restlet_with_cache("2089", ttl=300)
    logger.info(f"PowerBI /instalaciones returned {len(data.get('total_inst_caso', []))} rows")
    
    return {
        "rows": data
    }


@router.get("/facturacion_areas_tecnicas")
def facturacion(x_api_key: str = Header(...)):
    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    data = call_restlet_with_cache("2092", ttl=300)
    logger.info(f"PowerBI /facturacion_areas_tecnicas returned {len(data.get('facturacion_areas_tecnicas', []))} rows")

    return {
        "rows": data
    }


@router.get("/comercial")
def comercial(x_api_key: str = Header(...)):
    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    data = call_restlet_with_cache("2091", ttl=300)
    logger.info(f"PowerBI /comercial returned {len(data.get('clientes_potenciales', []))} clientes potenciales")

    return {
        "rows": data
    }


@router.get("/posventa")
def posventa(x_api_key: str = Header(...)):

    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    data = call_restlet_with_cache("2095", ttl=300)

    logger.info(
        f"PowerBI /posventa returned {len(data.get('relev_posventa', []))} rows"
    )

    return {
        "rows": data
    }