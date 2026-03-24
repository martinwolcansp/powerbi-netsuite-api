# powerbi.py
from fastapi import APIRouter
from app.netsuite_client import call_restlet_with_cache
import logging

router = APIRouter(prefix="/powerbi")
logger = logging.getLogger("powerbi")


# ==========================================================
# Endpoints PowerBI
# ==========================================================

@router.get("/instalaciones")
def instalaciones(case_assigned: str | None = None):
    """
    Endpoint para Power BI.
    - case_assigned: parámetro opcional para filtrar casos en NetSuite.
    """

    # Construimos params dinámicos para el Restlet
    params = {}
    if case_assigned:
        params["case_assigned"] = case_assigned

    # Llamada al Restlet con cache
    data = call_restlet_with_cache("2089", ttl=300, params=params)

    logger.info(
        f"PowerBI /instalaciones returned {len(data.get('total_inst_caso', []))} rows"
    )

    return {"rows": data}


@router.get("/facturacion_areas_tecnicas")
def facturacion_areas_tecnicas():
    data = call_restlet_with_cache("2092", ttl=300)
    return {"rows": data}


@router.get("/comercial")
def comercial():
    data = call_restlet_with_cache("2091", ttl=300)
    return {"rows": data}


@router.get("/posventa")
def posventa():
    data = call_restlet_with_cache("2121", ttl=300)
    return {"rows": data}