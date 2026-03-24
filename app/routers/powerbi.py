# netsuite.py

# ==========================================================
# Importaciones
# ==========================================================
from fastapi import APIRouter
from app.netsuite_client import call_restlet_with_cache
import logging

# Router con prefijo /netsuite
router = APIRouter(prefix="/netsuite")
logger = logging.getLogger("netsuite")


# ==========================================================
# ENDPOINT: INSTALACIONES (con parámetro dinámico)
# ==========================================================

@router.get("/instalaciones")
def instalaciones(case_assigned: str | None = None):
    """
    Endpoint principal para Power BI.

    Permite filtrar por:
    - case_assigned (opcional)

    Ejemplos:
    /netsuite/instalaciones
    /netsuite/instalaciones?case_assigned=123
    """

    # Construcción de parámetros dinámicos
    params = {}
    if case_assigned:
        params["case_assigned"] = case_assigned

    # Llamada a NetSuite con cache
    data = call_restlet_with_cache(
        "2089",
        ttl=300,
        params=params
    )

    logger.info(
        f"/netsuite/instalaciones → rows={len(data.get('total_inst_caso', []))} "
        f"params={params}"
    )

    return {
        "rows": data
    }


# ==========================================================
# ENDPOINT: FACTURACIÓN ÁREAS TÉCNICAS
# ==========================================================

@router.get("/facturacion_areas_tecnicas")
def facturacion_areas_tecnicas():
    data = call_restlet_with_cache("2092", ttl=300)

    logger.info(
        f"/netsuite/facturacion_areas_tecnicas → rows={len(data.get('facturacion_areas_tecnicas', []))}"
    )

    return {"rows": data}


# ==========================================================
# ENDPOINT: COMERCIAL
# ==========================================================

@router.get("/comercial")
def comercial():
    data = call_restlet_with_cache("2091", ttl=300)

    logger.info(
        f"/netsuite/comercial → rows={len(data.get('clientes_potenciales', []))}"
    )

    return {"rows": data}


# ==========================================================
# ENDPOINT: POSVENTA
# ==========================================================

@router.get("/posventa")
def posventa():
    data = call_restlet_with_cache("2121", ttl=300)

    logger.info(
        f"/netsuite/posventa → rows={len(data.get('relev_posventa', []))}"
    )

    return {"rows": data}