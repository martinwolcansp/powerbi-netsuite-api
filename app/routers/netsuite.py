# app/routers/netsuite.py
from fastapi import APIRouter
from app.netsuite_client import call_restlet_with_cache
import logging

router = APIRouter(prefix="/netsuite")
logger = logging.getLogger("netsuite")
logging.basicConfig(level=logging.INFO)


@router.get("/instalaciones")
def instalaciones():
    data = call_restlet_with_cache("2089", ttl=300)

    logger.info(
        f"/instalaciones called | "
        f"total_inst_caso={len(data.get('total_inst_caso', []))} | "
        f"lista_art_inst={len(data.get('lista_art_inst', []))} | "
        f"total_art_caso={len(data.get('total_art_caso', []))}"
    )

    return {
        "total_inst_caso": data.get("total_inst_caso", []),
        "lista_art_inst": data.get("lista_art_inst", []),
        "total_art_caso": data.get("total_art_caso", [])
    }


@router.get("/facturacion_areas_tecnicas")
def facturacion():
    data = call_restlet_with_cache("2092", ttl=300)

    logger.info(
        f"/facturacion_areas_tecnicas called | "
        f"total_rows={len(data.get('facturacion_areas_tecnicas', []))}"
    )

    return {
        "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", [])
    }


@router.get("/comercial")
def comercial():
    data = call_restlet_with_cache("2091", ttl=300)

    logger.info(
        f"/comercial called | "
        f"clientes_potenciales={len(data.get('clientes_potenciales', []))} | "
        f"oportunidades_cerradas={len(data.get('oportunidades_cerradas', []))}"
    )

    return {
        "clientes_potenciales": data.get("clientes_potenciales", []),
        "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
    }