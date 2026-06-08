# netsuite.py

# ==========================================================
# Importaciones
# ==========================================================
from fastapi import APIRouter, Query
from app.netsuite_client import call_restlet_with_cache
import logging

router = APIRouter(prefix="/netsuite")
logger = logging.getLogger("netsuite")

# ==========================================================
# Endpoint: Instalaciones
# ==========================================================
@router.get("/instalaciones")
def instalaciones(case_assigned: str | None = Query(None, description="Filtrar por case_assigned")):
    """
    Endpoint que expone datos del Restlet script_id=2089 con opción de filtrado dinámico.

    Flujo técnico:
    1. Recibe `case_assigned` como query param opcional.
    2. Llama a call_restlet_with_cache con params dinámicos.
    3. Genera cache en Redis diferenciada por params.
    4. Loggea información para trazabilidad.
    """

    logger.info(f"case_assigned recibido: {case_assigned}")

    # Construir params dinámicos solo si existe case_assigned
    params = {"case_assigned": case_assigned} if case_assigned else None

    # TTL de 300 segundos
    data = call_restlet_with_cache("2089", ttl=300, params=params)

    total_inst_caso = len(data.get("total_inst_caso", []))
    lista_art_inst = len(data.get("lista_art_inst", []))
    total_art_caso = len(data.get("total_art_caso", []))

    logger.info(
        f"Endpoint /instalaciones ejecutado. "
        f"Instalaciones del caso: {total_inst_caso}, "
        f"Artículos instalados listados: {lista_art_inst}, "
        f"Total de artículos del caso: {total_art_caso}."
    )

    return {
        "total_inst_caso": data.get("total_inst_caso", []),
        "lista_art_inst": data.get("lista_art_inst", []),
        "total_art_caso": data.get("total_art_caso", [])
    }


# ==========================================================
# Endpoint: Facturación Áreas Técnicas
# ==========================================================
@router.get("/facturacion_areas_tecnicas")
def facturacion():
    data = call_restlet_with_cache("2092", ttl=300)
    total_rows = len(data.get("facturacion_areas_tecnicas", []))

    logger.info(
        f"Endpoint /facturacion_areas_tecnicas ejecutado. "
        f"Registros devueltos: {total_rows}."
    )

    return {
        "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", [])
    }


# ==========================================================
# Endpoint: Comercial
# ==========================================================
@router.get("/comercial")
def comercial():
    data = call_restlet_with_cache("2091", ttl=300)

    clientes_potenciales = len(data.get("clientes_potenciales", []))
    oportunidades_cerradas = len(data.get("oportunidades_cerradas", []))

    logger.info(
        f"Endpoint /comercial ejecutado. "
        f"Clientes potenciales: {clientes_potenciales}, "
        f"Oportunidades cerradas: {oportunidades_cerradas}."
    )

    return {
        "clientes_potenciales": data.get("clientes_potenciales", []),
        "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
    }


# ==========================================================
# Endpoint: Posventa
# ==========================================================
@router.get("/posventa")
def posventa(case_assigned: str | None = Query(None, description="Filtrar instalaciones por case_assigned")):

    logger.info(f"case_assigned recibido en posventa: {case_assigned}")

    params = {"case_assigned": case_assigned} if case_assigned else None

    data = call_restlet_with_cache("2121", ttl=300, params=params)

    total_inst_caso = len(data.get("total_inst_caso", []))
    relev_posventa = len(data.get("relev_posventa", []))
    oportunidades_articulos = len(data.get("oportunidades_articulos", []))

    logger.info(
        f"Endpoint /posventa ejecutado. "
        f"Instalaciones del caso: {total_inst_caso}, "
        f"Relevamientos posventa: {relev_posventa}, "
        f"Oportunidades artículos: {oportunidades_articulos}."
    )

    return {
        "total_inst_caso": data.get("total_inst_caso", []),
        "relev_posventa": data.get("relev_posventa", []),
        "oportunidades_articulos": data.get("oportunidades_articulos", [])
    }