# netsuite.py

# ==========================================================
# Importaciones
# ==========================================================

# APIRouter permite modularizar endpoints dentro de FastAPI.
# Este router será luego incluido en main.py.
from fastapi import APIRouter

# Función centralizada que:
# - Invoca Restlets en NetSuite
# - Gestiona OAuth
# - Aplica cache distribuido (Redis)
# - Aplica lock local
from app.netsuite_client import call_restlet_with_cache

import logging

# Prefijo común para todos los endpoints de este módulo.
# Resultado final: /netsuite/...
router = APIRouter(prefix="/netsuite")

# Logger alineado con el logger del cliente NetSuite
# Permite trazabilidad completa desde endpoint → Restlet.
logger = logging.getLogger("netsuite")


# ==========================================================
# Endpoint: Instalaciones
# ==========================================================

@router.get("/instalaciones")
def instalaciones():
    """
    Endpoint que expone datos del Restlet script_id=2089.

    Flujo técnico:
    1. Invoca call_restlet_with_cache()
       - Si hay cache (TTL 300s) → no consulta NetSuite.
       - Si no hay cache → invoca Restlet y guarda resultado.
    2. Extrae subconjuntos específicos del payload.
    3. Loggea métricas básicas para observabilidad.
    4. Devuelve estructura normalizada.
    """

    # TTL de 300 segundos (5 minutos)
    # Reduce carga sobre NetSuite y mejora tiempos de respuesta.
    data = call_restlet_with_cache("2089", ttl=300)

    # Se calculan métricas de volumen para logging.
    # Esto no altera la respuesta, solo aporta trazabilidad.
    total_inst_caso = len(data.get("total_inst_caso", []))
    lista_art_inst = len(data.get("lista_art_inst", []))
    total_art_caso = len(data.get("total_art_caso", []))

    logger.info(
        "Endpoint /instalaciones ejecutado. "
        f"Instalaciones del caso: {total_inst_caso}, "
        f"Artículos instalados listados: {lista_art_inst}, "
        f"Total de artículos del caso: {total_art_caso}."
    )

    # Se devuelven únicamente las claves esperadas.
    # Si alguna no existe, se devuelve lista vacía para:
    # - Mantener contrato estable
    # - Evitar errores en Power BI
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
    """
    Endpoint que expone datos del Restlet script_id=2092.

    Características:
    - Cache de 5 minutos.
    - Logging del volumen de registros devueltos.
    - Devuelve estructura plana con una única colección.
    """

    data = call_restlet_with_cache("2092", ttl=300)

    # Métrica de control para monitorear variaciones
    # anómalas en cantidad de registros.
    total_rows = len(data.get("facturacion_areas_tecnicas", []))

    logger.info(
        "Endpoint /facturacion_areas_tecnicas ejecutado. "
        f"Registros devueltos: {total_rows}."
    )

    return {
        # Normalización defensiva:
        # Si la clave no existe, se devuelve lista vacía.
        "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", [])
    }


# ==========================================================
# Endpoint: Comercial
# ==========================================================

@router.get("/comercial")
def comercial():
    """
    Endpoint que expone datos del Restlet script_id=2091.

    Contiene información comercial segmentada en:
    - clientes_potenciales
    - oportunidades_cerradas

    Utiliza el mismo mecanismo de cache distribuido + lock local
    definido en netsuite_client.py.
    """

    data = call_restlet_with_cache("2091", ttl=300)

    # Métricas para observabilidad y control de negocio.
    clientes_potenciales = len(data.get("clientes_potenciales", []))
    oportunidades_cerradas = len(data.get("oportunidades_cerradas", []))

    logger.info(
        "Endpoint /comercial ejecutado. "
        f"Clientes potenciales: {clientes_potenciales}, "
        f"Oportunidades cerradas: {oportunidades_cerradas}."
    )

    return {
        # Contrato de respuesta explícito y estable
        "clientes_potenciales": data.get("clientes_potenciales", []),
        "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
    }