# powerbi.py
from fastapi import APIRouter, Header, HTTPException
from app.config import POWERBI_API_KEY
from app.netsuite_client import call_restlet_with_cache
import logging

router = APIRouter(prefix="/powerbi")
logger = logging.getLogger("powerbi")


# ==========================================================
# Función generadora de endpoints PowerBI
# ==========================================================

def create_powerbi_endpoint(route: str, script_id: str, log_key: str):
    @router.get(route)
    def endpoint(x_api_key: str = Header(...)):

        # Validación API Key
        if x_api_key != POWERBI_API_KEY:
            raise HTTPException(status_code=401, detail="Invalid API Key")

        # Llamada al Restlet con cache
        data = call_restlet_with_cache(script_id, ttl=300)

        # Logging
        logger.info(
            f"PowerBI {route} returned {len(data.get(log_key, []))} rows"
        )

        # Respuesta estándar
        return {
            "rows": data
        }

    return endpoint


# ==========================================================
# Endpoints PowerBI
# ==========================================================

create_powerbi_endpoint(
    "/instalaciones",
    "2089",
    "total_inst_caso"
)

create_powerbi_endpoint(
    "/facturacion_areas_tecnicas",
    "2092",
    "facturacion_areas_tecnicas"
)

create_powerbi_endpoint(
    "/comercial",
    "2091",
    "clientes_potenciales"
)

create_powerbi_endpoint(
    "/posventa",
    "2121",
    "relev_posventa"
)