from fastapi import APIRouter
from app.netsuite_client import call_restlet

router = APIRouter(prefix="/netsuite")

@router.get("/instalaciones")
def instalaciones():
    data = call_restlet("2089")
    return {
        "total_inst_caso": data.get("total_inst_caso", []),
        "lista_art_inst": data.get("lista_art_inst", []),
        "total_art_caso": data.get("total_art_caso", [])
    }

@router.get("/facturacion_areas_tecnicas")
def facturacion():
    data = call_restlet("2092")
    return {
        "facturacion_areas_tecnicas": data.get("facturacion_areas_tecnicas", [])
    }

@router.get("/comercial")
def comercial():
    data = call_restlet("2091")
    return {
        "clientes_potenciales": data.get("clientes_potenciales", []),
        "oportunidades_cerradas": data.get("oportunidades_cerradas", [])
    }