from fastapi import APIRouter, Header, HTTPException
from app.config import POWERBI_API_KEY
from app.netsuite import call_restlet

router = APIRouter()


@router.get("/powerbi/instalaciones")
def instalaciones(x_api_key: str = Header(...)):
    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    data = call_restlet(
        script_id=2089,
        deploy_id=1,
        method="GET"
    )

    # Normalización mínima para Power BI
    return {
        "rows": data
    }
