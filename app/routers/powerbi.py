from fastapi import APIRouter, Header, HTTPException
from app.config import POWERBI_API_KEY
from app.netsuite_client import call_restlet

router = APIRouter(prefix="/powerbi")

@router.get("/instalaciones")
def instalaciones(x_api_key: str = Header(...)):
    if x_api_key != POWERBI_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    data = call_restlet("2089")

    return {
        "rows": data
    }