from fastapi import FastAPI
import logging
from app.routers import netsuite, powerbi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

app = FastAPI(
    title="NetSuite → Power BI API",
    version="3.0.0"
)

app.include_router(netsuite.router)
app.include_router(powerbi.router)

@app.get("/")
def healthcheck():
    return {"status": "ok"}