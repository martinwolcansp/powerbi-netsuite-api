from fastapi import FastAPI
import logging

from app.routers import netsuite, powerbi

# Logging más limpio
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

app = FastAPI(
    title="NetSuite → Power BI API",
    version="3.0.0"
)

app.include_router(netsuite.router)
app.include_router(powerbi.router)

@app.get("/")
def healthcheck():
    return {"status": "ok", "version": "3.0.0"}