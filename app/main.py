from fastapi import FastAPI
from app.powerbi import router as powerbi_router

app = FastAPI(
    title="NetSuite → Power BI API",
    version="1.0.0"
)

app.include_router(powerbi_router)


@app.get("/")
def healthcheck():
    return {"status": "ok"}
