import uvicorn
from fastapi import FastAPI, APIRouter

from source import source_router
from cdc_reader import cdc_router
from connect_admin import connect_router

app = FastAPI(title="POC6 - Debezium CDC via FastAPI")
router = APIRouter()
router.include_router(source_router)
router.include_router(cdc_router)
router.include_router(connect_router)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
