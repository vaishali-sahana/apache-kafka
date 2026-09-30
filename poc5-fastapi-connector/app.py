import uvicorn
from fastapi import FastAPI, APIRouter

from source import source_router
from sink import sink_router

app = FastAPI(title="POC5 - Kafka Connect Pattern via FastAPI")
router = APIRouter()
router.include_router(source_router)
router.include_router(sink_router)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
