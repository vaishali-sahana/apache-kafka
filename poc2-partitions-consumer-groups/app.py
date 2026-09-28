import uvicorn
from fastapi import FastAPI, APIRouter

from producer import producer_router
from consumer import consumer_router
from topic import topic_router

app = FastAPI(title="POC2 - Partitions & Consumer Groups")
router = APIRouter()
router.include_router(topic_router)
router.include_router(producer_router)
router.include_router(consumer_router)

app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
