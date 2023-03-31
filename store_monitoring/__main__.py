import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import UJSONResponse, RedirectResponse
from fastapi_utils.tasks import repeat_every

from . import app, conn
from .database import init_db, update_db
from .routes import router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def startup(_: FastAPI):
    #  Create databases
    logger.info("Initializing database")
    init_db(conn)
    yield


@repeat_every(seconds=3600)
async def sync_data():
    update_db(conn)


@app.get(
    "/",
    summary="Redirect to /docs",
)
async def index():
    return RedirectResponse("/docs")


app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
