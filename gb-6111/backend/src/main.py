import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from scripts.seed_demo import seed, setup_schema
from src.config.settings import SEED_DEMO
from src.domain.scenario import CONFIG
from src.middlewares.errors import catch_errors
from src.middlewares.request_id import request_id
from src.routes import cases, runs, session, system, volumes


@asynccontextmanager
async def lifespan(_app):
    setup_schema()
    if SEED_DEMO:
        try:
            seed()
        except Exception:
            logging.getLogger(__name__).exception("Demo seed failed")
    yield


app = FastAPI(title=CONFIG["title"], lifespan=lifespan)
app.middleware("http")(catch_errors)
app.middleware("http")(request_id)
for group in (system, session, cases, runs):
    app.include_router(group.router)

# 分卷工具的近义资源前缀；核心业务仍在 src.domain.volumes。
for prefix in (
    "/volumes", "/archive-volumes", "/volume-batches", "/split-archives",
    "/library", "/archive-batches", "/receipts",
):
    app.include_router(volumes.router, prefix=prefix)
# 也允许 /batches/... 风格。
app.include_router(volumes.router, prefix="/batch-workflow")
app.include_router(volumes.router, prefix="/api")
