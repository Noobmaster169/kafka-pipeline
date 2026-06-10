"""
The FastAPI application: CORS, routers, and the live-hub lifecycle.

On startup it ensures the Mongo indexes exist (same set the pipeline relies on) and
starts the Kafka→WebSocket hub; on shutdown it stops the hub cleanly. Import target
for uvicorn: `backend.main:app`.
"""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Make `common` / `backend` importable when uvicorn loads this from any cwd.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402

from backend.live import hub  # noqa: E402
from backend.routers import cameras, cars, lanes, violations, ws  # noqa: E402
from common import config  # noqa: E402
from common.log import banner, get_logger  # noqa: E402
from common.mongo import ensure_indexes  # noqa: E402

log = get_logger("backend")


@asynccontextmanager
async def lifespan(app: FastAPI):
    banner("AWAS Backend API", [
        ("listen", f"{config.BACKEND_HOST}:{config.BACKEND_PORT}"),
        ("mongo", f"{config.MONGO_HOST}:{config.MONGO_PORT}/{config.MONGO_DB}"),
        ("broker", config.KAFKA_BOOTSTRAP_SERVERS),
        ("live in", config.KAFKA_TOPIC),
        ("live out", config.KAFKA_VIOLATIONS_TOPIC),
        ("cors", ", ".join(config.CORS_ORIGINS)),
    ])
    ensure_indexes()   # collections + unique violation index ready before we serve
    hub.start()        # launch the Kafka consumer threads (resilient if no broker yet)
    log.info("backend ready")
    yield
    hub.stop()
    log.info("backend stopped")


def create_app() -> FastAPI:
    app = FastAPI(title="AWAS A3 API", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.CORS_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(lanes.router)
    app.include_router(cameras.router)
    app.include_router(cars.router)
    app.include_router(violations.router)
    app.include_router(ws.router)

    @app.get("/health", tags=["meta"])
    def health() -> dict:
        return {"status": "ok"}

    return app


app = create_app()
