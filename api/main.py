"""
api/main.py
FastAPI application — mounts all route groups, CORS, WebSocket support.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from api.routes import connections, graph, commands, dag, preview, commit, plans

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    logger.info("migrate_io.starting", env=os.environ.get("APP_ENV", "development"))
    yield
    logger.info("migrate_io.stopping")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Migrate.io API",
        description=(
            "Universal Data Migration Platform — connect any source to any destination, "
            "build a schema graph, issue natural-language commands, compile to Spark, "
            "preview, and commit atomically."
        ),
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # ── CORS ──────────────────────────────────────────────────
    origins = os.environ.get("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # ── Routes ────────────────────────────────────────────────
    app.include_router(connections.router, prefix="/api/v1/connections", tags=["Connections"])
    app.include_router(graph.router, prefix="/api/v1/graph", tags=["Schema Graph"])
    app.include_router(commands.router, prefix="/api/v1/commands", tags=["NL Commands"])
    app.include_router(dag.router, prefix="/api/v1/dag", tags=["DAG"])
    app.include_router(preview.router, prefix="/api/v1/preview", tags=["Preview & Execution"])
    app.include_router(commit.router, prefix="/api/v1/commit", tags=["Commit"])
    app.include_router(plans.router, prefix="/api/v1/plans", tags=["Plan Versioning"])

    # ── Health ────────────────────────────────────────────────
    @app.get("/health", tags=["System"])
    async def health():
        return {"status": "ok", "service": "migrate-io"}

    return app


app = create_app()
