from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog
from .core.config import settings
from .core.database import check_database_connection, dispose_engine
from .core.logging import configure_logging
from .core.middleware import CorrelationIDMiddleware, SecurityHeadersMiddleware
from .api import api_router

# Configure structured logging
configure_logging()
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await dispose_engine()


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
        docs_url="/docs",
        redoc_url=None,
        lifespan=lifespan,
    )

    # Middleware
    app.add_middleware(CorrelationIDMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.BACKEND_CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API router
    app.include_router(api_router, prefix=settings.API_V1_STR)

    # Health endpoints
    @app.get("/health", tags=["health"])
    async def health_check():
        return {"status": "ok", "version": settings.VERSION}

    @app.get("/ready", tags=["health"])
    async def readiness_check():
        if not await check_database_connection():
            return JSONResponse(status_code=503, content={"status": "not_ready"})
        return {"status": "ready"}

    @app.get("/ready/telemetry", tags=["telemetry", "health"])
    async def telemetry_readiness_check():
        from .telemetry.reliability.readiness import check_telemetry_readiness
        is_ready, data = await check_telemetry_readiness()
        status_code = 200 if is_ready else 503
        return JSONResponse(status_code=status_code, content=data)

    @app.get("/telemetry/metrics", tags=["telemetry", "observability"])
    async def telemetry_metrics():
        from .telemetry.reliability.metrics import pipeline_metrics
        return pipeline_metrics.snapshot().model_dump(mode="json")

    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    return app

app = create_app()
