from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog
from .core.config import settings
from .core.logging import configure_logging
from .core.middleware import CorrelationIDMiddleware, SecurityHeadersMiddleware
from .api import api_router

# Configure structured logging
configure_logging()
logger = structlog.get_logger()

def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
        docs_url="/docs",
        redoc_url=None,
    )

    # Middleware
    app.add_middleware(CorrelationIDMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure restrictively in production
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
        # In a real implementation, this would check DB connectivity
        # For Phase 0 skeleton, we return ok immediately
        return {"status": "ready"}

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