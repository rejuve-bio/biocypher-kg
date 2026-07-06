from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from backend.api.routes import versions
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.core.config import settings
from backend.core.neo4j_client import neo4j_client
from backend.api.routes import updates, summary, graph_info
from backend.api.routes import console_config, builds

# `meta` and `entities` routers are referenced by the original Observatory but were
# never committed to this branch. Import them optionally so their absence doesn't
# prevent the app (and the Console) from starting.
try:
    from backend.api.routes import meta  # type: ignore
except ImportError:
    meta = None
try:
    from backend.api.routes import entities  # type: ignore
except ImportError:
    entities = None
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from backend.core.graph_info_cache import graph_info_cache
from backend.core.console.job_registry import registry as build_registry
from pathlib import Path


import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Background scheduler for auto-refresh
scheduler = BackgroundScheduler()

def refresh_graph_info():
    """Background task to refresh graph_info.json"""
    try:
        logger.info("🔄 Auto-refreshing graph_info.json...")
        graph_info_cache.refresh()
        logger.info("✅ Auto-refresh complete!")
    except Exception as e:
        logger.error(f"❌ Auto-refresh failed: {e}")

# Schedule refresh every 72 hour
scheduler.add_job(refresh_graph_info, 'interval', hours=72, id='graph_info_refresh')


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle (replaces deprecated @app.on_event handlers)."""
    logger.info(f"Starting {settings.APP_NAME}")

    # Background scheduler for graph_info refresh (Observatory).
    logger.info("🚀 Starting background scheduler...")
    scheduler.start()
    logger.info("✅ Background scheduler started (refresh every 72 hours)")
    if not graph_info_cache.cache_file.exists():
        logger.info("📊 Scheduling initial graph_info.json generation (background)...")
        scheduler.add_job(
            refresh_graph_info, "date",
            run_date=datetime.now() + timedelta(seconds=5),
            id="initial_generation",
        )
        logger.info("⏳ Initial generation will start in 5 seconds (API ready now!)")
    else:
        logger.info("✓ Existing cache file found, using it")

    # Console: repair build jobs left RUNNING by a previous process instance.
    try:
        build_registry.reconcile()
        logger.info("✅ Build registry reconciled")
    except Exception as e:  # noqa: BLE001 - never block startup on this
        logger.error(f"Build registry reconciliation failed: {e}")

    # Observatory: verify Neo4j (non-fatal — Console works without it).
    neo4j_client.verify_connection()

    yield

    scheduler.shutdown()
    logger.info("🛑 Background scheduler stopped")
    neo4j_client.close()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Real-time API for BioCypher Knowledge Graphs",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
if meta is not None:
    app.include_router(meta.router, prefix="/api", tags=["Meta"])
if entities is not None:
    app.include_router(entities.router, prefix="/api", tags=["Entities"])
app.include_router(updates.router, prefix="/api", tags=["Updates"])
app.include_router(summary.router, prefix="/api", tags=["Summary"])
app.include_router(versions.router)
app.include_router(graph_info.router, prefix="/api", tags=["graph-info"])

# Console (configuration + build management). Routers already carry the
# /api/console prefix and the "Console" tag.
app.include_router(console_config.router)
app.include_router(builds.router)


@app.get("/")
def root():
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs"
    }

@app.get("/health")
def health():
    connected = neo4j_client.verify_connection()
    return {
        "status": "healthy" if connected else "unhealthy",
        "neo4j": "connected" if connected else "disconnected"
    }

class SPAStaticFiles(StaticFiles):
    """StaticFiles that falls back to index.html for unknown paths.

    Without this, client-side routes (e.g. /console/history, /console/builds/<id>)
    would 404 on a direct visit or page refresh, since no matching file exists.
    """
    async def get_response(self, path, scope):
        try:
            response = await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404:
                return await super().get_response("index.html", scope)
            raise
        if response.status_code == 404:
            return await super().get_response("index.html", scope)
        return response


# Serve the built Console SPA at /console (mounted last so /api/* and the JSON
# routes above always win). No-op in dev, where Vite serves the app itself.
if settings.SERVE_FRONTEND:
    _frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if _frontend_dist.is_dir():
        app.mount("/console", SPAStaticFiles(directory=str(_frontend_dist), html=True),
                  name="console-frontend")
        logger.info(f"Serving Console frontend from {_frontend_dist} at /console")
    else:
        logger.info("Console frontend not built (frontend/dist missing); skipping mount")
