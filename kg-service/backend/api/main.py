 
from datetime import datetime, timedelta
from backend.api.routes import versions
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.core.config import settings
from backend.core.neo4j_client import neo4j_client
from backend.api.routes import meta, entities, updates, summary, graph_info
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from backend.core.graph_info_cache import graph_info_cache


import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Real-time API for BioCypher Knowledge Graphs"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

@app.on_event("startup")
async def startup_event():
    """Start background tasks on startup (NON-BLOCKING)"""
    logger.info("🚀 Starting background scheduler...")
    
    # Start scheduler first
    scheduler.start()
    logger.info("✅ Background scheduler started (refresh every 72 hours)")
    
    # If cache doesn't exist, trigger generation in background (non-blocking!)
    if not graph_info_cache.cache_file.exists():
        logger.info("📊 Scheduling initial graph_info.json generation (background)...")
        # Run 5 seconds from now (non-blocking)
        scheduler.add_job(
            refresh_graph_info, 
            'date', 
            run_date=datetime.now() + timedelta(seconds=5),
            id='initial_generation'
        )
        logger.info("⏳ Initial generation will start in 5 seconds (API ready now!)")
    else:
        logger.info("✓ Existing cache file found, using it")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop background tasks on shutdown"""
    scheduler.shutdown()
    logger.info("🛑 Background scheduler stopped")

# Register routes
app.include_router(meta.router, prefix="/api", tags=["Meta"])
app.include_router(entities.router, prefix="/api", tags=["Entities"])
app.include_router(updates.router, prefix="/api", tags=["Updates"])
app.include_router(summary.router, prefix="/api", tags=["Summary"])
app.include_router(versions.router)
app.include_router(graph_info.router, prefix="/api", tags=["graph-info"]) 

@app.on_event("startup")
async def startup():
    logger.info(f"Starting {settings.APP_NAME}")
    neo4j_client.verify_connection()

@app.on_event("shutdown")
async def shutdown():
    neo4j_client.close()

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
