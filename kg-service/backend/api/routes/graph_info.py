from fastapi import APIRouter, Query
from backend.core.graph_info_cache import graph_info_cache
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/graph-info")
def get_graph_info(force_refresh: bool = Query(False, description="Force regenerate instead of using cache")):
    """
    Get comprehensive graph information.
    
    - By default, serves cached version (fast, up to 1 hour old)
    - Use ?force_refresh=true to regenerate immediately
    """
    try:
        if force_refresh:
            logger.info("🔄 Force refresh requested...")
            data = graph_info_cache.refresh()
        else:
            # Try to load from cache
            data = graph_info_cache.load_from_file()
            
            if data is None:
                # Cache doesn't exist, generate it
                logger.info("... Cache not found, generating...")
                data = graph_info_cache.refresh()
            else:
                # Show cache age
                age = graph_info_cache.get_cache_age_minutes()
                logger.info(f"... Serving cached data (age: {age} minutes)")
        
        return data
        
    except Exception as e:
        logger.error(f"... Error: {e}")
        raise

@router.get("/graph-info/status")
def get_cache_status():
    """Get cache file status"""
    age = graph_info_cache.get_cache_age_minutes()
    
    return {
        "cache_file": str(graph_info_cache.cache_file),
        "exists": graph_info_cache.cache_file.exists(),
        "age_minutes": age,
        "last_generated": graph_info_cache.last_generated.isoformat() if graph_info_cache.last_generated else None,
        "next_refresh_in_minutes": 60 - (age if age else 0) if age else "unknown"
    }