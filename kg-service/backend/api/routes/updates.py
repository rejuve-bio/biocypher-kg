from fastapi import APIRouter, Query
from backend.core.neo4j_client import neo4j_client
from datetime import datetime, timedelta
from typing import Optional

router = APIRouter()

@router.get("/updates")
def get_updates(
    since: Optional[str] = Query(default=None, description="ISO timestamp"),
    hours: Optional[int] = Query(default=None, description="Last N hours")
):
    if since:
        since_timestamp = since
    elif hours:
        since_timestamp = (datetime.utcnow() - timedelta(hours=hours)).isoformat() + 'Z'
    else:
        since_timestamp = (datetime.utcnow() - timedelta(hours=24)).isoformat() + 'Z'

    return neo4j_client.get_updates_since(since_timestamp)
