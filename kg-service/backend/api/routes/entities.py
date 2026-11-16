from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from backend.core.neo4j_client import neo4j_client

router = APIRouter()

@router.get("/entities/{label}")
def get_entities(
    label: str,
    limit: int = Query(default=100, ge=1, le=1000, description="Max number of entities to return"),
    offset: int = Query(default=0, ge=0, description="Number of entities to skip"),
    updated_since: Optional[str] = Query(default=None, description="ISO timestamp filter"),
):
    """Search/list entities for a given label with pagination."""
    entities = neo4j_client.get_entities(
        label, limit=limit, offset=offset, updated_since=updated_since
    )
    total = neo4j_client.get_entity_count(label)
    return {
        "label": label,
        "total": total,
        "limit": limit,
        "offset": offset,
        "entities": entities,
    }

@router.get("/entities/{label}/count")
def get_entity_count(label: str):
    """Get the total number of entities with the given label."""
    return {"label": label, "count": neo4j_client.get_entity_count(label)}

@router.get("/entities/{label}/{entity_id}")
def get_entity_by_id(label: str, entity_id: str):
    """Get a single entity by its id for the given label."""
    entity = neo4j_client.get_entity_by_id(label, entity_id)
    if entity is None:
        raise HTTPException(
            status_code=404,
            detail=f"No {label} entity found with id '{entity_id}'",
        )
    return entity
