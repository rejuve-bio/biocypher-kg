from fastapi import APIRouter, Query
from backend.core.neo4j_client import neo4j_client

router = APIRouter()

@router.get("/labels")
def get_labels():
    """List all node labels in the graph."""
    return {"labels": neo4j_client.get_labels()}

@router.get("/relationship-types")
def get_relationship_types():
    """List all relationship types in the graph."""
    return {"relationship_types": neo4j_client.get_relationship_types()}

@router.get("/schema")
def get_detailed_schema():
    """Get the comprehensive schema (node properties and edge connections)."""
    return neo4j_client.get_detailed_schema()

@router.get("/properties")
def get_entity_properties(label: str = Query(..., description="Node label to inspect")):
    """List the properties present on nodes with the given label."""
    return {"label": label, "properties": neo4j_client.get_entity_properties(label)}
