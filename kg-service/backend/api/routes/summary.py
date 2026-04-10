from fastapi import APIRouter
from backend.core.neo4j_client import neo4j_client

router = APIRouter()

@router.get("/summary")
def get_summary():
    counts = neo4j_client.get_total_counts()
    node_dist = neo4j_client.get_node_type_distribution()
    edge_dist = neo4j_client.get_edge_type_distribution()
    last_updated = neo4j_client.get_last_updated()
    datasets = neo4j_client.get_datasets_with_metadata()
    labels = neo4j_client.get_labels()
    rels = neo4j_client.get_relationship_types()
    db_size = neo4j_client.get_database_size()   

    return {
        "node_count": counts["node_count"],
        "edge_count": counts["edge_count"],
        "last_updated_at": last_updated,
        "dataset_count": len(datasets),
        "top_entities": node_dist,
        "top_connections": edge_dist,
        "datasets": datasets,
        "database_size_gb": db_size,
        "schema": {
            "node_types": labels,
            "relationship_types": rels
        }
    }
