"""
Database Routes
Endpoints for listing and selecting databases
"""

from fastapi import APIRouter, HTTPException
from typing import List, Dict

router = APIRouter()


@router.get("/databases", response_model=List[Dict])
async def list_databases():
    """
    List available databases
    
    Returns:
        List of database configurations
    """
    return [
        {
            "name": "neo4j",
            "display_name": "Neo4j",
            "description": "Graph database with CSV data",
            "type": "graph",
            "enabled": True
        },
        {
            "name": "mork",
            "display_name": "MORK",
            "description": "AtomSpace database with MeTTa data",
            "type": "atomspace",
            "enabled": True
        }
    ]


@router.get("/databases/{db_type}/status")
async def get_database_status(db_type: str):
    """
    Get database status and summary
    
    Args:
        db_type: Database type (neo4j or mork)
    
    Returns:
        Database status information
    """
    if db_type == "neo4j":
        from ..core.neo4j_client import Neo4jClient
        
        try:
            client = Neo4jClient()
            # Get stats from Neo4j
            return {
                "database": "neo4j",
                "status": "online",
                "message": "Connected to Neo4j"
            }
        except Exception as e:
            return {
                "database": "neo4j",
                "status": "offline",
                "message": str(e)
            }
    
    elif db_type == "mork":
        from ..core.mork_client import MORKClient
        
        try:
            client = MORKClient()
            latest = client.get_latest_version()
            
            return {
                "database": "mork",
                "status": "online",
                "latest_version": latest.get("version") if latest else None,
                "dataset_count": client.get_dataset_count(),
                "atom_count": client.get_atom_count()
            }
        except Exception as e:
            return {
                "database": "mork",
                "status": "offline",
                "message": str(e)
            }
    
    else:
        raise HTTPException(status_code=404, detail=f"Database '{db_type}' not found")