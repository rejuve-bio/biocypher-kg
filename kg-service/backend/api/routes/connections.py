"""What each species' Neo4j and MORK is, and connecting your own.

Backs Settings > In use (GET /connections) and the "Use existing" side of Settings >
Species databases. The "Run here" side is backend/api/routes/instances.py.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.core.console import connections

router = APIRouter(prefix="/api/console", tags=["Console"])


class ExistingDatabase(BaseModel):
    address: str
    # Neo4j only; MORK has no login. A blank password keeps the saved one when the
    # address is unchanged.
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


@router.get("/connections")
def list_connections(check: bool = True):
    """Each species: the Neo4j and MORK it uses, where they come from (run by the
    console or your own), the Neo4j login user, and (check=true) whether they work."""
    return {"species": connections.status(check=check)}


@router.put("/connections/{species}/{kind}")
def connect_existing(species: str, kind: str, body: ExistingDatabase):
    """Use a database you already run. Checked first; saved only if it works."""
    try:
        return connections.connect_existing(
            species, kind, body.address, body.user, body.password, body.database,
        )
    except connections.ConnectError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/connections/{species}/{kind}")
def disconnect(species: str, kind: str):
    """Stop using a species' database. The database itself is left alone."""
    try:
        return connections.disconnect(species, kind)
    except connections.ConnectError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/connections/{species}/neo4j/login")
def console_login(species: str):
    """The login of a Neo4j the console runs, to sign in to Neo4j Browser. Your own
    databases' passwords are never returned."""
    try:
        return connections.console_login(species)
    except connections.ConnectError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
