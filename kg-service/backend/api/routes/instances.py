"""Console-managed database instances (one Neo4j + one MORK per species).

Backs Settings > Species databases ("Run here"): set up, start, stop and remove each species' instances
from the browser. Requires the Docker socket mounted into the console container.
"""
from fastapi import APIRouter, HTTPException

from backend.core.console import instances

router = APIRouter(prefix="/api/console", tags=["Console"])


@router.get("/instances")
def list_instances():
    """Each species' Neo4j and MORK: exists / running, where to open it, setup progress."""
    try:
        return instances.status()
    except instances.InstanceError as exc:
        return {"docker": False, "error": str(exc), "species": []}


@router.post("/instances/{species}/{kind}/{action}", status_code=202)
def instance_action(species: str, kind: str, action: str):
    """action: "setup" creates (if needed), starts and connects the instance in the
    background (poll GET /instances for progress); "stop" stops it; "remove" removes
    its container but keeps its data volume, and clears its saved connection."""
    try:
        if action == "setup":
            instances.setup(species, kind)
        elif action == "stop":
            instances.stop(species, kind)
        elif action == "remove":
            instances.remove(species, kind)
        else:
            raise HTTPException(
                status_code=400, detail=f"Unknown action '{action}' (setup, stop or remove).",
            )
    except instances.InstanceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"species": species, "kind": kind, "action": action}
