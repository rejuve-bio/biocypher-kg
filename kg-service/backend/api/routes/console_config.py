"""Console configuration introspection + editing endpoints."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.core.console import config_introspect as ci

router = APIRouter(prefix="/api/console", tags=["Console"])


class ConfigWrite(BaseModel):
    content: str


@router.get("/species")
def get_species():
    """List species and their datasets, with config-file existence flags."""
    try:
        return {"species": ci.list_species_and_datasets()}
    except ci.ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/species/{species}/datasets/{dataset}/adapters")
def get_adapters(species: str, dataset: str):
    """List adapters declared for a species/dataset."""
    try:
        return ci.list_adapters(species, dataset)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/species/{species}/datasets/{dataset}/schema")
def get_schema(species: str, dataset: str):
    """Shallow schema view (node/edge type names + per-source schema files)."""
    try:
        return ci.list_schema(species, dataset)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/species/{species}/datasets/{dataset}/config/{kind}")
def get_config(species: str, dataset: str, kind: str):
    """Raw text of the adapters or schema config for editing (kind: adapters|schema)."""
    try:
        return ci.read_config_text(species, dataset, kind)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.put("/species/{species}/datasets/{dataset}/config/{kind}")
def put_config(species: str, dataset: str, kind: str, body: ConfigWrite):
    """Validate and atomically save an edited config file (history is git's job)."""
    try:
        return ci.save_config_text(species, dataset, kind, body.content)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/writers")
def get_writers():
    return {"writers": ci.list_writers()}


@router.get("/flags")
def get_flags():
    return {"flags": ci.list_flags()}


@router.get("/mork/summary")
def mork_summary():
    """Live MORK stats (atomspace, so counts/versions rather than a node/edge schema)."""
    from backend.core.config import settings
    from backend.core.mork_client import MORKClient

    def _safe(fn):
        try:
            return fn()
        except Exception:  # noqa: BLE001
            return None

    try:
        client = MORKClient()
        atom_count = client.get_atom_count()
    except ImportError as exc:  # MORK client library missing (a deploy problem)
        return {"reachable": False, "error": str(exc)}
    except Exception:  # noqa: BLE001 - server down or unreachable at MORK_URL
        return {"reachable": False, "error": f"Could not connect to MORK at {settings.MORK_URL}."}
    return {
        "reachable": True,
        "atom_count": atom_count,
        "dataset_count": _safe(client.get_dataset_count),
        "latest_version": _safe(client.get_latest_version),
        "versions": _safe(client.get_all_versions) or [],
    }


@router.get("/mork/schema")
def mork_schema():
    """Live MORK node/edge schema, derived from the atomspace structure.

    MORK has no native schema, so this projects the KG atom shapes (read-only)
    and returns the same node/edge schema the Neo4j Explore graph uses.
    """
    from backend.core.config import settings
    from backend.core.mork_client import MORKClient

    try:
        client = MORKClient()
        result = client.get_schema()
    except ImportError as exc:  # MORK client library missing (a deploy problem)
        return {"reachable": False, "error": str(exc)}
    except Exception:  # noqa: BLE001 - server down or unreachable at MORK_URL
        return {"reachable": False, "error": f"Could not connect to MORK at {settings.MORK_URL}."}
    return {"reachable": True, **result}
