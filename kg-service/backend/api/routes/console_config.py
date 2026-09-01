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


def _mork_call(op):
    """Construct a MORK client (its constructor probes the server) and run ``op``.

    Connection failures report unreachable; an unsafe interpolated symbol is a
    400; a library problem keeps the import message.
    """
    from backend.core.config import settings
    from backend.core.mork_client import MORKClient

    try:
        client = MORKClient()
    except ImportError as exc:
        return {"reachable": False, "error": str(exc)}
    except Exception:  # noqa: BLE001 - server down or unreachable at MORK_URL
        return {"reachable": False, "error": f"Could not connect to MORK at {settings.MORK_URL}."}
    try:
        return {"reachable": True, **op(client)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/mork/instances/{node_type}")
def mork_instances(node_type: str, limit: int = 50):
    """List instances of a MORK node type with their properties."""
    return _mork_call(lambda c: c.get_instances(node_type, limit=limit))


@router.get("/mork/neighbors/{node_type}/{node_id}")
def mork_neighbors(node_type: str, node_id: str, limit: int = 50):
    """Immediate neighborhood (in/out edges + properties) of one MORK instance."""
    return _mork_call(lambda c: c.get_neighbors(node_type, node_id, limit=limit))


class PatternQuery(BaseModel):
    pattern: str
    template: str | None = None
    limit: int = 200
    distinct: bool = True


@router.post("/mork/query")
def mork_query(body: PatternQuery):
    """Run a read-only MeTTa pattern query against the live atomspace.

    Uses ``download`` (read-only) scoped to the annotation namespace; never
    ``transform``/``query`` (which write). A malformed pattern returns an error
    field rather than a misleading unreachable.
    """
    from backend.core.config import settings
    from backend.core.mork_client import MORKClient

    limit = max(1, min(body.limit, 2000))
    try:
        client = MORKClient()
    except ImportError as exc:
        return {"reachable": False, "error": str(exc)}
    except Exception:  # noqa: BLE001
        return {"reachable": False, "error": f"Could not connect to MORK at {settings.MORK_URL}."}
    try:
        result = client.run_pattern(body.pattern, body.template, limit=limit, distinct=body.distinct)
    except Exception as exc:  # noqa: BLE001 - malformed pattern / server-side query error
        return {"reachable": True, "error": f"Query failed: {exc}"}
    return {"reachable": True, **result}


@router.get("/mork/atoms")
def mork_atoms(limit: int = 200):
    """A sample of raw MeTTa atoms for the S-expression tree view."""
    return _mork_call(lambda c: c.get_raw_atoms(limit=max(1, min(limit, 2000))))


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
