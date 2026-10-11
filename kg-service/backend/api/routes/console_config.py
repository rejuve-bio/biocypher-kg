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


# ── Guided (structured) config editing ───────────────────────────────────────

class EntryWrite(BaseModel):
    data: dict


@router.get("/species/{species}/datasets/{dataset}/config/{kind}/entries")
def get_config_entries(species: str, dataset: str, kind: str):
    """Ordered list of the config's named entries with a light summary of each."""
    try:
        return ci.list_config_entries(species, dataset, kind)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/species/{species}/datasets/{dataset}/config/{kind}/entries/{name}")
def get_config_entry(species: str, dataset: str, kind: str, name: str):
    """Full structured data for a single entry."""
    try:
        return ci.get_config_entry(species, dataset, kind, name)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.put("/species/{species}/datasets/{dataset}/config/{kind}/entries/{name}")
def put_config_entry(species: str, dataset: str, kind: str, name: str, body: EntryWrite):
    """Add or replace one entry (splices only its block; validated on save)."""
    try:
        return ci.save_config_entry(species, dataset, kind, name, body.data)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/species/{species}/datasets/{dataset}/config/{kind}/entries/{name}")
def delete_config_entry(species: str, dataset: str, kind: str, name: str):
    """Remove one entry from the config."""
    try:
        return ci.delete_config_entry(species, dataset, kind, name)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/biolink/categories")
def biolink_categories():
    """Distinct biolink categories used across the schema configs (for pickers)."""
    return {"categories": ci.list_biolink_categories()}


@router.get("/writers")
def get_writers():
    return {"writers": ci.list_writers()}


@router.get("/flags")
def get_flags():
    return {"flags": ci.list_flags()}


@router.get("/build/limits")
def get_build_limits():
    """Host limits for build options, e.g. the CPU count that caps useful workers."""
    import os
    cpu = os.cpu_count() or 1
    # The CLI default is min(8, CPUs); more workers than CPUs only oversubscribe.
    return {"cpu_count": cpu, "max_workers": min(8, cpu)}


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


@router.get("/explore/targets")
def explore_targets():
    """Species and the Neo4j/MORK endpoints they resolve to (for the Explore selector)."""
    from backend.core.console import explore_targets as et
    return {"targets": et.list_targets()}


@router.get("/explore/schema")
def explore_schema(species: str, db: str = "neo4j"):
    """Live schema (+ stats) for one species' database instance.

    db=neo4j reads that species' Neo4j; db=mork reads that species' MORK. Both
    return the same {schema:{nodes,edges}} shape the SchemaGraph renders, plus
    db-specific stats. Unreachable/misconfigured instances report it rather than
    failing the whole request.
    """
    from backend.core.console import explore_targets as et

    if db == "neo4j":
        client = et.get_neo4j_client(species)
        if client is None:
            return {"species": species, "db": db, "reachable": False, "error": "No Neo4j endpoint configured for this species."}
        try:
            schema = client.get_detailed_schema()
            counts = client.get_total_counts()
            top_entities = client.get_node_type_distribution()
            top_connections = client.get_edge_type_distribution()
        except Exception:  # noqa: BLE001 - instance down/unreachable
            return {"species": species, "db": db, "reachable": False,
                    "error": f"Could not reach Neo4j at {et.neo4j_uri_for(species)}."}
        return {
            "species": species, "db": db, "reachable": True,
            "schema": schema,
            "node_types": len(schema.get("nodes", [])),
            "edge_types": len(top_connections or []),
            "node_count": counts.get("node_count"),
            "edge_count": counts.get("edge_count"),
            "top_entities": top_entities,
            "top_connections": top_connections,
        }

    if db == "mork":
        try:
            client = et.get_mork_client(species)
        except ImportError as exc:
            return {"species": species, "db": db, "reachable": False, "error": str(exc)}
        except Exception:  # noqa: BLE001 - instance down/unreachable
            return {"species": species, "db": db, "reachable": False,
                    "error": f"Could not reach MORK at {et.mork_url_for(species)}."}
        if client is None:
            return {"species": species, "db": db, "reachable": False, "error": "No MORK endpoint configured for this species."}
        try:
            result = client.get_schema()
            atom_count = client.get_atom_count()
        except Exception:  # noqa: BLE001
            return {"species": species, "db": db, "reachable": False,
                    "error": f"Could not reach MORK at {et.mork_url_for(species)}."}
        latest = None
        try:
            lv = client.get_latest_version()
            latest = lv.get("version") if lv else None
        except Exception:  # noqa: BLE001
            latest = None
        return {
            "species": species, "db": db, "reachable": True,
            "schema": result.get("schema"),
            "node_types": result.get("node_type_count"),
            "edge_types": result.get("edge_type_count"),
            "sampled": result.get("sampled"),
            "atom_count": atom_count,
            "version": latest,
        }

    raise HTTPException(status_code=400, detail=f"Unknown db '{db}' (expected neo4j or mork)")


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
