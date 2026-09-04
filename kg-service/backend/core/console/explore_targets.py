"""Per-species database endpoints for the Explore tab.

Neo4j Community cannot host a database per species, so each species runs its own
Neo4j and MORK instance (its own port), and the console is told each species'
endpoints via env vars:

    NEO4J_URI_<SPECIES>   e.g. NEO4J_URI_HSA=bolt://localhost:7687
    MORK_URL_<SPECIES>    e.g. MORK_URL_HSA=http://localhost:8432

Species names come from species_config.yaml (hsa, mmu, ...). When a species has
no specific var, it falls back to the global NEO4J_URI / MORK_URL, so a single
combined instance keeps working until per-species instances are configured.
Neo4j creds (NEO4J_USER / NEO4J_PASSWORD / NEO4J_DATABASE) are shared.
"""
import os
from functools import lru_cache

from backend.core.config import settings
from backend.core.console import config_introspect as ci


def species_list() -> list[str]:
    try:
        return [e["species"] for e in ci.list_species_and_datasets()]
    except Exception:  # noqa: BLE001 - config missing/unreadable
        return []


def _env(name: str):
    v = os.environ.get(name)
    v = v.strip() if v else ""
    return v or None


def species_specific(species: str) -> bool:
    """True if this species has at least one endpoint set explicitly."""
    return bool(_env(f"NEO4J_URI_{species.upper()}") or _env(f"MORK_URL_{species.upper()}"))


def _any_explicit() -> bool:
    """True if any species has its own instance (per-species mode)."""
    return any(species_specific(sp) for sp in species_list())


def neo4j_uri_for(species: str):
    """Per-species Neo4j URI. Never falls back to another species' instance: in
    per-species mode an unconfigured species returns None (not configured); the
    global default is used only in single-instance mode (nothing per-species set)."""
    v = _env(f"NEO4J_URI_{species.upper()}")
    if v:
        return v
    return None if _any_explicit() else (settings.NEO4J_URI or None)


def mork_url_for(species: str):
    """Per-species MORK URL, with the same no-cross-species-fallback rule."""
    v = _env(f"MORK_URL_{species.upper()}")
    if v:
        return v
    return None if _any_explicit() else (settings.MORK_URL or None)


def list_targets() -> list[dict]:
    """Species and which endpoints they resolve to (no reachability probe)."""
    out = []
    for sp in species_list():
        out.append({
            "species": sp,
            "explicit": species_specific(sp),
            "neo4j_uri": neo4j_uri_for(sp),
            "mork_url": mork_url_for(sp),
        })
    return out


@lru_cache(maxsize=None)
def _neo4j_client_for_uri(uri: str):
    from backend.core.neo4j_client import Neo4jClient
    return Neo4jClient(uri=uri)


def get_neo4j_client(species: str):
    uri = neo4j_uri_for(species)
    return _neo4j_client_for_uri(uri) if uri else None


def get_mork_client(species: str):
    """Construct a MORK client for the species (its constructor probes the server)."""
    from backend.core.mork_client import MORKClient
    url = mork_url_for(species)
    if not url:
        return None
    return MORKClient(mork_url=url)
