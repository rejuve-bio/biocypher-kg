"""Per-species database endpoints for the Explore tab.

Neo4j Community cannot host a database per species, so each species runs its own
Neo4j and MORK instance (its own port), and the console is told each species'
endpoints via env vars:

    NEO4J_URI_<SPECIES>   e.g. NEO4J_URI_HSA=bolt://localhost:7687
    MORK_URL_<SPECIES>    e.g. MORK_URL_HSA=http://localhost:8432

Species names come from species_config.yaml. Setting any NEO4J_URI_<SP> /
MORK_URL_<SP> puts the console in per-species mode: each species resolves only to
its own endpoint with no cross-species fallback, and a species with no endpoint has
no instance (it is omitted from Explore, not shown mirroring another species). With
nothing per-species set, the global NEO4J_URI / MORK_URL is used as a single default
instance. Neo4j creds (NEO4J_USER / NEO4J_PASSWORD / NEO4J_DATABASE) are shared.
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
    """The instances the console is configured to explore, driven entirely by the env.

    - Per-species mode (any NEO4J_URI_<SP> / MORK_URL_<SP> set): one entry per species
      that actually has an endpoint. Unconfigured species are omitted, not shown as
      empty, so Explore reflects exactly what is configured.
    - Single-instance mode (only the global NEO4J_URI / MORK_URL): a single "default"
      entry, not one card per species.
    - Nothing configured: empty.

    Manage which instances appear purely by editing docker/console.env (add or remove
    NEO4J_URI_<SP> lines); no code or UI change needed.
    """
    if _any_explicit():
        out = []
        for sp in species_list():
            nu, mu = neo4j_uri_for(sp), mork_url_for(sp)
            if nu or mu:
                out.append({"species": sp, "explicit": True, "neo4j_uri": nu, "mork_url": mu})
        return out

    nu = settings.NEO4J_URI or None
    mu = settings.MORK_URL or None
    if not (nu or mu):
        return []
    return [{"species": "default", "explicit": False, "neo4j_uri": nu, "mork_url": mu}]


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
