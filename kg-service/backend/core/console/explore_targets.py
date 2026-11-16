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
nothing per-species set, a NEO4J_URI / MORK_URL set in the environment is used as a
single default instance (never the built-in defaults). Each species' Neo4j login is
its own; see connections.neo4j_login.
"""
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from backend.core.console import config_introspect as ci
from backend.core.console import runtime_config


def species_list() -> list[str]:
    try:
        return [e["species"] for e in ci.list_species_and_datasets()]
    except Exception:  # noqa: BLE001 - config missing/unreadable
        return []


def species_specific(species: str) -> bool:
    """True if this species has at least one database set in Settings."""
    o = runtime_config.species_override(species)
    return bool(o.get("neo4j_uri") or o.get("mork_url"))


def neo4j_uri_for(species: str):
    """The species' own Neo4j, as set in Settings, or None. There is no default or
    shared instance: a species without one has no Neo4j."""
    return runtime_config.species_override(species).get("neo4j_uri") or None


def mork_url_for(species: str):
    """The species' own MORK, as set in Settings, or None (no default instance)."""
    return runtime_config.species_override(species).get("mork_url") or None


def list_targets() -> list[dict]:
    """The databases Explore shows: one entry per species that has one set in
    Settings > Species databases. Species without one are left out."""
    out = []
    for sp in species_list():
        nu, mu = neo4j_uri_for(sp), mork_url_for(sp)
        if nu or mu:
            out.append({"species": sp, "explicit": True, "neo4j_uri": nu, "mork_url": mu})
    return out


_LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")


def connect_address(address):
    """The address to actually connect to. People type and see "localhost" for a
    database on their computer, but inside Docker localhost is the console itself,
    so the connection goes to host.docker.internal instead. Only connections use
    this; what's saved and shown stays as typed."""
    if not address or not Path("/.dockerenv").exists():
        return address
    p = urlparse(address)
    if p.hostname not in _LOCAL_HOSTS:
        return address
    host = "[::1]" if p.hostname == "::1" else p.hostname
    netloc = p.netloc.replace(host, "host.docker.internal", 1)
    return urlunparse(p._replace(netloc=netloc))


@lru_cache(maxsize=None)
def _neo4j_client_for(uri: str, user: str, password: str, database: str):
    from backend.core.neo4j_client import Neo4jClient
    return Neo4jClient(uri=uri, user=user, password=password, database=database)


def get_neo4j_client(species: str):
    """A client for the species' Neo4j, logged in with that species' own login."""
    from backend.core.console import connections  # local: connections imports this module

    login = connections.neo4j_login(species)
    if not login:
        return None
    return _neo4j_client_for(connect_address(login["uri"]), login["user"], login["password"],
                             login["database"])


def get_mork_client(species: str):
    """Construct a MORK client for the species (its constructor probes the server),
    reading that species' own version metadata (see job_runner.archive_base_for)."""
    from backend.core.console import job_runner
    from backend.core.mork_client import MORKClient
    url = mork_url_for(species)
    if not url:
        return None
    return MORKClient(mork_url=connect_address(url),
                      archive_base=job_runner.archive_base_for(species))
