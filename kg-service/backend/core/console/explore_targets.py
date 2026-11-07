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
import os
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


def _env(name: str):
    v = os.environ.get(name)
    v = v.strip() if v else ""
    return v or None


def species_specific(species: str) -> bool:
    """True if this species has at least one endpoint set explicitly (UI store or env)."""
    o = runtime_config.species_override(species)
    return bool(o.get("neo4j_uri") or o.get("mork_url")
                or _env(f"NEO4J_URI_{species.upper()}") or _env(f"MORK_URL_{species.upper()}"))


def _default_uri(name: str):
    """A single default instance only when one is explicitly configured (environment
    or an older saved setting), never the built-in placeholder address."""
    saved = (runtime_config.load().get("global") or {}).get(name.lower())
    return saved or _env(name)


def _any_explicit() -> bool:
    """True if any species has its own instance (per-species mode)."""
    return any(species_specific(sp) for sp in species_list())


def neo4j_uri_for(species: str):
    """Per-species Neo4j URI. Never falls back to another species' instance: in
    per-species mode an unconfigured species returns None (not configured); the
    global default is used only in single-instance mode (nothing per-species set)."""
    v = runtime_config.species_override(species).get("neo4j_uri") or _env(f"NEO4J_URI_{species.upper()}")
    if v:
        return v
    return None if _any_explicit() else _default_uri("NEO4J_URI")


def mork_url_for(species: str):
    """Per-species MORK URL, with the same no-cross-species-fallback rule."""
    v = runtime_config.species_override(species).get("mork_url") or _env(f"MORK_URL_{species.upper()}")
    if v:
        return v
    return None if _any_explicit() else _default_uri("MORK_URL")


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

    nu = _default_uri("NEO4J_URI")
    mu = _default_uri("MORK_URL")
    if not (nu or mu):
        return []
    return [{"species": "default", "explicit": False, "neo4j_uri": nu, "mork_url": mu}]


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
