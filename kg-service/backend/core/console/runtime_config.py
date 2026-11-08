"""Runtime, UI-editable Console configuration.

Everything a researcher sets lives in the Settings page, never an env file: each
species' databases, the data and builds folders, and how builds run. This module
keeps those settings in a small JSON file on the persisted builds volume and
projects them onto the process-wide ``settings`` object:

    saved in Settings  >  unset

Folders and databases have no defaults: what the console writes to or connects to is
only ever what someone chose here. ``require()`` names what's missing, and every
build, download, dbSNP prepare and load checks it first. ``apply()`` projects the
saved values (and clears any that were cleared), so the many call sites that read
``settings.X`` at call time honour Settings with no change.

Secrets note: the Neo4j password is stored here in plain text (file mode 600). That
is the deliberate trade for a repo-free, fully UI-driven deploy, and the Console is
assumed to be bound to loopback (see docker-compose). Do not expose it unguarded.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from backend.core.config import settings

_LOCK = threading.RLock()
_VERSION = 0

# Store key -> Settings attribute. A present, non-empty store value is projected
# onto settings by apply(); integer-typed settings are coerced.
_GLOBAL_KEYS: Dict[str, str] = {
    "neo4j_uri": "NEO4J_URI",
    "neo4j_user": "NEO4J_USER",
    "neo4j_password": "NEO4J_PASSWORD",
    "neo4j_database": "NEO4J_DATABASE",
    "mork_url": "MORK_URL",
    "data_root": "DATA_ROOT",
    "output_root": "OUTPUT_ROOT",
    "neo4j_memory": "NEO4J_MEMORY",
    "max_concurrent_builds": "MAX_CONCURRENT_BUILDS",
    "max_output_builds": "MAX_OUTPUT_BUILDS",
    "max_build_history": "MAX_BUILD_HISTORY",
    # Not UV_BIN: the program that runs builds is fixed when the console is
    # installed. Letting a request change it would let it run any program.
}
_INT_KEYS = {"max_concurrent_builds", "max_output_builds", "max_build_history"}

# What each setting is when nothing is saved (unset, for folders and memory): taken
# once, before anything is applied, so clearing a field takes effect right away.
_DEFAULTS: Dict[str, Any] = {attr: getattr(settings, attr) for attr in _GLOBAL_KEYS.values()}


def store_path() -> Path:
    """Location of the settings file (on the persisted builds volume)."""
    return settings.builds_dir / "console_settings.json"


def load() -> Dict[str, Any]:
    """Return the raw store, or {} when absent/unreadable."""
    with _LOCK:
        p = store_path()
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text()) or {}
        except (OSError, ValueError):
            return {}


def _write(data: Dict[str, Any]) -> None:
    p = store_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    tmp.replace(p)
    try:
        os.chmod(p, 0o600)  # holds the Neo4j password; keep it owner-only
    except OSError:
        pass


def version() -> int:
    """Monotonic counter bumped on every save; clients rebuild when it changes."""
    return _VERSION


def _bump() -> None:
    global _VERSION
    _VERSION += 1


def _coerce(key: str, value: Any) -> Any:
    if key in _INT_KEYS:
        return int(value)  # may raise; caller guards
    return value


def apply(data: Optional[Dict[str, Any]] = None) -> None:
    """Project the saved values onto the process-wide settings object; a value that
    isn't saved (or was cleared) gets its default back."""
    g = (data if data is not None else load()).get("global", {}) or {}
    for key, attr in _GLOBAL_KEYS.items():
        value = _DEFAULTS[attr]
        if key in g and g[key] not in (None, ""):
            try:
                value = _coerce(key, g[key])
            except (TypeError, ValueError):
                pass  # ignore a malformed value rather than break startup
        setattr(settings, attr, value)


def builds_folder() -> Optional[str]:
    """Where builds are saved: the Builds folder chosen in Settings, or None."""
    return settings.OUTPUT_ROOT or None


class SetupError(ValueError):
    """Something must be set in Settings first; the message is shown as-is."""


# What must be chosen in Settings before the console uses it: no defaults.
_REQUIRED = {
    "data": ("DATA_ROOT", "console data folder"),
    "builds": ("OUTPUT_ROOT", "Builds folder"),
    "memory": ("NEO4J_MEMORY", "Neo4j memory"),
}


def missing(*what: str) -> list:
    """The required settings among ``what`` (data, builds, memory) not set yet."""
    return [_REQUIRED[k][1] for k in what if not getattr(settings, _REQUIRED[k][0], None)]


def require(*what: str) -> None:
    """Raise SetupError naming whatever of ``what`` isn't set in Settings yet."""
    gaps = missing(*what)
    if gaps:
        raise SetupError(f"set in Settings first: {', '.join(gaps)}")


def rebuild_clients() -> None:
    """Drop cached DB clients so the next use reconnects with current settings."""
    try:
        from backend.core.console import explore_targets as et
        et._neo4j_client_for.cache_clear()
    except Exception:  # noqa: BLE001 - never let a reload break on cache internals
        pass


def _deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    """Merge patch into base. Nested dicts merge; a None value clears a key."""
    for k, v in patch.items():
        if v is None:
            base.pop(k, None)
        elif isinstance(v, dict):
            node = base.get(k)
            if not isinstance(node, dict):
                node = {}
            base[k] = _deep_merge(node, v)
        else:
            base[k] = v
    return base


def save(patch: Dict[str, Any]) -> Dict[str, Any]:
    """Merge ``patch`` into the store, persist, apply, and reconnect clients."""
    with _LOCK:
        data = load()
        _deep_merge(data, patch)
        _write(data)
        apply(data)
        _bump()
        rebuild_clients()
        return data


def data_locations() -> Dict[str, Any]:
    """Folders chosen in Settings > Data locations: per-species source-data folders
    and the dbSNP cache root, for data the user already has on disk. Empty when unset,
    so the console's defaults apply."""
    d = load().get("data") or {}
    return {
        "input_dirs": {k: v for k, v in (d.get("input_dirs") or {}).items() if v},
        "dbsnp_root": d.get("dbsnp_root") or None,
    }


def species_override(species: str) -> Dict[str, Any]:
    """Per-species endpoint overrides set via the UI. Keys are stored upper-case,
    but match case-insensitively so a caller's casing never matters."""
    target = species.upper()
    for key, val in (load().get("species") or {}).items():
        if key.upper() == target and isinstance(val, dict):
            return val
    return {}


def has_species_overrides() -> bool:
    return any(
        v.get("neo4j_uri") or v.get("mork_url")
        for v in (load().get("species") or {}).values()
        if isinstance(v, dict)
    )


def effective(masked: bool = True) -> Dict[str, Any]:
    """The effective config for the Settings UI (saved over defaults). Folders say
    what you chose (None when you haven't) apart from what's in use, so the page never
    shows a default as if it had been chosen.

    With masked=True the password is replaced by a ``password_set`` boolean.
    """
    g = load().get("global", {}) or {}

    def val(key: str, attr: str) -> Any:
        v = g.get(key)
        if v not in (None, ""):
            return v
        return getattr(settings, attr, None)

    out: Dict[str, Any] = {
        "neo4j": {
            "uri": val("neo4j_uri", "NEO4J_URI"),
            "user": val("neo4j_user", "NEO4J_USER"),
            "database": val("neo4j_database", "NEO4J_DATABASE"),
            "password_set": bool(val("neo4j_password", "NEO4J_PASSWORD")),
        },
        "mork": {"url": val("mork_url", "MORK_URL")},
        "build": {
            # Required, no defaults: None until chosen in Settings.
            "data_root": g.get("data_root") or None,
            "output_root": g.get("output_root") or None,
            "neo4j_memory": g.get("neo4j_memory") or None,
            # What a build still needs set here (empty when ready).
            "missing": missing("data", "builds"),
            "max_concurrent_builds": val("max_concurrent_builds", "MAX_CONCURRENT_BUILDS"),
            "max_output_builds": val("max_output_builds", "MAX_OUTPUT_BUILDS"),
            "max_build_history": val("max_build_history", "MAX_BUILD_HISTORY"),
        },
        "species": load().get("species") or {},
        "data": data_locations(),
    }
    if not masked:
        out["neo4j"]["password"] = val("neo4j_password", "NEO4J_PASSWORD")
    return out
