"""Known sizes: how many nodes and edges each adapter really produces, per species
and dataset, learned from the console's own builds.

Every build saves what each adapter wrote (graph_info.json "adapter_counts") and
whether a size limit cut it short. When a build succeeds, its complete counts are
recorded here; a capped count never is, because it isn't the source's real size.
The record fills in over time: a full build makes every adapter known, and a build
of a few sources teaches the console just those.

Everything size-related reads this one record: each source's size when you pick
sources, "percent of known size" (turned into per-adapter caps when a build
launches), the size preview before you build, and the counts on a build's page.

Kept in the console's builds folder (builds_dir/sizes.json), not in build output
folders, so it outlives the clean-up of old builds.
"""
from __future__ import annotations

import json
import math
import os
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from backend.core.config import settings

_lock = threading.Lock()


class SizeError(ValueError):
    """Shown to the user as-is."""


def _store_path() -> Path:
    return settings.builds_dir / "sizes.json"


def _key(species: str, dataset: str) -> str:
    return f"{species}/{dataset}"


def _load() -> dict:
    try:
        data = json.loads(_store_path().read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save(data: dict) -> None:
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(tmp, path)


def known(species: str, dataset: str) -> Dict[str, dict]:
    """{adapter: {"nodes", "edges", "job_id", "built_at"}} for this species+dataset.
    nodes/edges is None for a kind the adapter doesn't write."""
    return dict(_load().get(_key(species, dataset), {}))


def record_build(job) -> int:
    """Learn the complete per-adapter counts of a succeeded build. Returns how many
    adapters it taught. Capped counts and builds without counts are ignored."""
    params = job.params or {}
    species, dataset = params.get("species"), params.get("dataset")
    if not species or not dataset or species.lower() == "all" or not job.output_dir:
        return 0
    try:
        info = json.loads((Path(job.output_dir) / "graph_info.json").read_text())
    except (OSError, ValueError):
        return 0
    counts = info.get("adapter_counts") or {}
    learned = {
        name: {"nodes": c.get("nodes"), "edges": c.get("edges"),
               "job_id": job.id, "built_at": job.finished_at or job.created_at}
        for name, c in counts.items()
        if isinstance(c, dict) and c.get("complete")
    }
    if not learned:
        return 0
    with _lock:
        data = _load()
        data.setdefault(_key(species, dataset), {}).update(learned)
        _save(data)
    return len(learned)


def selected_adapters(species: str, dataset: str, include: Optional[List[str]]) -> List[str]:
    """The adapters a build will run: the chosen ones, or every adapter in the config."""
    if include:
        return list(include)
    from backend.core.console import config_introspect as ci

    return [a["name"] for a in ci.list_adapters(species, dataset).get("adapters", [])]


def _part(n: Optional[int], percent: float) -> int:
    # 0 means "no cap" to the CLI, which is right for a kind the adapter doesn't
    # write or never produced; anything real keeps at least one row.
    if not n:
        return 0
    return max(1, math.ceil(n * percent / 100))


def percent_caps(species: str, dataset: str, adapters: List[str],
                 percent: float) -> Tuple[Dict[str, dict], List[str]]:
    """Per-adapter caps for "percent of known size": ({adapter: {"nodes", "edges"}},
    adapters whose size isn't known yet)."""
    sizes = known(species, dataset)
    caps, unknown = {}, []
    for name in adapters:
        size = sizes.get(name)
        if size is None:
            unknown.append(name)
            continue
        caps[name] = {"nodes": _part(size.get("nodes"), percent),
                      "edges": _part(size.get("edges"), percent)}
    return caps, unknown


def resolve(req) -> Optional[Dict[str, dict]]:
    """The per-adapter caps a build request means, or None for none. A percentage
    becomes caps from the known sizes; it needs every selected adapter known."""
    if getattr(req, "adapter_limits", None):
        return req.adapter_limits
    percent = getattr(req, "percent", None)
    if not percent:
        return None
    if getattr(req, "limit", 0):
        raise SizeError("Choose either rows per adapter or a percentage, not both.")
    if not req.species or req.species.lower() == "all" or not req.dataset:
        raise SizeError("A percentage needs one species and dataset.")
    adapters = selected_adapters(req.species, req.dataset, req.include_adapters)
    caps, unknown = percent_caps(req.species, req.dataset, adapters, percent)
    if unknown:
        shown = ", ".join(unknown[:6]) + (f" and {len(unknown) - 6} more" if len(unknown) > 6 else "")
        raise SizeError(
            f"A percentage needs each selected source's full size, and these aren't known yet: "
            f"{shown}. Build them once without a size limit, or use rows per adapter."
        )
    return caps
