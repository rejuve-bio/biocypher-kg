"""Each console instance's own config: the shipped config, plus this instance's edits.

The console image carries the config it was built with, read-only, in
``<REPO_ROOT>/.config-shipped``. The config the console and its builds read,
``<REPO_ROOT>/config``, is the instance's own copy (a Docker volume), so edits made
in the Config page stay with this instance: across restarts and image updates,
and never in a repo.

- ``sync()`` (at start-up) brings every file this instance hasn't edited up to the
  shipped version, adds new ones and removes ones no longer shipped. Edited files
  keep this instance's version.
- ``mark_edited()`` records an edit (with the shipped version it was made against,
  so the page can say when the shipped file changed since); saving a file back to
  exactly the shipped content un-marks it.
- ``reset()`` puts the shipped version back.

Run from a checkout without the shipped copy (development), ``config/`` is the
repo's own folder: edits go there, as before, and none of this applies.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from backend.core.config import settings

_lock = threading.Lock()
EDITS_FILE = ".console-edits.json"
# Never copied, compared or removed: Python caches, temp files the build and editor
# write next to the configs, and the edits record itself.
_SKIP_NAMES = {"__pycache__", EDITS_FILE}


def shipped_dir() -> Path:
    return settings.repo_root_path / ".config-shipped"


def live_dir() -> Path:
    return settings.repo_root_path / "config"


def managed() -> bool:
    """True when this instance has its own config copy (the shipped copy exists)."""
    return shipped_dir().is_dir()


def _sha(path: Path) -> Optional[str]:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _skip(rel: Path) -> bool:
    name = rel.name
    return (any(part in _SKIP_NAMES for part in rel.parts) or name.endswith(".console-tmp")
            or (len(rel.parts) == 1 and name.startswith("tmp") and name.endswith(".yaml")))


def _files(root: Path):
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root)
            if not _skip(rel):
                yield rel


def _load() -> Dict[str, dict]:
    try:
        data = json.loads((live_dir() / EDITS_FILE).read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save(data: Dict[str, dict]) -> None:
    path = live_dir() / EDITS_FILE
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(tmp, path)


def _rel(path: Path) -> Optional[str]:
    try:
        return str(Path(path).resolve().relative_to(live_dir().resolve()))
    except ValueError:
        return None


def sync() -> dict:
    """Bring this instance's config up to the shipped one, keeping its edits.
    Returns {"updated", "added", "removed", "kept"} (relative paths)."""
    out = {"updated": [], "added": [], "removed": [], "kept": []}
    if not managed():
        return out
    shipped, live = shipped_dir(), live_dir()
    with _lock:
        edits = _load()
        for rel in _files(shipped):
            key = str(rel)
            src, dst = shipped / rel, live / rel
            if key in edits:
                out["kept"].append(key)
                continue
            if not dst.exists():
                out["added"].append(key)
            elif _sha(dst) != _sha(src):
                out["updated"].append(key)
            else:
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        for rel in list(_files(live)):
            key = str(rel)
            if key not in edits and not (shipped / rel).exists():
                (live / rel).unlink(missing_ok=True)
                out["removed"].append(key)
    return out


def mark_edited(path: Path) -> None:
    """Record that this instance edited ``path`` (a file under config/). Saving it
    back to exactly the shipped content un-marks it."""
    if not managed():
        return
    key = _rel(path)
    if key is None:
        return
    shipped = shipped_dir() / key
    with _lock:
        edits = _load()
        if shipped.exists() and _sha(shipped) == _sha(Path(path)):
            edits.pop(key, None)
        else:
            edits[key] = {"shipped_sha": _sha(shipped) if shipped.exists() else None,
                          "edited_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        _save(edits)


def status(path: Path) -> dict:
    """{"managed", "edited", "shipped_changed", "edited_at"} for a config file:
    whether this instance has its own config, whether it edited this file, and
    whether the shipped file changed since that edit."""
    out = {"managed": managed(), "edited": False, "shipped_changed": False, "edited_at": None}
    if not out["managed"]:
        return out
    key = _rel(path)
    edit = _load().get(key) if key else None
    if edit:
        shipped = shipped_dir() / key
        out.update(edited=True, edited_at=edit.get("edited_at"),
                   shipped_changed=(_sha(shipped) if shipped.exists() else None) != edit.get("shipped_sha"))
    return out


def reset(path: Path) -> None:
    """Put the shipped version of ``path`` back and forget this instance's edit."""
    if not managed():
        raise ValueError("this console has no shipped config to go back to")
    key = _rel(path)
    shipped = shipped_dir() / key if key else None
    if not shipped or not shipped.exists():
        raise ValueError("no shipped version of this file")
    with _lock:
        shutil.copy2(shipped, live_dir() / key)
        edits = _load()
        edits.pop(key, None)
        _save(edits)
