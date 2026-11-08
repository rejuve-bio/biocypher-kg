"""Find the folder someone chose in Finder, for the folder fields in Settings.

Browse… opens the computer's own Finder window (the browser's folder picker). A
browser never tells a page where a folder is, only its name and what's in it, so the
console finds it: it looks for a folder with that name and the same contents among
the folders it can open.

The console runs in Docker and only sees folders shared into its container. The
compose files share what a Finder sidebar shows: Desktop, Documents, Downloads and
external drives (/Volumes), plus the console's default data folder. They're shared
writable so the data and Builds folders can be anywhere there; the console only
writes into those two, and only reads folders chosen as source data or a dbSNP
cache. Not the whole home folder: Docker Desktop passes every file change in a
shared folder to its VM, and a home folder's constant changes crash the engine.
Outside Docker, your home folder and drives are searched. Searching reads names and
sizes only: nothing is opened or changed.
"""
from __future__ import annotations

import os
import time
import unicodedata
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from backend.core.config import settings

SEARCH_SECONDS = 10.0  # give up looking after this long
MAX_DEPTH = 8  # folders deep inside a place
# Never worth searching: app support files, caches, code dependencies.
_SKIP = {"Library", "node_modules", "__pycache__", "site-packages", "$RECYCLE.BIN"}
# The container's own plumbing, not folders you mounted: kernel filesystems, the
# image's root, and the console's own folders (config, build history).
_PLUMBING_FS = {"proc", "sysfs", "tmpfs", "devpts", "devtmpfs", "mqueue", "cgroup", "cgroup2",
                "overlay", "securityfs", "debugfs", "pstore", "bpf", "tracefs", "hugetlbfs",
                "fusectl", "configfs", "nsfs", "autofs", "binfmt_misc"}
_PLUMBING_DIRS = ("/proc", "/sys", "/dev", "/app")


class FolderError(ValueError):
    """Shown to the user as-is."""


def in_docker() -> bool:
    return Path("/.dockerenv").exists()


def _plumbing(mount_point: str, fstype: str) -> bool:
    return (mount_point == "/" or fstype in _PLUMBING_FS
            or any(mount_point == d or mount_point.startswith(d + "/") for d in _PLUMBING_DIRS))


def mounted_folders(mountinfo: str) -> List[str]:
    """Folders mounted into this container, from /proc/self/mountinfo. Each line is
    "id parent major:minor root mount-point options [optional...] - fstype source ..."."""
    out = []
    for line in mountinfo.splitlines():
        left, sep, right = line.partition(" - ")
        fields, after = left.split(), right.split()
        if not sep or len(fields) < 5 or not after:
            continue
        mount_point = fields[4].replace("\\040", " ")
        if not _plumbing(mount_point, after[0]):
            out.append(mount_point)
    return out


def _mounted() -> List[str]:
    try:
        info = Path("/proc/self/mountinfo").read_text()
    except OSError:
        return []
    return [m for m in mounted_folders(info) if Path(m).is_dir()]  # not single files


def _data_root() -> Optional[str]:
    return str(Path(settings.DATA_ROOT)) if settings.DATA_ROOT else None


# Finder sidebar folders, by name, and the order places are listed in.
_SIDEBAR = {"Desktop": "desktop", "Documents": "documents", "Downloads": "downloads"}
_ORDER = ["desktop", "documents", "downloads", "home", "data", "drive", "folder"]
VOLUMES = "/Volumes"  # where macOS mounts external drives


def _icon(path: str) -> str:
    if path == _data_root():
        return "data"
    if not in_docker() and path == str(Path.home()):
        return "home"
    return _SIDEBAR.get(Path(path).name, "folder")


def _label(path: str) -> str:
    return {"data": "Console data folder", "home": "Home"}.get(_icon(path)) or Path(path).name or path


def _drives(volumes: str) -> List[dict]:
    """External drives: the real folders in /Volumes (not "Macintosh HD", a link to /)."""
    out = []
    try:
        for c in sorted(Path(volumes).iterdir(), key=lambda c: c.name.lower()):
            if not c.name.startswith(".") and not c.is_symlink() and c.is_dir():
                out.append({"path": str(c), "label": c.name, "icon": "drive"})
    except OSError:
        pass
    return out


def places() -> List[dict]:
    """The sidebar: where you can browse, as {path, label, icon}. In Docker, only the
    folders shared into the console."""
    data = _data_root()
    if in_docker():
        candidates = _mounted()
    else:
        home = Path.home()
        candidates = [str(home / name) for name in _SIDEBAR] + [str(home), VOLUMES]
    if data:
        candidates.append(data)
    out: List[dict] = []
    seen = set()
    for path in candidates:
        if path == VOLUMES:
            out.extend(d for d in _drives(path) if d["path"] not in seen)
            seen.update(d["path"] for d in out)
            continue
        if path in seen or not Path(path).is_dir():
            continue
        seen.add(path)
        out.append({"path": path, "label": _label(path), "icon": _icon(path)})
    return sorted(out, key=lambda p: (_ORDER.index(p["icon"]), p["label"].lower()))


def _allowed(path: Path, allowed_roots: List[str]) -> bool:
    if not in_docker():
        return True
    p = str(path)
    return any(p == r or p.startswith(r.rstrip("/") + "/") for r in allowed_roots)


def _writable(path: Path) -> bool:
    # A read-only share fails W_OK too (EROFS), not just file permissions.
    return os.access(path, os.W_OK)


def _nfc(name: str) -> str:
    # macOS stores names decomposed (NFD); browsers send them composed (NFC).
    return unicodedata.normalize("NFC", name)


def _contents(path: Path) -> Optional[Dict[str, Tuple[bool, Optional[int]]]]:
    """name -> (is_folder, size) for everything directly in ``path``."""
    out: Dict[str, Tuple[bool, Optional[int]]] = {}
    try:
        with os.scandir(path) as it:
            for e in it:
                try:
                    is_dir = e.is_dir()
                    out[_nfc(e.name)] = (True, None) if is_dir else (False, e.stat().st_size)
                except OSError:
                    continue
    except OSError:
        return None
    return out


def _same_contents(path: Path, entries: List[dict], count: Optional[int]) -> bool:
    """Does this folder hold what the browser saw? ``entries`` may be a sample of
    the folder (``count`` is the full number, when the browser knows it)."""
    found = _contents(path)
    if found is None or (count is not None and len(found) != count):
        return False
    for e in entries:
        f = found.get(_nfc(e["name"]))
        if f is None or f[0] != bool(e.get("dir")):
            return False
        if not e.get("dir") and e.get("size") is not None and f[1] != e["size"]:
            return False
    return True


def _named(name: str, start: str, deadline: float) -> Iterator[Path]:
    """Folders called ``name`` in or under ``start`` (not inside hidden folders)."""
    root = Path(start)
    if _nfc(root.name) == name:
        yield root
    base_depth = len(root.parts)
    for dirpath, dirnames, _ in os.walk(start):
        if time.monotonic() > deadline:
            return
        dirnames[:] = [d for d in dirnames if not d.startswith(".") and d not in _SKIP]
        if len(Path(dirpath).parts) - base_depth >= MAX_DEPTH:
            dirnames[:] = []
        for d in dirnames:
            if _nfc(d) == name:
                yield Path(dirpath) / d


def _search_order(all_places: List[dict]) -> List[str]:
    # The console data folder first (most likely), then the rest as listed.
    first = [pl["path"] for pl in all_places if pl["icon"] == "data"]
    return first + [pl["path"] for pl in all_places if pl["icon"] != "data"]


def locate(name: str, entries: List[dict], count: Optional[int] = None,
           kind: Optional[str] = None, species: Optional[str] = None) -> dict:
    """Where is the folder someone chose in Finder? Returns {"matches": [paths]}:
    one normally, more if identical copies exist. Then it must be what the field
    is for (data_checks): kind="dbsnp" a real dbSNP cache, kind="source" that
    species' source data, kind="writable" a folder the console can keep its data in,
    kind="builds" a folder it can save builds in."""
    name = _nfc((name or "").strip())
    if not name:
        raise FolderError("Choose a folder.")
    all_places = places()
    allowed = [pl["path"] for pl in all_places]
    deadline = time.monotonic() + SEARCH_SECONDS
    matches: List[Path] = []
    seen = set()
    for start in _search_order(all_places):
        for cand in _named(name, start, deadline):
            real = cand.resolve()
            if real in seen or not _allowed(real, allowed):
                continue
            seen.add(real)
            if _same_contents(real, entries, count):
                matches.append(real)
        if time.monotonic() > deadline:
            break

    if not matches:
        if in_docker():
            raise FolderError(
                f"The console can't open “{name}”. It can use folders in your Desktop, "
                "Documents or Downloads, on an external drive, or in the console data "
                "folder. Move or copy it to one of those, then choose it again."
            )
        raise FolderError(f"Couldn't find “{name}” on this computer. Type its path instead.")
    from backend.core.console import data_checks

    if kind == "dbsnp":
        problems = [data_checks.check_dbsnp_root(m)[1] for m in matches]
    elif kind == "source":
        problems = [data_checks.check_species_folder(species or "", m)["problem"] for m in matches]
    elif kind in ("writable", "builds"):
        check = data_checks.check_data_folder if kind == "writable" else data_checks.check_builds_folder
        problems = [None if _writable(m) else
                    (f"The console can't save in “{name}”. Choose a folder in your Desktop, "
                     "Documents or Downloads, or on an external drive.")
                    for m in matches]
        problems = [p or check(m) for p, m in zip(problems, matches)]
    else:
        problems = [None] * len(matches)
    good = [m for m, p in zip(matches, problems) if p is None]
    if not good:
        raise FolderError(f"“{name}” can't be used here. {problems[0]}")
    return {"matches": [str(m) for m in good]}
