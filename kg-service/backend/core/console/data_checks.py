"""Is a chosen folder really what the code reads? Checked before the console uses it.

- dbSNP cache: <root>/<variant>/dbsnp_mapping.db must be the SQLite database
  scripts/update_dbsnp.py builds and biocypher_metta/processors/dbsnp_processor.py
  reads: an rsid_to_pos(rsid, chr, pos) table holding dbSNP rows, the (chr, pos)
  index position lookups need, and a version file that says it's dbSNP and which
  variant it is. Only the database form is accepted, never the older pickle form:
  loading a pickle from someone else's folder can run code.
- Species source data: the folder must hold at least one of the sources that
  species' adapters config declares, laid out the way the config expects (the
  same rule the build's data check uses), and those files must really be what
  their names say (a .gz is gzip, a .zip is zip...).
- Console data folder: empty, or already laid out like a console data folder, so
  builds (and the clean-up of old builds) never mix with other files.

All checks only read: names, a few bytes of a file's start, and one row.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import List, Optional

DBSNP_VARIANTS = ("common", "full")
SQLITE_HEADER = b"SQLite format 3\x00"

# The first bytes of a file of each kind (what the downloads and adapters produce).
_SIGNATURES = {
    ".gz": [b"\x1f\x8b"], ".bgz": [b"\x1f\x8b"], ".tgz": [b"\x1f\x8b"],
    ".zip": [b"PK\x03\x04", b"PK\x05\x06"],
    ".bz2": [b"BZh"],
    ".xz": [b"\xfd7zXZ\x00"],
    ".parquet": [b"PAR1"],
    ".db": [SQLITE_HEADER], ".sqlite": [SQLITE_HEADER],
}
MAX_FILES_CHECKED = 40  # enough to catch a wrong folder without reading everything


# ── dbSNP cache ────────────────────────────────────────────────────────────────

def _starts_with(path: Path, prefixes: List[bytes]) -> bool:
    try:
        with open(path, "rb") as f:
            head = f.read(max(len(p) for p in prefixes))
    except OSError:
        return False
    return any(head.startswith(p) for p in prefixes)


def check_dbsnp_variant(variant_dir: Path, variant: str) -> Optional[str]:
    """None if <variant_dir>/dbsnp_mapping.db is a usable dbSNP cache of that
    variant, else what's wrong with it (in plain words)."""
    db = Path(variant_dir) / "dbsnp_mapping.db"
    where = f"{variant}/dbsnp_mapping.db"
    if not db.is_file():
        return f"{where} is missing."
    if not _starts_with(db, [SQLITE_HEADER]):
        return f"{where} isn't a dbSNP cache database (wrong kind of file)."
    try:
        # Read-only and immutable: no locks, no writes, nothing changes on disk.
        conn = sqlite3.connect(db.resolve().as_uri() + "?mode=ro&immutable=1", uri=True,
                               timeout=2)
    except sqlite3.Error:
        return f"{where} can't be opened: it's damaged or not a database."
    try:
        conn.execute("PRAGMA query_only=ON")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(rsid_to_pos)")}
        if not cols:
            return f"{where} has no dbSNP table: it isn't a dbSNP cache."
        if not {"rsid", "chr", "pos"} <= cols:
            return f"{where} has the wrong layout: it isn't a dbSNP cache."
        row = conn.execute("SELECT rsid, chr, pos FROM rsid_to_pos LIMIT 1").fetchone()
        if row is None:
            return f"{where} is empty: the cache was never finished."
        rsid, _chr, pos = row
        if not (isinstance(rsid, str) and rsid.lower().startswith("rs") and isinstance(pos, int)):
            return f"{where} doesn't hold dbSNP entries (rsID, chromosome, position)."
        indexed = False
        for idx in conn.execute("PRAGMA index_list(rsid_to_pos)"):
            # The name comes from the file being checked: quote it safely.
            quoted = str(idx[1]).replace("'", "''")
            names = [r[2] for r in conn.execute(f"PRAGMA index_info('{quoted}')")]
            if names[:2] == ["chr", "pos"]:
                indexed = True
                break
        if not indexed:
            return f"{where} wasn't finished: it lacks the position index lookups need."
    except sqlite3.Error:
        return f"{where} can't be read: it's damaged or not a dbSNP cache."
    finally:
        conn.close()

    version = Path(variant_dir) / "dbsnp_version.json"
    if version.is_file():
        try:
            info = json.loads(version.read_text())
        except (OSError, ValueError):
            return f"{variant}/dbsnp_version.json is damaged."
        if not isinstance(info, dict) or info.get("processor", "dbsnp") != "dbsnp":
            return f"{variant}/dbsnp_version.json doesn't describe a dbSNP cache."
        if "common_only" in info and bool(info["common_only"]) != (variant == "common"):
            held = "common" if info["common_only"] else "full"
            return f"The {variant}/ folder holds the {held} cache: it's in the wrong folder."
    return None


def check_dbsnp_root(root: Path) -> tuple:
    """(usable variants, problem). A cache root holds common/ and/or full/; it's
    usable when at least one of them is a real cache."""
    root = Path(root)
    if not root.is_dir():
        return [], "Can't find this folder."
    ok, problems = [], []
    for variant in DBSNP_VARIANTS:
        if (root / variant).is_dir():
            problem = check_dbsnp_variant(root / variant, variant)
            if problem is None:
                ok.append(variant)
            else:
                problems.append(problem)
    if ok:
        return ok, None
    if problems:
        return [], problems[0]
    if root.name in DBSNP_VARIANTS and check_dbsnp_variant(root, root.name) is None:
        return [], (f"This is the {root.name}/ folder itself. Choose the folder that "
                    f"holds it: {root.parent}.")
    return [], ("No dbSNP cache here. Choose the folder that holds common/ or full/ "
                "(each with dbsnp_mapping.db).")


# ── Species source data ────────────────────────────────────────────────────────

def _wrong_kind(path: Path) -> Optional[str]:
    """A file whose name says one kind but whose content is another (or empty)."""
    suffix = path.suffix.lower()
    if suffix not in _SIGNATURES:
        return None
    try:
        if path.stat().st_size == 0:
            return f"{path.name} is empty."
    except OSError:
        return None
    if not _starts_with(path, _SIGNATURES[suffix]):
        return f"{path.name} isn't a real {suffix} file."
    return None


def _files_in(paths: List[Path]) -> List[Path]:
    out: List[Path] = []
    for p in paths:
        if p.is_file():
            out.append(p)
        elif p.is_dir():
            for root, dirs, files in os.walk(p):
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                out.extend(Path(root) / f for f in files if not f.startswith("."))
                if len(out) >= MAX_FILES_CHECKED:
                    break
        if len(out) >= MAX_FILES_CHECKED:
            break
    return out[:MAX_FILES_CHECKED]


def _sources(species: str) -> list:
    from backend.core.console import config_introspect as ci

    try:
        return ci.list_data_sources(species).get("sources", [])
    except Exception:  # noqa: BLE001 - no config: nothing is expected
        return []


def _examples(sources: list) -> str:
    names = []
    for s in sources:
        first = next(iter(s.get("local_paths") or []), None)
        names.append((first.split("/")[0] if first else s["key"]) + "/")
    unique = list(dict.fromkeys(names))
    return ", ".join(unique[:4])


def check_species_folder(species: str, folder: Path) -> dict:
    """{"found": [source names], "total": n, "problem": str|None} for a folder chosen
    as a species' source data."""
    from backend.core.console import job_runner as jr

    folder = Path(folder)
    sources = _sources(species)
    if not folder.is_dir():
        return {"found": [], "total": len(sources), "problem": "Can't find this folder."}
    if not sources:
        return {"found": [], "total": 0,
                "problem": f"{species} has no data sources in its config to check against."}
    dirs = jr._dirs_by_key(folder, sources)
    found, files = [], []
    for s in sources:
        hits = jr._source_paths(folder, s, dirs)
        if hits:
            found.append(s["name"])
            files.extend(hits)
    if not found:
        parent = folder.parent
        if parent != folder and any(jr._source_paths(parent, s, jr._dirs_by_key(parent, sources))
                                    for s in sources):
            return {"found": [], "total": len(sources), "problem": (
                f"This looks like one source's folder. Choose the folder that holds it: {parent}.")}
        return {"found": [], "total": len(sources), "problem": (
            f"This folder has none of the files or folders the {species} adapters read "
            f"(such as {_examples(sources)}).")}
    for f in _files_in(files):
        bad = _wrong_kind(f)
        if bad:
            return {"found": found, "total": len(sources),
                    "problem": f"{bad} The {species} adapters can't read it."}
    return {"found": found, "total": len(sources), "problem": None}


# ── The console's data folder ──────────────────────────────────────────────────

def _console_layout() -> set:
    from backend.core.console import config_introspect as ci

    return {"input", "archives", "configs", "dbsnp", *ci.WRITER_TYPES}


def check_data_folder(folder: Path) -> Optional[str]:
    """None if the console can keep its data here: an empty folder, or one already
    laid out like a console data folder (input/, metta/, dbsnp/...)."""
    folder = Path(folder)
    if not folder.is_dir():
        return "Can't find this folder."
    if not os.access(folder, os.W_OK):
        return "The console can't save in this folder."
    try:
        entries = [e for e in folder.iterdir() if not e.name.startswith(".")]
    except OSError:
        return "The console can't open this folder."
    if not entries:
        return None
    layout = _console_layout()
    if any(e.is_dir() and e.name in layout for e in entries):
        return None
    return ("This folder already holds other things. Choose an empty folder, or one the "
            "console already uses (it has folders like input/ and metta/).")


def check_builds_folder(folder: Path) -> Optional[str]:
    """None if the console can save builds here: an empty folder, or one that already
    holds builds (metta/, neo4j/...) or is a console data folder. Old builds are
    cleaned out of it, so a folder that holds other things is refused."""
    from backend.core.console import config_introspect as ci

    folder = Path(folder)
    if not folder.is_dir():
        return "Can't find this folder."
    if not os.access(folder, os.W_OK):
        return "The console can't save in this folder."
    try:
        entries = [e for e in folder.iterdir() if not e.name.startswith(".")]
    except OSError:
        return "The console can't open this folder."
    if not entries or any(e.is_dir() and e.name in _console_layout() for e in entries):
        return None
    return ("This folder already holds other things. Choose an empty folder, or one that "
            f"already has your builds (folders like {', '.join(ci.WRITER_TYPES[:2])}/).")
