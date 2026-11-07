"""Runtime Console settings: read, save, and test DB + build configuration.

Backs the Settings page so a prebuilt image (which ships without docker/console.env)
can be pointed at databases and tuned entirely from the browser. Values are held by
backend.core.console.runtime_config and layered over the environment.
"""
import os
import threading
from pathlib import Path
from typing import Literal, Optional, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.core.config import settings
from backend.core.console import runtime_config
from backend.core.console import explore_targets as et

router = APIRouter(prefix="/api/console", tags=["Console"])


# ── Request models ────────────────────────────────────────────────────────────
# All fields optional: a PUT is a partial patch. Send null to clear a stored value
# (the env/default then shows through); omit a field to leave it unchanged. The
# password is the one exception: an empty string is ignored so the stored secret is
# never wiped by a form that simply did not re-enter it.

class Neo4jSettings(BaseModel):
    uri: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


class MorkSettings(BaseModel):
    url: Optional[str] = None


class BuildSettings(BaseModel):
    data_root: Optional[str] = None
    max_concurrent_builds: Optional[int] = Field(default=None, ge=1, le=64)
    max_output_builds: Optional[int] = Field(default=None, ge=1, le=1000)
    max_build_history: Optional[int] = Field(default=None, ge=1, le=10000)


class SpeciesEndpoint(BaseModel):
    neo4j_uri: Optional[str] = None
    mork_url: Optional[str] = None


class DataLocations(BaseModel):
    # species -> folder already holding that species' source files (null/"" clears it).
    input_dirs: Optional[Dict[str, Optional[str]]] = None
    # Root of a dbSNP cache you already have (contains common/ and/or full/).
    dbsnp_root: Optional[str] = None


class SettingsPatch(BaseModel):
    neo4j: Optional[Neo4jSettings] = None
    mork: Optional[MorkSettings] = None
    build: Optional[BuildSettings] = None
    # species map; a null value removes that species' overrides entirely.
    species: Optional[Dict[str, Optional[SpeciesEndpoint]]] = None
    data: Optional[DataLocations] = None


class TestRequest(BaseModel):
    target: str  # "neo4j" | "mork"
    # Optional overrides to test values before saving; fall back to effective config.
    uri: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    url: Optional[str] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

def _effective_species() -> Dict[str, dict]:
    """Per-species endpoints as the console uses them: saved here, else from the
    environment (NEO4J_URI_<SP> / MORK_URL_<SP>). Showing both means the Settings page
    reflects every connection in use, and a save makes them console-owned."""
    out: Dict[str, dict] = {}
    for sp in et.species_list():
        key = sp.upper()
        saved = runtime_config.species_override(sp)
        neo = saved.get("neo4j_uri") or et._env(f"NEO4J_URI_{key}")
        mork = saved.get("mork_url") or et._env(f"MORK_URL_{key}")
        if neo or mork:
            out[key] = {"neo4j_uri": neo, "mork_url": mork}
    return out


def _settings_response() -> dict:
    eff = runtime_config.effective(masked=True)
    eff["species"] = _effective_species()
    eff["available_species"] = et.species_list()
    return eff


@router.get("/settings")
def get_settings():
    """Effective settings (store over env), password masked, plus known species."""
    return _settings_response()


# Where the console must never save builds (or delete old ones from).
_SYSTEM_DIRS = ("/", "/app", "/bin", "/boot", "/dev", "/etc", "/lib", "/proc", "/root",
                "/run", "/sbin", "/sys", "/usr", "/var", "/System", "/Library",
                "/Applications", "/private", "/Users", "/home", "/Volumes")


def _check_folder(path: str) -> str:
    """A folder field's value: a full path, tidied (no "..", no trailing slash)."""
    p = Path(path.strip()).expanduser()
    if not p.is_absolute():
        raise HTTPException(status_code=400, detail=f"Use a full folder path (starting with /), not “{path}”.")
    return os.path.normpath(str(p))


def _check_data_root(path: str) -> str:
    """The console data folder: an existing folder the console can save in, and not a
    system or top-level folder (old builds are deleted from it)."""
    p = _check_folder(path)
    real = os.path.realpath(p)
    if real in _SYSTEM_DIRS or os.path.dirname(real) in ("/", "/Users", "/home", "/Volumes"):
        raise HTTPException(status_code=400, detail=(
            f"“{p}” can't be the console data folder. Choose a folder made for it, "
            "such as one inside the current console data folder."))
    if not os.path.isdir(real) or not os.access(real, os.W_OK):
        raise HTTPException(status_code=400, detail=(
            f"The console can't save in “{p}”. Choose a folder inside the console data folder."))
    from backend.core.console import data_checks

    problem = data_checks.check_data_folder(Path(real))
    if problem:
        raise HTTPException(status_code=400, detail=f"Console data folder: {problem}")
    return p


def _same_path(a: Optional[str], b: Optional[str]) -> bool:
    return bool(a) and bool(b) and os.path.normpath(str(a)) == os.path.normpath(str(b))


def _check_species_folder(species: str, path: str) -> None:
    """A folder chosen as a species' source data must hold that species' data."""
    from backend.core.console import data_checks

    if species not in et.species_list():
        raise HTTPException(status_code=400, detail=f"Unknown species “{species}”.")
    problem = data_checks.check_species_folder(species, Path(path))["problem"]
    if problem:
        raise HTTPException(status_code=400, detail=f"Your {species} source data: {problem}")


def _check_dbsnp_root(path: str) -> None:
    from backend.core.console import data_checks

    problem = data_checks.check_dbsnp_root(Path(path))[1]
    if problem:
        raise HTTPException(status_code=400, detail=f"Your dbSNP cache: {problem}")


@router.put("/settings")
def put_settings(patch: SettingsPatch):
    """Merge a partial patch into the store, persist, apply, and reconnect clients."""
    store_patch: Dict[str, dict] = {}

    glob: Dict[str, object] = {}
    if patch.neo4j is not None:
        n = patch.neo4j
        if n.uri is not None or "uri" in n.__fields_set__:
            glob["neo4j_uri"] = n.uri
        if "user" in n.__fields_set__:
            glob["neo4j_user"] = n.user
        if "database" in n.__fields_set__:
            glob["neo4j_database"] = n.database
        # Ignore an empty password (form not re-entered); null explicitly clears it.
        if "password" in n.__fields_set__ and n.password != "":
            glob["neo4j_password"] = n.password
    if patch.mork is not None and "url" in patch.mork.__fields_set__:
        glob["mork_url"] = patch.mork.url
    if patch.build is not None:
        b = patch.build
        for field in ("data_root", "max_concurrent_builds", "max_output_builds",
                      "max_build_history"):
            if field in b.__fields_set__:
                glob[field] = getattr(b, field)
        # Checked when it changes (an unchanged value was accepted before).
        if glob.get("data_root") and not _same_path(glob["data_root"], settings.DATA_ROOT):
            glob["data_root"] = _check_data_root(glob["data_root"])
    if glob:
        store_patch["global"] = glob

    if patch.species is not None:
        sp: Dict[str, object] = {}
        for name, ep in patch.species.items():
            key = name.upper()
            if ep is None:
                sp[key] = None  # remove this species' overrides
            else:
                entry = {}
                if "neo4j_uri" in ep.__fields_set__:
                    entry["neo4j_uri"] = ep.neo4j_uri
                if "mork_url" in ep.__fields_set__:
                    entry["mork_url"] = ep.mork_url
                sp[key] = entry
        if sp:
            store_patch["species"] = sp

    if patch.data is not None:
        def _clean(v):
            if not (isinstance(v, str) and v.strip()):
                return None  # blank clears
            return _check_folder(v)

        # A folder is checked when it changes: it must be what the code reads.
        saved = runtime_config.data_locations()
        data_patch: Dict[str, object] = {}
        if patch.data.input_dirs is not None:
            dirs = {name: _clean(path) for name, path in patch.data.input_dirs.items()}
            for name, path in dirs.items():
                if path and not _same_path(path, saved["input_dirs"].get(name)):
                    _check_species_folder(name, path)
            data_patch["input_dirs"] = dirs
        if "dbsnp_root" in patch.data.__fields_set__:
            root = _clean(patch.data.dbsnp_root)
            if root and not _same_path(root, saved["dbsnp_root"]):
                _check_dbsnp_root(root)
            data_patch["dbsnp_root"] = root
        if data_patch:
            store_patch["data"] = data_patch

    if not store_patch:
        raise HTTPException(status_code=400, detail="No settings provided to update.")

    runtime_config.save(store_patch)
    return _settings_response()


@router.get("/settings/data-locations")
def get_data_locations():
    """Where each species' source data and the dbSNP cache come from.

    ``readonly`` is a folder you chose (data you already have): read first, never
    written. ``path`` is where downloads go; its origin is "console" (the console's own
    folder) or "config" (the adapters config's input_dir, used when it exists here).
    """
    from pathlib import Path

    from backend.core.console import data_checks, job_runner

    locs = runtime_config.data_locations()
    species = []
    for name in et.species_list():
        loc = job_runner.resolve_species_input(name)
        ro = loc["readonly"]
        species.append({
            # Does your folder hold this species' data? {found, total, problem}
            "readonly_check": data_checks.check_species_folder(name, Path(ro)) if ro else None,
            "species": name,
            "path": loc["path"],
            "origin": loc["origin"],
            "exists": Path(loc["path"]).is_dir(),
            "readonly": loc["readonly"],
            "readonly_exists": bool(loc["readonly"]) and Path(loc["readonly"]).is_dir(),
            "override": locs["input_dirs"].get(name),
            "default": str(job_runner.console_input_dir(name)),
            "config_dir": job_runner.config_input_dir(name),
        })
    root = job_runner.dbsnp_root()
    readonly = job_runner.dbsnp_readonly_root()
    return {
        # In Docker the console only sees folders mounted into its container.
        "in_docker": Path("/.dockerenv").exists(),
        "species": species,
        "dbsnp": {
            "path": str(root),  # where Prepare writes
            "default": str(root),
            "override": locs["dbsnp_root"],
            "readonly_root": str(readonly) if readonly else None,
            "variants": job_runner.dbsnp_variants_at(root),
            "readonly_variants": job_runner.dbsnp_variants_at(readonly) if readonly else None,
            "readonly_problem": data_checks.check_dbsnp_root(readonly)[1] if readonly else None,
        },
    }


class FolderEntry(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    dir: bool = False
    size: Optional[int] = Field(default=None, ge=0)


class ChosenFolder(BaseModel):
    """A folder chosen in Finder, as the browser describes it: its name and (a sample
    of) what's in it. ``count`` is how many entries it has, when the browser knows."""
    name: str = Field(min_length=1, max_length=255, pattern=r"^[^/\x00]+$")
    entries: list[FolderEntry] = Field(default_factory=list, max_length=500)
    count: Optional[int] = Field(default=None, ge=0)
    kind: Optional[Literal["dbsnp", "writable", "source"]] = None
    species: Optional[str] = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,32}$")


# A search walks folders for up to a few seconds: one at a time.
_locate_lock = threading.Lock()


@router.post("/settings/folders/locate")
def locate_folder(req: ChosenFolder):
    """Find where a folder chosen in Finder is (browsers don't say), among the
    folders the console can open."""
    from backend.core.console import folders

    if not _locate_lock.acquire(timeout=15):
        raise HTTPException(status_code=429, detail="Still finding the last folder. Try again.")
    try:
        entries = [e.model_dump() for e in req.entries]
        if req.kind == "source" and req.species not in et.species_list():
            raise HTTPException(status_code=400, detail="Unknown species.")
        return folders.locate(req.name, entries, req.count, req.kind, req.species)
    except folders.FolderError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        _locate_lock.release()


@router.post("/settings/test")
def test_connection(req: TestRequest):
    """Probe a Neo4j or MORK endpoint using the given values (or effective ones).

    Does not save. Returns {reachable, error?} so the UI can validate before saving.
    """
    if req.target == "neo4j":
        eff = runtime_config.effective(masked=False)["neo4j"]
        from backend.core.neo4j_client import Neo4jClient
        client = None
        try:
            client = Neo4jClient(
                uri=et.connect_address(req.uri or eff["uri"]),
                user=req.user or eff["user"],
                password=req.password or eff.get("password"),
                database=req.database or eff["database"],
            )
            ok = client.verify_connection()
            return {"reachable": ok} if ok else {
                "reachable": False,
                "error": "Couldn't sign in to Neo4j. Check the address, username, password "
                         "and database.",
            }
        except Exception as exc:  # noqa: BLE001 - surface the reason to the UI
            return {"reachable": False, "error": str(exc)}
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass

    if req.target == "mork":
        url = req.url or runtime_config.effective(masked=True)["mork"]["url"]
        if not url:
            return {"reachable": False, "error": "No MORK URL set."}
        try:
            from backend.core.mork_client import MORKClient
            client = MORKClient(mork_url=et.connect_address(url))
            client.get_atom_count()
            return {"reachable": True}
        except ImportError as exc:
            return {"reachable": False, "error": str(exc)}
        except Exception:  # noqa: BLE001
            return {"reachable": False, "error": f"Can't reach MORK at {url}."}

    raise HTTPException(status_code=400, detail=f"Unknown target '{req.target}' (expected neo4j or mork).")
