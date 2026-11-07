"""Launch and track knowledge-graph builds as subprocesses."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
import uuid

import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from backend.core.config import settings
from backend.core.console import config_introspect as ci
from backend.core.console.job_models import BuildJob, BuildRequest, DownloadRequest, JobStatus
from backend.core.console.job_registry import registry

logger = logging.getLogger(__name__)



class _BuildSlots:
    """At most MAX_CONCURRENT_BUILDS builds run at once. The limit is read on every
    acquire (not fixed at startup), so changing it in Settings applies right away."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._running = 0

    def acquire(self) -> None:
        with self._cond:
            while self._running >= max(1, int(settings.MAX_CONCURRENT_BUILDS or 1)):
                self._cond.wait(timeout=5)  # also wakes to notice a raised limit
            self._running += 1

    def release(self) -> None:
        with self._cond:
            self._running = max(0, self._running - 1)
            self._cond.notify_all()


_semaphore = _BuildSlots()
# job_id -> Popen, for cancellation. Guarded by _procs_lock.
_procs: dict[str, subprocess.Popen] = {}
_procs_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _abs(path_str: str) -> str:
    """Resolve a repo-relative path against REPO_ROOT; leave absolute paths intact."""
    p = Path(path_str)
    return str(p if p.is_absolute() else (settings.repo_root_path / p))


def _subprocess_env() -> dict:
    """Env for the build: extend PATH with ~/.local/bin so `uv` is found (Makefile does this)."""
    env = os.environ.copy()
    local_bin = str(Path.home() / ".local" / "bin")
    if local_bin not in env.get("PATH", ""):
        env["PATH"] = local_bin + os.pathsep + env.get("PATH", "")
    return env


def build_argv(req: BuildRequest, output_dir: str, resume: bool = False) -> list[str]:
    """Construct the exact argv to launch a build. Single source of truth."""
    argv = [settings.UV_BIN, "run", "python", "create_knowledge_graph.py"]
    if req.species:
        argv += ["--species", req.species, "--dataset", req.dataset]
    if req.adapters_config:
        argv += ["--adapters-config", _abs(req.adapters_config)]
    if req.schema_config:
        argv += ["--schema-config", _abs(req.schema_config)]
    argv += ["--output-dir", output_dir]
    argv += ["--writer-type", req.writer_type]
    for adapter in (req.include_adapters or []):
        argv += ["--include-adapters", adapter]
    argv += _input_dir_args(effective_input_dirs(req))
    if req.dbsnp_cache_root:
        argv += ["--dbsnp-cache-root", req.dbsnp_cache_root]
    if req.dbsnp_variant:
        argv += ["--dbsnp-variant", req.dbsnp_variant]
    # Negatable booleans: only emit when they differ from the CLI default.
    if not req.write_properties:
        argv.append("--no-write-properties")
    if not req.add_provenance:
        argv.append("--no-add-provenance")
    if not req.include_taxon_id:
        argv.append("--no-taxon-id")
    if req.include_curie:
        argv.append("--include-curie")
    if req.skip_preflight:
        argv.append("--skip-preflight")
    if not req.generate_data_source_schemas:
        argv.append("--no-generate-data-source-schemas")
    # Adapter parallelism, chosen per build. The CLI defaults to min(8, CPUs), which
    # OOMs a memory-limited container (BrokenProcessPool); the request defaults to 1.
    argv += ["--max-workers", str(req.max_workers)]
    # Optional per-adapter row cap for small, bounded sample graphs (0 = no limit).
    if getattr(req, "limit", 0) and req.limit > 0:
        argv += ["--limit", str(req.limit)]
    # Explicit resume/restart so the CLI never blocks on its interactive prompt.
    argv.append("--resume" if resume else "--restart")
    return argv


def check_only_argv(adapters_config_abs: str,
                    include_adapters: Optional[list[str]] = None,
                    input_dir=None) -> list[str]:
    """argv for `--check-only` path validation (runs no adapters).

    ``input_dir`` is one folder or an ordered list (read-only data first)."""
    argv = [settings.UV_BIN, "run", "python", "create_knowledge_graph.py",
            "--adapters-config", adapters_config_abs]
    for adapter in (include_adapters or []):
        argv += ["--include-adapters", adapter]
    dirs = input_dir if isinstance(input_dir, list) else ([input_dir] if input_dir else [])
    argv += _input_dir_args([_abs(d) for d in dirs])
    argv.append("--check-only")
    return argv


def resolve_output_dir(req: BuildRequest, job_dir: Path) -> str:
    """Where the build writes: explicit output_dir -> dated DATA_ROOT dir -> <job_dir>/output."""
    if req.output_dir:
        return _abs(req.output_dir)
    if settings.DATA_ROOT:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        name = f"{req.species}-{req.dataset}-{ts}" if req.species else f"build-{ts}"
        return _abs(str(Path(settings.DATA_ROOT) / req.writer_type / name))
    return str(job_dir / "output")


def read_checkpoint(output_dir: str) -> Optional[dict]:
    """Summarise ``<output_dir>/kg_checkpoint.json`` if the CLI left one behind."""
    p = Path(output_dir) / "kg_checkpoint.json"
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    completed = data.get("completed_adapters", []) or []
    return {
        "completed_adapters": completed,
        "completed_count": len(completed),
        "failed_adapter": data.get("failed_adapter"),
        "updated_at": data.get("updated_at"),
    }


def _reap_orphan_temp_schemas() -> None:
    """Delete stray config/tmp*.yaml left by builds killed before their own cleanup."""
    # Skip while a build is RUNNING: temp files aren't mapped to a job and may be in use.
    if any(j.status == JobStatus.RUNNING for j in registry.list()):
        return
    for f in (settings.repo_root_path / "config").glob("tmp*.yaml"):
        try:
            f.unlink()
        except OSError:
            pass


def _count_adapters(req: BuildRequest) -> Optional[int]:
    """Best-effort total adapter count for this build (for progress %)."""
    if req.include_adapters:
        return len(req.include_adapters)
    try:
        if req.species:
            return ci.list_adapters(req.species, req.dataset)["count"]
        if req.adapters_config:
            loader = ci._load_yaml_with_includes()
            data = loader(ci._resolve(req.adapters_config).as_posix()) or {}
            data.pop("input_dir", None)
            return len([k for k, v in data.items() if isinstance(v, dict)])
    except Exception:  # noqa: BLE001 - progress is best-effort, never block a launch
        return None
    return None


def launch(req: BuildRequest, resume: bool = False) -> BuildJob:
    """Create a job, persist it QUEUED, and start a worker thread that runs the build."""
    job_id = uuid.uuid4().hex
    job_dir = registry.job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    output_dir = resolve_output_dir(req, job_dir)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    log_path = str(job_dir / "build.log")
    argv = build_argv(req, output_dir, resume=resume)

    (job_dir / "params.json").write_text(req.model_dump_json(indent=2))

    job = BuildJob(
        id=job_id,
        status=JobStatus.QUEUED,
        params=req.model_dump(),
        cmd=argv,
        cwd=str(settings.repo_root_path),
        output_dir=output_dir,
        log_path=log_path,
        total_adapters=_count_adapters(req),
        created_at=_now_iso(),
    )
    registry.add(job)
    registry.prune()

    worker = threading.Thread(target=_run_job, args=(job_id, argv, log_path), daemon=True)
    worker.start()
    return job


# Secrets a job's command may carry, and the environment variable each one goes in
# instead: a command line is visible to every process (ps) and is written to the job
# log, an environment variable is neither.
_SECRET_ENV = {"--password": "NEO4J_PASSWORD"}


def _secrets_to_env(argv: list[str]) -> tuple[list[str], dict]:
    """Move secret flags (e.g. the Neo4j password) out of argv into env vars."""
    out, env, i = [], {}, 0
    while i < len(argv):
        a = argv[i]
        if a in _SECRET_ENV and i + 1 < len(argv):
            env[_SECRET_ENV[a]] = argv[i + 1]
            i += 2
            continue
        flag = a.split("=", 1)[0]
        if "=" in a and flag in _SECRET_ENV:
            env[_SECRET_ENV[flag]] = a.split("=", 1)[1]
        else:
            out.append(a)
        i += 1
    return out, env


def _run_job(job_id: str, argv: list[str], log_path: str) -> None:
    """Worker thread: acquire a slot, run the build, capture logs, persist final status."""
    argv, secret_env = _secrets_to_env(argv)
    _semaphore.acquire()
    try:
        job = registry.get(job_id)
        if job is None or job.status == JobStatus.CANCELLED:
            return  # cancelled while queued
        with open(log_path, "w") as log_fh:
            log_fh.write(f"$ {' '.join(_redact_argv(argv))}\n\n")
            log_fh.flush()
            try:
                proc = subprocess.Popen(
                    argv,
                    cwd=str(settings.repo_root_path),
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env={**_subprocess_env(), **secret_env},
                    start_new_session=True,  # own process group, so cancel can killpg
                )
            except (OSError, ValueError) as exc:
                registry.update(job_id, status=JobStatus.FAILED, error=str(exc),
                                finished_at=_now_iso())
                logger.error("Failed to launch build %s: %s", job_id, exc)
                return

            with _procs_lock:
                _procs[job_id] = proc
            registry.update(job_id, status=JobStatus.RUNNING, pid=proc.pid,
                            started_at=_now_iso())
            rc = proc.wait()

        with _procs_lock:
            _procs.pop(job_id, None)

        current = registry.get(job_id)
        if current and current.status == JobStatus.CANCELLED:
            return  # a cancel already set the terminal state
        status = JobStatus.SUCCEEDED if rc == 0 else JobStatus.FAILED
        registry.update(job_id, status=status, return_code=rc, finished_at=_now_iso(),
                        error=None if rc == 0 else "Failed. The log below shows why.")
        logger.info("Build %s finished: %s (rc=%s)", job_id, status.value, rc)
        if status == JobStatus.SUCCEEDED and (job.kind or "build") == "build":
            try:
                snapshot_build_config(job)
            except Exception as exc:  # noqa: BLE001 - provenance is best-effort
                logger.warning("Config snapshot skipped: %s", exc)
            try:
                n = prune_output_dirs()
                if n:
                    logger.info("Pruned %d old build output dir(s)", n)
            except OSError as exc:
                logger.warning("Output prune skipped: %s", exc)
    finally:
        _semaphore.release()
        _reap_orphan_temp_schemas()


def snapshot_build_config(job: BuildJob) -> Optional[dict]:
    """Record the exact resolved config that produced a build, into <output>/build_config/.

    Writes the include-merged adapters + schema YAML (self-contained) plus a manifest
    with per-file and combined sha256 hashes, so a KG's output carries which config
    version made it. Best-effort: returns None if it can't be produced.
    """
    from backend.core.console import config_introspect as ci

    p = job.params or {}
    species, dataset = p.get("species"), p.get("dataset")
    out = Path(job.output_dir)
    if not species or not dataset or not out.is_dir():
        return None

    load = ci._load_yaml_with_includes()
    try:
        adapters_path = ci.resolve_adapters_config_path(species, dataset)
        schema_path = ci.resolve_schema_config_path(species, dataset)
    except ci.ConfigError:
        return None
    adapters = load(str(adapters_path)) or {}
    # Effective schema = primer overlaid by the species schema (mirrors the build merge).
    primer_path = settings.repo_root_path / "config" / "primer_schema_config.yaml"
    primer = (load(str(primer_path)) or {}) if primer_path.exists() else {}
    schema = {**primer, **(load(str(schema_path)) or {})}

    adapters_yaml = yaml.safe_dump(adapters, sort_keys=False)
    schema_yaml = yaml.safe_dump(schema, sort_keys=False)

    dest = out / "build_config"
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "adapters_config.yaml").write_text(adapters_yaml)
    (dest / "schema_config.yaml").write_text(schema_yaml)

    def _sha(text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()

    manifest = {
        "species": species,
        "dataset": dataset,
        "writer_type": p.get("writer_type"),
        "source": {"adapters_config": str(adapters_path), "schema_config": str(schema_path)},
        "sha256": {"adapters_config": _sha(adapters_yaml), "schema_config": _sha(schema_yaml)},
        "config_hash": _sha(adapters_yaml + schema_yaml),
        "recorded_at": _now_iso(),
    }
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


# Matches the ...-YYYYMMDD-HHMMSS suffix of an auto-named build dir.
_BUILD_TS_SUFFIX = re.compile(r"-\d{8}-\d{6}$")


def prune_output_dirs(keep: Optional[int] = None) -> int:
    """Keep only the newest ``keep`` build output folders per (writer folder, species)
    under DATA_ROOT, deleting older ones. Leaves ``archives/`` and ``configs/`` alone,
    and never removes a folder a queued/running build is writing to."""
    keep = keep if keep is not None else settings.MAX_OUTPUT_BUILDS
    root = Path(settings.DATA_ROOT) if settings.DATA_ROOT else None
    if root is None or keep <= 0 or not root.is_dir():
        return 0

    active = {
        str(Path(j.output_dir).resolve())
        for j in registry.list()
        if j.output_dir and j.status in {JobStatus.QUEUED, JobStatus.RUNNING}
    }

    # Scan DATA_ROOT itself (legacy flat builds) plus each writer subdir (metta/, neo4j/…).
    skip = {"archives", "configs", "dbsnp"}
    parents = [root] + [
        c for c in root.iterdir()
        if c.is_dir() and c.name not in skip and not _BUILD_TS_SUFFIX.search(c.name)
    ]

    groups: dict[tuple, list[Path]] = {}
    for parent in parents:
        for d in parent.iterdir():
            if d.is_dir() and _BUILD_TS_SUFFIX.search(d.name):
                species = d.name.split("-", 1)[0]
                groups.setdefault((str(parent), species), []).append(d)

    removed = 0
    for dirs in groups.values():
        dirs.sort(key=lambda p: p.name, reverse=True)  # newest first (name ends in timestamp)
        for d in dirs[keep:]:
            if str(d.resolve()) in active:
                continue
            shutil.rmtree(d, ignore_errors=True)
            removed += 1
    return removed


_SECRET_FLAGS = {"--password", "--pass", "--pwd", "--token", "--secret"}


def _redact_argv(argv: list[str]) -> list[str]:
    """Copy of argv with secret values masked, for storing/displaying the command
    without leaking the Neo4j password to the registry JSON or the UI."""
    out = list(argv)
    i = 0
    while i < len(out):
        a = out[i]
        if a in _SECRET_FLAGS and i + 1 < len(out):
            out[i + 1] = "***"
            i += 2
            continue
        if "=" in a and a.split("=", 1)[0] in _SECRET_FLAGS:
            out[i] = a.split("=", 1)[0] + "=***"
        i += 1
    return out


def archive_base_for(species: Optional[str] = None) -> str:
    """Where a species' loads keep archives and version metadata. Each species has
    its own databases, so each gets its own folder (ARCHIVE_BASE/<species>);
    otherwise loading one species would overwrite the hashes and version the
    others' Explore cards show. Without a species (one shared instance) it's
    ARCHIVE_BASE itself. The loaders append neo4j/ or mork/."""
    base = settings.ARCHIVE_BASE
    return str(Path(base) / species) if species else base


def build_load_argv(target: str, output_dir: str, species: str = None) -> list[str]:
    """argv to load a build's output into Neo4j or MORK via the existing loaders.

    Routes to the species' own instance (NEO4J_URI_<SP> / MORK_URL_<SP>) so a
    build for a species loads into the database attached to that species. In
    per-species mode a species with no instance raises rather than loading into
    another species' database; in single-instance mode the global endpoint is used.
    """
    from urllib.parse import urlparse
    from backend.core.console import explore_targets as et
    # The loaders append the target subdir (neo4j/mork) to this.
    archive_dir = archive_base_for(species)
    if target == "neo4j":
        from backend.core.console import connections

        if species:
            # The species' own database and login (Settings > Species databases).
            login = connections.neo4j_login(species)
        else:
            login = {"uri": settings.NEO4J_URI, "user": settings.NEO4J_USER,
                     "password": settings.NEO4J_PASSWORD, "database": settings.NEO4J_DATABASE}
        if not login:
            raise ValueError(
                f"{species} has no Neo4j yet. Set one up or connect one in "
                "Settings > Species databases, then load again."
            )
        return [settings.UV_BIN, "run", "python", "kg-service/neo4j_loader.py",
                "--output-dir", output_dir,
                # --import-dir makes LOAD CSV use absolute file:/// URLs, so any dir works.
                "--import-dir", output_dir,
                "--archive-dir", archive_dir,
                "--uri", et.connect_address(login["uri"]),
                "--username", login["user"],
                "--password", login["password"],
                "--database", login["database"]]
    if target == "mork":
        url = et.mork_url_for(species) if species else settings.MORK_URL
        if not url:
            raise ValueError(
                f"{species} has no MORK yet. Set one up or connect one in "
                "Settings > Species databases, then load again."
            )
        u = urlparse(et.connect_address(url))
        host, port = (u.hostname or "localhost", str(u.port or 8432))
        return [settings.UV_BIN, "run", "python", "kg-service/mork_loader.py",
                "--data-dir", output_dir,
                "--archive-dir", archive_dir,
                "--host", host, "--port", port]
    raise ValueError(f"unknown load target: {target!r}")


def _start_load(target: str, output_dir: str, params: dict) -> BuildJob:
    """Create + start a tracked load job for the given target + output dir."""
    job_id = uuid.uuid4().hex
    # Build (and so validate) the argv before any filesystem/registry work, so an
    # unconfigured target raises ValueError without leaving a stray job behind.
    argv = build_load_argv(target, output_dir, params.get("species"))
    job_dir = registry.job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(job_dir / "load.log")
    job = BuildJob(
        id=job_id,
        status=JobStatus.QUEUED,
        kind=f"load-{target}",
        params={"target": target, "output_dir": output_dir, **params},
        # Store a redacted copy so the password never lands in the registry/UI;
        # the real argv (below) is what actually runs.
        cmd=_redact_argv(argv),
        cwd=str(settings.repo_root_path),
        output_dir=output_dir,
        log_path=log_path,
        created_at=_now_iso(),
    )
    registry.add(job)
    registry.prune()
    worker = threading.Thread(target=_run_job, args=(job_id, argv, log_path), daemon=True)
    worker.start()
    return job


def launch_load(build_job_id: str, target: str) -> Optional[BuildJob]:
    """Launch a load job that pushes a build's output into Neo4j/MORK."""
    src = registry.get(build_job_id)
    if src is None:
        return None
    return _start_load(target, src.output_dir, {
        "source_build": build_job_id,
        "species": (src.params or {}).get("species"),
        "dataset": (src.params or {}).get("dataset"),
    })


def retry_load(job_id: str) -> Optional[BuildJob]:
    """Re-run a failed/cancelled load job with the same target + output dir."""
    prior = registry.get(job_id)
    if prior is None or not (prior.kind or "").startswith("load-"):
        return None
    p = prior.params or {}
    target = p.get("target") or (prior.kind or "").removeprefix("load-")
    output_dir = p.get("output_dir") or prior.output_dir
    if target not in ("neo4j", "mork") or not output_dir:
        return None
    return _start_load(target, output_dir, {
        "source_build": p.get("source_build"),
        "species": p.get("species"),
        "dataset": p.get("dataset"),
        "retry_of": job_id,
    })


def loads_for(build_id: str) -> dict:
    """Latest load job per target (neo4j/mork) that loaded this build's output."""
    out: dict = {}
    for j in registry.list():
        if not (j.kind or "").startswith("load-"):
            continue
        p = j.params or {}
        if p.get("source_build") != build_id:
            continue
        target = p.get("target") or (j.kind or "").removeprefix("load-")
        cur = out.get(target)
        if cur is None or (j.created_at or "") > (cur["created_at"] or ""):
            out[target] = {
                "job_id": j.id,
                "status": j.status.value,
                "created_at": j.created_at,
            }
    return out


# ── Source-data download ──────────────────────────────────────────────────────
# A new local user can only build "sample" out of the box; a "full" build needs the
# source data, which the CLI downloader (biocypher_dataset_downloader) fetches. These
# wrap it as a tracked job so the whole acquire -> build -> load flow stays in the UI.


def console_input_dir(species: str) -> Path:
    """The console's own download folder for a species: DATA_ROOT/input/<species>,
    or <repo>/input/<species> when DATA_ROOT is unset (dev without a data root)."""
    root = settings.DATA_ROOT or str(settings.repo_root_path / "input")
    base = Path(root)
    # Keep the input tree beside build output (archives/, configs/) under DATA_ROOT.
    if settings.DATA_ROOT:
        base = base / "input"
    return (base / species).resolve()


def _user_path(p: str) -> Path:
    """A path typed by the user or found in a config: ~ expanded, relative to the repo."""
    path = Path(p.strip()).expanduser()
    return (path if path.is_absolute() else settings.repo_root_path / path).resolve()


def config_input_dir(species: str) -> Optional[str]:
    """The input_dir the species' full adapters config points at (e.g. the server's
    /mnt/hdd_1/biocypher-kg/input/<species>), or None if it has none."""
    try:
        _, cfg_dir = ci.load_adapters_dict(species, "full")
    except Exception:  # noqa: BLE001 - species without a full dataset/config
        return None
    return str(_user_path(cfg_dir)) if cfg_dir else None


def resolve_species_input(species: str) -> dict:
    """Where a species' source data is read from and downloaded to.

    ``readonly``: a folder chosen in Settings > Data locations (data you already have).
    The console reads from it first and never writes to it.

    ``path`` / ``origin``: the writable folder downloads go into, also read:
      "console": the console's own folder, when it already holds data;
      "config":  else the adapters config's input_dir, when that path exists here, so a
                 host that already keeps the data there (the team server) needs no setup;
      "console": else the console's folder, which downloads create.
    Presence checks, builds and validation read both (read-only first).
    """
    from backend.core.console import runtime_config  # local: keep import graph simple

    override = runtime_config.data_locations()["input_dirs"].get(species)
    readonly = str(_user_path(override)) if override else None
    console_dir = console_input_dir(species)
    if console_dir.is_dir() and any(console_dir.iterdir()):
        path, origin = str(console_dir), "console"
    else:
        cfg_dir = config_input_dir(species)
        if cfg_dir and Path(cfg_dir).is_dir():
            path, origin = cfg_dir, "config"
        else:
            path, origin = str(console_dir), "console"
    if readonly == path:
        readonly = None  # pointing "your data" at the download folder itself
    return {"path": path, "origin": origin, "readonly": readonly}


def species_input_dir(species: str) -> Optional[str]:
    """The writable folder a species' downloads go into (never the read-only one)."""
    if not species:
        return None
    return resolve_species_input(species)["path"]


def species_read_dirs(species: str) -> list:
    """Folders a species' source files are read from, in priority order."""
    loc = resolve_species_input(species)
    return ([loc["readonly"]] if loc["readonly"] else []) + [loc["path"]]


def effective_input_dirs(req: BuildRequest) -> list:
    """The input folders a build reads, in order (first is --input-dir, the rest are
    --extra-input-dir); validation's pre-flight checks the same ones.

    An explicit request dir wins. Full species builds read the read-only folder (if
    set) and then the download folder, whichever exist; sample builds return [] so they
    keep the bundled sample data.
    """
    if req.input_dir:
        return [_abs(req.input_dir)]
    if req.species and req.species.lower() != "all" and req.dataset != "sample":
        return [d for d in species_read_dirs(req.species) if Path(d).is_dir()]
    return []


def _input_dir_args(dirs: list) -> list:
    """argv for an ordered list of input folders."""
    if not dirs:
        return []
    args = ["--input-dir", dirs[0]]
    for d in dirs[1:]:
        args += ["--extra-input-dir", d]
    return args


def _data_source_config_rel(species: str) -> str:
    """Repo-relative data source config path (subprocess cwd is the repo root)."""
    return f"config/{species}/{species}_data_source_config.yaml"


def build_download_argv(req: DownloadRequest, output_dir: str) -> list[str]:
    """argv to fetch a species' source data via the existing downloader module."""
    argv = [settings.UV_BIN, "run", "python", "-m",
            "biocypher_dataset_downloader.download_data",
            "--output-dir", output_dir,
            "--config-file", _data_source_config_rel(req.species)]
    keys = ([req.source] if req.source else []) + list(req.sources or [])
    for key in dict.fromkeys(keys):  # de-duplicate, keep order
        argv += ["--source", key]
    if req.no_checksum:
        argv.append("--no-checksum")
    # Don't rebuild the bundled connected sample from a console download.
    argv.append("--skip-sample-generation")
    return argv


def launch_download(req: DownloadRequest) -> Optional[BuildJob]:
    """Start a tracked download job that fetches a species' source data locally."""
    cfg = settings.repo_root_path / _data_source_config_rel(req.species)
    if not cfg.exists():
        raise ValueError(f"No data source config for species '{req.species}' ({cfg}).")
    output_dir = species_input_dir(req.species)
    target = Path(output_dir).resolve()
    for ro in _readonly_dirs():
        if target == ro or ro in target.parents:
            raise ValueError(
                f"Downloads would go to {output_dir}, inside your read-only data folder "
                f"({ro}). Point Settings > Data locations or the data root elsewhere."
            )
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    job_id = uuid.uuid4().hex
    argv = build_download_argv(req, output_dir)
    job_dir = registry.job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(job_dir / "download.log")
    job = BuildJob(
        id=job_id,
        status=JobStatus.QUEUED,
        kind="download",
        params={"species": req.species, "source": req.source, "sources": req.sources,
                "output_dir": output_dir},
        cmd=argv,
        cwd=str(settings.repo_root_path),
        output_dir=output_dir,
        log_path=log_path,
        created_at=_now_iso(),
    )
    registry.add(job)
    registry.prune()
    worker = threading.Thread(target=_run_job, args=(job_id, argv, log_path), daemon=True)
    worker.start()
    return job


# ── dbSNP cache (prepared by the console, not by hand) ─────────────────────────
# Builds whose adapters take dbsnp_rsid_map / dbsnp_pos_map need a dbSNP SQLite cache
# at <root>/<variant>/dbsnp_mapping.db. Instead of asking the user for a local path,
# the console can build it into the data volume with scripts/update_dbsnp.py and
# hand that root to the build.

DBSNP_VARIANTS = ("common", "full")


def dbsnp_root() -> Path:
    """Where the console prepares (writes) its dbSNP cache: <DATA_ROOT>/dbsnp, else
    <REPO_ROOT>/dbsnp_cache. A cache chosen in Settings is only read, never written."""
    if settings.DATA_ROOT:
        return Path(settings.DATA_ROOT) / "dbsnp"
    return settings.repo_root_path / "dbsnp_cache"


def dbsnp_readonly_root() -> Optional[Path]:
    """A dbSNP cache you already have (Settings > Data locations); read-only."""
    from backend.core.console import runtime_config  # local: keep import graph simple

    override = runtime_config.data_locations()["dbsnp_root"]
    return _user_path(override) if override else None


def _active_dbsnp_job() -> Optional[BuildJob]:
    for job in registry.list():
        if job.kind == "dbsnp" and job.status in (JobStatus.QUEUED, JobStatus.RUNNING):
            return job
    return None


def dbsnp_variants_at(root: Path) -> dict:
    """Per-variant readiness of a dbSNP cache root (<root>/<variant>/dbsnp_mapping.db).
    ``present`` means usable: the file is there and really is a dbSNP cache (see
    data_checks); ``problem`` says why a file that's there can't be used."""
    from backend.core.console import data_checks

    variants: dict = {}
    for variant in DBSNP_VARIANTS:
        db = root / variant / "dbsnp_mapping.db"
        exists = db.is_file()
        problem = data_checks.check_dbsnp_variant(root / variant, variant) if exists else None
        present = exists and problem is None
        info: dict = {"present": present, "bytes": db.stat().st_size if exists else 0,
                      "built_at": None, "problem": problem}
        version_file = root / variant / "dbsnp_version.json"
        if version_file.is_file():
            try:
                info["built_at"] = json.loads(version_file.read_text()).get("timestamp")
            except (OSError, ValueError):
                pass
        variants[variant] = info
    return variants


def dbsnp_status(custom_root: Optional[str] = None) -> dict:
    """Which dbSNP variants are ready and where they are (a read-only cache chosen in
    Settings first, then the console's own), any running prepare job, and optionally
    whether a typed cache root holds a usable cache (so the UI can say so before a
    build fails on it). Each variant carries the ``root`` a build should use."""
    root = dbsnp_root()
    readonly = dbsnp_readonly_root()
    roots = ([readonly] if readonly else []) + [root]
    at = {r: dbsnp_variants_at(r) for r in roots}
    variants: dict = {}
    for variant in DBSNP_VARIANTS:
        where = next((r for r in roots if at[r][variant]["present"]), root)
        variants[variant] = {**at[where][variant], "root": str(where)}
    active = _active_dbsnp_job()
    out = {
        "root": str(root),
        "readonly_root": str(readonly) if readonly else None,
        "variants": variants,
        "job_id": active.id if active else None,
        "job_variant": (active.params or {}).get("variant") if active else None,
    }
    if custom_root and custom_root.strip():
        p = Path(custom_root.strip())
        if not p.is_absolute():
            p = settings.repo_root_path / p
        out["custom"] = {"root": custom_root.strip(), "variants": dbsnp_variants_at(p)}
    return out


def launch_dbsnp_prepare(variant: str) -> BuildJob:
    """Start a tracked job that builds the dbSNP cache for one variant."""
    if variant not in DBSNP_VARIANTS:
        raise ValueError(f"dbSNP variant must be one of {DBSNP_VARIANTS} (got '{variant}').")
    if _active_dbsnp_job():
        raise ValueError("A dbSNP cache job is already running.")
    cache_dir = dbsnp_root() / variant
    cache_dir.mkdir(parents=True, exist_ok=True)
    # --force: (re)build now rather than only when the 90-day refresh is due.
    argv = [settings.UV_BIN, "run", "python", "scripts/update_dbsnp.py",
            "--cache-dir", str(cache_dir), "--force"]
    if variant == "common":
        argv.append("--common-only")

    job_id = uuid.uuid4().hex
    job_dir = registry.job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(job_dir / "dbsnp.log")
    job = BuildJob(
        id=job_id,
        status=JobStatus.QUEUED,
        kind="dbsnp",
        params={"variant": variant, "output_dir": str(cache_dir)},
        cmd=argv,
        cwd=str(settings.repo_root_path),
        output_dir=str(cache_dir),
        log_path=log_path,
        created_at=_now_iso(),
    )
    registry.add(job)
    registry.prune()
    worker = threading.Thread(target=_run_job, args=(job_id, argv, log_path), daemon=True)
    worker.start()
    return job


def _path_bytes(p: Path) -> int:
    return _dir_size(p) if p.is_dir() else (p.stat().st_size if p.is_file() else 0)


def _source_paths(input_dir: Path, source: dict, dirs_by_key: dict) -> list:
    """Where one data source's files are in a species input dir ([] if absent).

    Prefers the exact files the source declares (local_files), because several
    sources don't land under input/<key>/: GENCODE goes to ensembl/, GTEx to gtex/,
    Roadmap under forgedb/. Falls back to the source's (possibly versioned) directory.
    """
    found: list = []
    for rel in source.get("local_paths") or []:
        if any(c in rel for c in "*?["):
            matches = list(input_dir.glob(rel))
        else:
            matches = [input_dir / rel] if (input_dir / rel).exists() else []
        if not matches:
            found = []
            break
        found.extend(matches)
    if found:
        return found
    for d in dirs_by_key.get(source["key"], []):
        if d.is_dir() and any(d.iterdir()):
            return [d]
    return []


def _source_presence(input_dir: Path, source: dict, dirs_by_key: dict) -> tuple:
    """(present, bytes) for one data source in the species input dir."""
    found = _source_paths(input_dir, source, dirs_by_key)
    return (True, sum(_path_bytes(m) for m in found)) if found else (False, 0)


def _dir_size(path: Path) -> int:
    """Total size in bytes of a directory tree (0 if missing); best-effort."""
    if not path.exists():
        return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += (Path(root) / f).stat().st_size
            except OSError:
                pass
    return total


def _dirs_by_key(root: Path, sources: list) -> dict:
    """Candidate directories per source under one root: its own <key>/ plus any
    versioned dir resolving to it (catlasv1/ -> catlas, gtex/ -> gtex_eqtl), the same
    rule as grouping."""
    key_by_lower = {s["key"].lower(): s["key"] for s in sources}
    dirs: dict = {s["key"]: [root / s["key"]] for s in sources}
    if root.is_dir():
        for child in root.iterdir():
            if child.is_dir():
                m = ci._match_source(child.name, set(key_by_lower))
                if m and child not in dirs[key_by_lower[m]]:
                    dirs[key_by_lower[m]].append(child)
    return dirs


def data_status(species: str) -> dict:
    """What source data is present for a species (per-source + totals), looking in
    your read-only folder first and then the download folder. Each source records
    where it was found ("yours" or "console") so the UI can say what is downloadable."""
    loc = resolve_species_input(species)
    input_dir = Path(loc["path"])
    roots = ([(Path(loc["readonly"]), "yours")] if loc["readonly"] else []) + [(input_dir, "console")]
    try:
        sources = ci.list_data_sources(species).get("sources", [])
    except Exception:  # noqa: BLE001 - no config -> no sources
        sources = []
    candidates = {tag: _dirs_by_key(root, sources) for root, tag in roots}

    out = []
    for s in sources:
        present, size, where = False, 0, None
        for root, tag in roots:
            present, size = _source_presence(root, s, candidates[tag])
            if present:
                where = tag
                break
        out.append({"key": s["key"], "name": s["name"], "present": present, "bytes": size,
                    "location": where})
    return {
        "species": species,
        "input_dir": str(input_dir),          # downloads go here
        "input_origin": loc["origin"],        # console | config
        "readonly_dir": loc["readonly"],      # your data, read first, never written
        "exists": any(root.is_dir() for root, _ in roots),
        "manifest_present": (input_dir / "download_manifest.json").exists(),
        "source_count": len(out),
        "present_count": sum(1 for o in out if o["present"]),
        "total_bytes": sum(o["bytes"] for o in out),
        "sources": out,
    }


def _readonly_dirs() -> list:
    """Folders chosen in Settings > Data locations: data you already have, which the
    console only reads (per-species source folders and the dbSNP cache)."""
    from backend.core.console import runtime_config  # local: keep import graph simple

    locs = runtime_config.data_locations()
    paths = list(locs["input_dirs"].values()) + ([locs["dbsnp_root"]] if locs["dbsnp_root"] else [])
    return [_user_path(p) for p in paths]


def _protected_data_dirs() -> list:
    """Source-data folders the console reads but didn't create: folders chosen in
    Settings and each species' adapters-config input_dir."""
    dirs = _readonly_dirs()
    try:
        species = [e["species"] for e in ci.list_species_and_datasets()]
    except Exception:  # noqa: BLE001 - no config: nothing else to protect
        species = []
    for sp in species:
        cfg = config_input_dir(sp)
        if cfg:
            dirs.append(Path(cfg))
    return dirs


def purge_output_dir(output_dir: str) -> bool:
    """Delete a build's output folder (for reclaiming disk). Refuses to touch the
    shared archives/configs/input trees or anything outside DATA_ROOT."""
    if not output_dir:
        return False
    try:
        target = Path(output_dir).resolve()
    except OSError:
        return False
    if not target.is_dir():
        return False
    # The managed dbSNP cache is shared by every build and takes hours to rebuild;
    # never purge it as if it were one job's output.
    dbsnp = dbsnp_root().resolve()
    if target == dbsnp or dbsnp in target.parents:
        return False
    # Nor data the console was pointed at rather than created (a folder chosen in
    # Settings, or an adapters config's input_dir): a download job's "output" is that
    # folder, and purging it would delete the user's own source data.
    for d in _protected_data_dirs():
        if target == d or d in target.parents or target in d.parents:
            return False
    if settings.DATA_ROOT:
        root = Path(settings.DATA_ROOT).resolve()
        # Must live under DATA_ROOT, and never be a protected shared dir or the root.
        if root not in target.parents or target.name in {"archives", "configs", "input"}:
            return False
    shutil.rmtree(target, ignore_errors=True)
    return not target.exists()


def disk_usage() -> dict:
    """Disk usage for the console's data root: builds, input, archives, and free space."""
    info: dict = {"data_root": settings.DATA_ROOT or None}
    root = Path(settings.DATA_ROOT) if settings.DATA_ROOT else None
    if root is None or not root.is_dir():
        info["available"] = False
        return info
    try:
        usage = shutil.disk_usage(str(root))
        info["free_bytes"] = usage.free
        info["total_bytes_fs"] = usage.total
    except OSError:
        pass
    categories: dict[str, int] = {}
    for name in ("archives", "configs", "input"):
        categories[name] = _dir_size(root / name)
    # Everything else under the root counts as build output.
    build_bytes = 0
    for child in root.iterdir():
        if child.is_dir() and child.name not in ("archives", "configs", "input"):
            build_bytes += _dir_size(child)
    categories["builds"] = build_bytes
    info["available"] = True
    info["categories"] = categories
    info["used_bytes"] = sum(categories.values())
    return info


def resume(job_id: str) -> Optional[BuildJob]:
    """Launch a new job that continues a prior failed/cancelled build's checkpoint."""
    prior = registry.get(job_id)
    if prior is None or read_checkpoint(prior.output_dir) is None:
        return None
    req = BuildRequest(**prior.params)
    req.output_dir = prior.output_dir  # same dir → CLI finds the checkpoint
    return launch(req, resume=True)


def cancel(job_id: str, grace_seconds: float = 5.0) -> bool:
    """Cancel a running/queued job. Returns True if a state change happened."""
    job = registry.get(job_id)
    if job is None or job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED,
                                     JobStatus.CANCELLED):
        return False

    with _procs_lock:
        proc = _procs.get(job_id)

    if proc is None:
        # Still queued (no process yet): mark cancelled so the worker skips it.
        registry.update(job_id, status=JobStatus.CANCELLED, finished_at=_now_iso(),
                        error="Cancelled before start.")
        return True

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass

    registry.update(job_id, status=JobStatus.CANCELLED, finished_at=_now_iso(),
                    error="Cancelled by user.")
    with _procs_lock:
        _procs.pop(job_id, None)
    return True
