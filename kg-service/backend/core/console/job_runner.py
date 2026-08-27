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
from backend.core.console.job_models import BuildJob, BuildRequest, JobStatus
from backend.core.console.job_registry import registry

logger = logging.getLogger(__name__)

_semaphore = threading.Semaphore(max(1, settings.MAX_CONCURRENT_BUILDS))
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
    if req.input_dir:
        argv += ["--input-dir", _abs(req.input_dir)]
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
    # Explicit resume/restart so the CLI never blocks on its interactive prompt.
    argv.append("--resume" if resume else "--restart")
    return argv


def check_only_argv(adapters_config_abs: str,
                    include_adapters: Optional[list[str]] = None,
                    input_dir: Optional[str] = None) -> list[str]:
    """argv for `--check-only` path validation (runs no adapters)."""
    argv = [settings.UV_BIN, "run", "python", "create_knowledge_graph.py",
            "--adapters-config", adapters_config_abs]
    for adapter in (include_adapters or []):
        argv += ["--include-adapters", adapter]
    if input_dir:
        argv += ["--input-dir", _abs(input_dir)]
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


def _run_job(job_id: str, argv: list[str], log_path: str) -> None:
    """Worker thread: acquire a slot, run the build, capture logs, persist final status."""
    _semaphore.acquire()
    try:
        job = registry.get(job_id)
        if job is None or job.status == JobStatus.CANCELLED:
            return  # cancelled while queued
        with open(log_path, "w") as log_fh:
            log_fh.write(f"$ {' '.join(argv)}\n\n")
            log_fh.flush()
            try:
                proc = subprocess.Popen(
                    argv,
                    cwd=str(settings.repo_root_path),
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env=_subprocess_env(),
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
                        error=None if rc == 0 else f"Build exited with code {rc}.")
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
    skip = {"archives", "configs"}
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


def _mork_host_port() -> tuple[str, str]:
    from urllib.parse import urlparse
    u = urlparse(settings.MORK_URL)
    return (u.hostname or "localhost", str(u.port or 8432))


def build_load_argv(target: str, output_dir: str) -> list[str]:
    """argv to load a build's output into Neo4j or MORK via the existing loaders."""
    # ARCHIVE_BASE passed verbatim: the loaders append the target subdir (neo4j/mork).
    archive_dir = settings.ARCHIVE_BASE
    if target == "neo4j":
        return [settings.UV_BIN, "run", "python", "kg-service/neo4j_loader.py",
                "--output-dir", output_dir,
                # --import-dir makes LOAD CSV use absolute file:/// URLs, so any dir works.
                "--import-dir", output_dir,
                "--archive-dir", archive_dir,
                "--uri", settings.NEO4J_URI,
                "--username", settings.NEO4J_USER,
                "--password", settings.NEO4J_PASSWORD]
    if target == "mork":
        host, port = _mork_host_port()
        return [settings.UV_BIN, "run", "python", "kg-service/mork_loader.py",
                "--data-dir", output_dir,
                "--archive-dir", archive_dir,
                "--host", host, "--port", port]
    raise ValueError(f"unknown load target: {target!r}")


def _start_load(target: str, output_dir: str, params: dict) -> BuildJob:
    """Create + start a tracked load job for the given target + output dir."""
    job_id = uuid.uuid4().hex
    job_dir = registry.job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(job_dir / "load.log")
    argv = build_load_argv(target, output_dir)
    job = BuildJob(
        id=job_id,
        status=JobStatus.QUEUED,
        kind=f"load-{target}",
        params={"target": target, "output_dir": output_dir, **params},
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
