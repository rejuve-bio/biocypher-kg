"""Launch and track knowledge-graph builds as subprocesses.

Builds always shell out to ``create_knowledge_graph.py`` with ``cwd = REPO_ROOT``
(the CLI hardcodes repo-root-relative paths). We never import the CLI here.

Concurrency is bounded by a semaphore of size ``MAX_CONCURRENT_BUILDS``. A job that
cannot get a slot stays QUEUED until one frees — this queue + semaphore is the seam
that Phase 3 parallelization will widen.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import threading
import uuid
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
    """Construct the exact argv to launch a build. Single source of truth.

    Flag names mirror create_knowledge_graph.py::main exactly.

    Always passes an explicit ``--resume``/``--restart`` so the CLI never falls
    back to its interactive checkpoint prompt (we run with stdin=DEVNULL, so a
    prompt would EOF and crash the build). ``resume=True`` continues from an
    existing checkpoint in ``output_dir``; the default starts fresh.
    """
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
    # Negatable booleans (only emit when they differ from the CLI default).
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
    """Where the build writes.

    Priority: explicit output_dir → dated dir under DATA_ROOT → <job_dir>/output.
    The dated name reuses the repo's build-<timestamp> convention:
    <species>-<dataset>-<YYYYMMDD-HHMMSS>.
    """
    if req.output_dir:
        return _abs(req.output_dir)
    if settings.DATA_ROOT:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        name = f"{req.species}-{req.dataset}-{ts}" if req.species else f"build-{ts}"
        return _abs(str(Path(settings.DATA_ROOT) / name))
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
    """Delete stray config/tmp*.yaml left by builds killed before their own cleanup.

    Only runs when no build is currently RUNNING — the temp files aren't mapped to
    a specific job, so we must not delete one an active build is still using.
    """
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
    """Create a job, persist it QUEUED, and start a worker thread that runs the build.

    ``resume=True`` continues from an existing checkpoint in ``req.output_dir``.
    """
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
    registry.prune()  # enforce MAX_BUILD_HISTORY retention

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
    finally:
        _semaphore.release()
        _reap_orphan_temp_schemas()


def resume(job_id: str) -> Optional[BuildJob]:
    """Launch a new job that continues a prior failed/cancelled build's checkpoint.

    Reuses the prior job's output_dir (where kg_checkpoint.json lives) and its
    original parameters, launched with --resume. Returns None if there's no
    checkpoint to resume from.
    """
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
        pass  # already gone

    registry.update(job_id, status=JobStatus.CANCELLED, finished_at=_now_iso(),
                    error="Cancelled by user.")
    with _procs_lock:
        _procs.pop(job_id, None)
    return True
