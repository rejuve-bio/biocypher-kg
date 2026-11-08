"""Console build-management endpoints: validate, launch, track, and cancel builds."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

_GRAPH_INFO_KEYS = [
    "node_count", "edge_count", "dataset_count", "last_updated_at",
    "kg_format", "data_size", "top_entities", "top_connections",
    "datasets",  # per-adapter node/edge type composition
    "adapter_counts",  # what each adapter wrote, and whether a size limit cut it short
    "schema",  # node types + source/target edges (drives the schema graph, any writer)
    "frequent_relationships",
]

from backend.core.console import job_runner
from backend.core.console.job_models import BuildJob, BuildRequest, DownloadRequest, JobStatus
from backend.core.console.job_registry import registry
from backend.core.console.validation import validate_build

router = APIRouter(prefix="/api/console", tags=["Console"])


def _job_or_404(job_id: str) -> BuildJob:
    """Look up a job strictly from the registry (also guards path traversal)."""
    job = registry.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"No build job '{job_id}'.")
    return job


def _enrich(job: BuildJob) -> dict:
    """Job dict + checkpoint summary + resumable flag (shared by list & detail)."""
    data = job.to_dict()
    checkpoint = job_runner.read_checkpoint(job.output_dir) if job.kind == "build" else None
    data["checkpoint"] = checkpoint
    data["resumable"] = checkpoint is not None and job.status in {
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
    data["retryable"] = (job.kind or "").startswith("load-") and job.status in {
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
    if (job.kind or "build") == "build":
        data["loads"] = job_runner.loads_for(job.id)
    return data


@router.post("/builds/{job_id}/retry", status_code=201)
def retry_load(job_id: str):
    """Re-run a failed/cancelled load job (same target + output dir) as a new job."""
    job = _job_or_404(job_id)
    if not (job.kind or "").startswith("load-"):
        raise HTTPException(status_code=400, detail="Retry is only for load jobs.")
    try:
        new_job = job_runner.retry_load(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if new_job is None:
        raise HTTPException(status_code=400, detail="Could not retry this load.")
    return {"id": new_job.id, "status": new_job.status.value,
            "kind": new_job.kind, "retry_of": job_id, "job": new_job.to_dict()}


@router.post("/builds/{job_id}/load/{target}", status_code=201)
def load_build(job_id: str, target: str):
    """Load a succeeded build's output into Neo4j or MORK (surgical/versioned)."""
    job = _job_or_404(job_id)
    if target not in ("neo4j", "mork"):
        raise HTTPException(status_code=400, detail="target must be 'neo4j' or 'mork'.")
    if job.status != JobStatus.SUCCEEDED:
        raise HTTPException(status_code=400, detail="Only a succeeded build can be loaded.")
    writer = (job.params or {}).get("writer_type")
    if target == "neo4j" and writer != "neo4j":
        raise HTTPException(status_code=400,
                            detail="Neo4j load needs a build made with writer_type 'neo4j'.")
    if target == "mork" and writer != "metta":
        raise HTTPException(status_code=400,
                            detail="MORK load needs a build made with writer_type 'metta'.")
    try:
        load_job = job_runner.launch_load(job_id, target)
    except ValueError as exc:
        # e.g. the build's species has no Neo4j/MORK instance configured.
        raise HTTPException(status_code=400, detail=str(exc))
    if load_job is None:
        raise HTTPException(status_code=404, detail="Source build not found.")
    return {"id": load_job.id, "status": load_job.status.value,
            "kind": load_job.kind, "job": load_job.to_dict()}


@router.post("/builds/validate")
def validate(req: BuildRequest):
    """Validate a proposed build without launching it."""
    return validate_build(req)


@router.post("/builds", status_code=201)
def create_build(req: BuildRequest):
    """Validate then launch a build as a tracked job."""
    result = validate_build(req, run_check_only=not req.skip_preflight)
    if not result["valid"]:
        raise HTTPException(status_code=400, detail={"message": "Validation failed",
                                                     "validation": result})
    try:
        job = job_runner.launch(req)
    except ValueError as exc:  # sizes changed since validating, or a folder isn't set
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "id": job.id,
        "status": job.status.value,
        "log_url": f"/api/console/builds/{job.id}/logs",
        "job": job.to_dict(),
    }


@router.get("/builds")
def list_builds(status: Optional[str] = Query(default=None)):
    """List BUILD jobs, newest first (optional ?status= filter); load jobs excluded."""
    builds = [j for j in registry.list(status=status) if (j.kind or "build") == "build"]
    return {"builds": [_enrich(j) for j in builds]}


@router.get("/builds/{job_id}")
def get_build(job_id: str):
    return _enrich(_job_or_404(job_id))


# ── Source-data download ──────────────────────────────────────────────────────

@router.post("/downloads", status_code=201)
def start_download(req: DownloadRequest):
    """Fetch a species' source data into the local input dir (tracked like a build)."""
    try:
        job = job_runner.launch_download(req)
    except ValueError as exc:  # incl. runtime_config.SetupError: a folder isn't set
        raise HTTPException(status_code=400, detail=str(exc))
    if job is None:
        raise HTTPException(status_code=400, detail="Could not start the download.")
    return {"id": job.id, "status": job.status.value, "kind": job.kind, "job": job.to_dict()}


@router.get("/downloads")
def list_downloads(status: Optional[str] = Query(default=None)):
    """List DOWNLOAD jobs, newest first (optional ?status= filter)."""
    jobs = [j for j in registry.list(status=status) if (j.kind or "") == "download"]
    return {"downloads": [j.to_dict() for j in jobs]}


class DbsnpPrepareRequest(BaseModel):
    variant: str = "common"


@router.get("/dbsnp/status")
def get_dbsnp_status(root: Optional[str] = Query(default=None)):
    """Managed dbSNP cache: which variants are ready (size, build time) and any running
    job. With ?root=<path>, also report whether that custom cache root is usable."""
    return job_runner.dbsnp_status(root)


@router.post("/dbsnp/prepare", status_code=201)
def prepare_dbsnp(req: DbsnpPrepareRequest):
    """Build the dbSNP cache for a variant into the data volume (tracked like a build)."""
    try:
        job = job_runner.launch_dbsnp_prepare(req.variant)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"id": job.id, "status": job.status.value, "kind": job.kind, "job": job.to_dict(),
            "root": str(job_runner.dbsnp_root() or "")}


@router.get("/species/{species}/data-status")
def get_data_status(species: str):
    """Which source data is present locally for a species, and how much disk it uses."""
    return job_runner.data_status(species)


@router.post("/builds/{job_id}/resume", status_code=201)
def resume_build(job_id: str):
    """Continue a failed/cancelled build from its checkpoint as a new job."""
    _job_or_404(job_id)
    new_job = job_runner.resume(job_id)
    if new_job is None:
        raise HTTPException(
            status_code=400,
            detail="No checkpoint available to resume this build from.",
        )
    return {
        "id": new_job.id,
        "status": new_job.status.value,
        "resumed_from": job_id,
        "job": new_job.to_dict(),
    }


@router.get("/builds/{job_id}/logs")
def get_logs(job_id: str, tail: int = Query(default=200, ge=1, le=100000)):
    """Return the last ``tail`` lines of the build log."""
    job = _job_or_404(job_id)
    log_path = Path(job.log_path)
    if not log_path.exists():
        return {"id": job_id, "status": job.status.value, "lines": []}
    lines = log_path.read_text(errors="replace").splitlines()
    return {
        "id": job_id,
        "status": job.status.value,
        "return_code": job.return_code,
        "lines": lines[-tail:],
    }


@router.get("/builds/{job_id}/logs/stream")
async def stream_logs(job_id: str):
    """Stream the build log, following new output while the job runs."""
    _job_or_404(job_id)

    async def generator():
        log_path = Path(registry.get(job_id).log_path)
        pos = 0
        while True:
            if log_path.exists():
                with open(log_path, "r", errors="replace") as fh:
                    fh.seek(pos)
                    chunk = fh.read()
                    pos = fh.tell()
                if chunk:
                    yield chunk
            job = registry.get(job_id)
            if job is None or job.status in {JobStatus.SUCCEEDED, JobStatus.FAILED,
                                             JobStatus.CANCELLED}:
                # final flush of anything written after the last read
                if log_path.exists():
                    with open(log_path, "r", errors="replace") as fh:
                        fh.seek(pos)
                        tail = fh.read()
                    if tail:
                        yield tail
                break
            await asyncio.sleep(1.0)

    return StreamingResponse(generator(), media_type="text/plain")


@router.get("/builds/{job_id}/output")
def list_output(job_id: str, limit: int = Query(default=1000, ge=1, le=20000)):
    """List files the build produced under its output dir (path + size)."""
    job = _job_or_404(job_id)
    base = Path(job.output_dir)
    if not base.is_dir():
        return {"output_dir": str(base), "exists": False, "count": 0, "files": []}
    files = [
        {"path": str(p.relative_to(base)), "size": p.stat().st_size}
        for p in sorted(base.rglob("*")) if p.is_file()
    ]
    return {
        "output_dir": str(base),
        "exists": True,
        "count": len(files),
        "truncated": len(files) > limit,
        "files": files[:limit],
    }


@router.get("/builds/{job_id}/graph-info")
def build_graph_info(job_id: str):
    """Return the build's graph_info.json summary (node/edge counts, top types)."""
    job = _job_or_404(job_id)
    p = Path(job.output_dir) / "graph_info.json"
    if not p.exists():
        return {"present": False}
    try:
        data = json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return {"present": False}
    return {"present": True, "summary": {k: data.get(k) for k in _GRAPH_INFO_KEYS}}


@router.get("/builds/{job_id}/config-snapshot")
def build_config_snapshot(job_id: str):
    """Return the recorded config manifest (hashes + source paths) for a build."""
    job = _job_or_404(job_id)
    p = Path(job.output_dir) / "build_config" / "manifest.json"
    if not p.exists():
        return {"present": False}
    try:
        return {"present": True, "manifest": json.loads(p.read_text())}
    except (json.JSONDecodeError, OSError):
        return {"present": False}


@router.get("/builds/{job_id}/output/download")
def download_output(job_id: str, path: str = Query(...)):
    """Download a single output file. Confined to the build's output dir."""
    job = _job_or_404(job_id)
    base = Path(job.output_dir).resolve()
    target = (base / path).resolve()
    if target != base and base not in target.parents:
        raise HTTPException(status_code=400, detail="Path escapes the output directory.")
    if not target.is_file():
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(str(target), filename=target.name)


@router.delete("/builds/{job_id}")
def delete_build(job_id: str, purge_output: bool = Query(default=False)):
    """Cancel a running/queued job, or remove a finished one from the registry.

    With ?purge_output=true, also delete the build's output folder on disk (for
    reclaiming space on a local machine). The registry's own job dir is always
    removed; the large output under DATA_ROOT is only removed when purge is set."""
    job = _job_or_404(job_id)
    if job.status in {JobStatus.QUEUED, JobStatus.RUNNING}:
        job_runner.cancel(job_id)
        return {"id": job_id, "status": registry.get(job_id).status.value,
                "action": "cancelled"}
    purged = False
    if purge_output and job.output_dir:
        purged = job_runner.purge_output_dir(job.output_dir)
    registry.remove(job_id)
    return {"id": job_id, "action": "removed", "output_purged": purged}


@router.get("/disk")
def disk_usage():
    """Local disk usage for the console (data root total, builds, input, free space)."""
    return job_runner.disk_usage()


@router.get("/sizes")
def known_sizes(species: str, dataset: str):
    """Each adapter's known full size for this species+dataset (see sizes.py): what
    the console has learned from builds that weren't cut short by a size limit."""
    from backend.core.console import sizes

    return {"species": species, "dataset": dataset, "adapters": sizes.known(species, dataset)}


