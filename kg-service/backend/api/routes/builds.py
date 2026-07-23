"""Console build-management endpoints: validate, launch, track, and cancel builds."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

_GRAPH_INFO_KEYS = [
    "node_count", "edge_count", "dataset_count", "last_updated_at",
    "kg_format", "data_size", "top_entities", "top_connections",
]

from backend.core.console import job_runner
from backend.core.console.job_models import BuildJob, BuildRequest, JobStatus
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
    # Checkpoint/resume only apply to build jobs, not load jobs.
    checkpoint = job_runner.read_checkpoint(job.output_dir) if job.kind == "build" else None
    data["checkpoint"] = checkpoint
    data["resumable"] = checkpoint is not None and job.status in {
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
    # Load jobs can be retried (re-run the same loader) when they didn't succeed.
    data["retryable"] = (job.kind or "").startswith("load-") and job.status in {
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
    return data


@router.post("/builds/{job_id}/retry", status_code=201)
def retry_load(job_id: str):
    """Re-run a failed/cancelled load job (same target + output dir) as a new job."""
    job = _job_or_404(job_id)
    if not (job.kind or "").startswith("load-"):
        raise HTTPException(status_code=400, detail="Retry is only for load jobs.")
    new_job = job_runner.retry_load(job_id)
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
    load_job = job_runner.launch_load(job_id, target)
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
    job = job_runner.launch(req)
    return {
        "id": job.id,
        "status": job.status.value,
        "log_url": f"/api/console/builds/{job.id}/logs",
        "job": job.to_dict(),
    }


@router.get("/builds")
def list_builds(status: Optional[str] = Query(default=None)):
    """List build jobs, newest first. Optional ?status= filter."""
    return {"builds": [_enrich(j) for j in registry.list(status=status)]}


@router.get("/builds/{job_id}")
def get_build(job_id: str):
    return _enrich(_job_or_404(job_id))


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
                # one last flush of anything written after the final read
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
def delete_build(job_id: str):
    """Cancel a running/queued job, or remove a finished one from the registry."""
    job = _job_or_404(job_id)
    if job.status in {JobStatus.QUEUED, JobStatus.RUNNING}:
        job_runner.cancel(job_id)
        return {"id": job_id, "status": registry.get(job_id).status.value,
                "action": "cancelled"}
    registry.remove(job_id)
    return {"id": job_id, "action": "removed"}
