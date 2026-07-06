"""JSON-persisted registry of build jobs, safe for concurrent access.

The registry file (``<BUILDS_DIR>/registry.json``) is the source of truth so jobs
survive an API restart. On startup, ``reconcile()`` repairs the status of jobs that
were RUNNING when the process died.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from backend.core.config import settings
from backend.core.console.job_models import BuildJob, JobStatus, TERMINAL_STATUSES

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists but owned by another user — treat as alive.
        return True
    return True


class JobRegistry:
    def __init__(self, builds_dir: Optional[Path] = None):
        self._lock = threading.RLock()
        self._builds_dir = builds_dir or settings.builds_dir
        self._jobs: dict[str, BuildJob] = {}
        self._builds_dir.mkdir(parents=True, exist_ok=True)
        self._load()

    @property
    def registry_file(self) -> Path:
        return self._builds_dir / "registry.json"

    def job_dir(self, job_id: str) -> Path:
        return self._builds_dir / job_id

    # ---- persistence ----
    def _load(self) -> None:
        if not self.registry_file.exists():
            return
        try:
            data = json.loads(self.registry_file.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Could not read build registry %s: %s", self.registry_file, exc)
            return
        with self._lock:
            self._jobs = {
                jid: BuildJob.from_dict(jd) for jid, jd in data.get("jobs", {}).items()
            }

    def _save_locked(self) -> None:
        payload = {"jobs": {jid: job.to_dict() for jid, job in self._jobs.items()}}
        tmp = self.registry_file.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        os.replace(tmp, self.registry_file)  # atomic on POSIX

    # ---- CRUD ----
    def add(self, job: BuildJob) -> None:
        with self._lock:
            self._jobs[job.id] = job
            self._save_locked()

    def get(self, job_id: str) -> Optional[BuildJob]:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self, status: Optional[str] = None) -> list[BuildJob]:
        with self._lock:
            jobs = list(self._jobs.values())
        if status:
            jobs = [j for j in jobs if j.status.value == status]
        # newest first (created_at is ISO8601, lexicographically sortable)
        jobs.sort(key=lambda j: j.created_at or "", reverse=True)
        return jobs

    def update(self, job_id: str, **fields) -> Optional[BuildJob]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            for k, v in fields.items():
                setattr(job, k, v)
            self._save_locked()
            return job

    def remove(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                self._save_locked()
                # Drop the job's artifacts dir (logs + default output). A custom
                # output_dir lives outside .builds and is intentionally left alone.
                shutil.rmtree(self.job_dir(job_id), ignore_errors=True)
                return True
            return False

    def prune(self, keep: Optional[int] = None) -> int:
        """Retain only the newest ``keep`` terminal jobs; delete older ones.

        Never touches QUEUED/RUNNING jobs. Returns how many were removed.
        """
        keep = keep if keep is not None else settings.MAX_BUILD_HISTORY
        with self._lock:
            terminal = [j for j in self._jobs.values() if j.status in TERMINAL_STATUSES]
            terminal.sort(key=lambda j: j.created_at or "", reverse=True)
            to_delete = terminal[keep:]
            for job in to_delete:
                del self._jobs[job.id]
                shutil.rmtree(self.job_dir(job.id), ignore_errors=True)
            if to_delete:
                self._save_locked()
            return len(to_delete)

    # ---- restart repair ----
    def reconcile(self) -> None:
        """Repair jobs left RUNNING by a previous process instance.

        Returns nothing; logs what it changed. Jobs whose PID is gone are marked
        FAILED (orphaned). Live PIDs are left RUNNING (a fresh monitor is attached
        by the caller if desired).
        """
        with self._lock:
            changed = False
            for job in self._jobs.values():
                if job.status in (JobStatus.RUNNING, JobStatus.QUEUED):
                    if not _pid_alive(job.pid):
                        job.status = JobStatus.FAILED
                        job.error = "Process not found after API restart (orphaned)."
                        job.finished_at = job.finished_at or _now_iso()
                        changed = True
                        logger.warning("Reconciled orphaned job %s -> FAILED", job.id)
            if changed:
                self._save_locked()


# Module-level singleton used by the runner and routes.
registry = JobRegistry()
