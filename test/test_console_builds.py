"""Tests for the Console build job system (registry + runner + endpoints).

No real KG builds run here: the runner's argv is monkeypatched to trivial commands.
"""
import sys
import time

import pytest

from backend.core.console import job_runner
from backend.core.console.job_models import BuildJob, BuildRequest, JobStatus
from backend.core.console.job_registry import JobRegistry, registry


def _wait_for(job_id, statuses, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = registry.get(job_id)
        if job and job.status in statuses:
            return job
        time.sleep(0.05)
    return registry.get(job_id)


def test_build_runs_to_success(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "print('hello-from-build')"])
    req = BuildRequest(species="hsa", dataset="sample", include_adapters=["gencode_gene"])
    job = job_runner.launch(req)
    final = _wait_for(job.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert final.status == JobStatus.SUCCEEDED
    assert final.return_code == 0
    assert "hello-from-build" in open(final.log_path).read()


def test_build_nonzero_exit_fails(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "import sys; sys.exit(3)"])
    job = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    final = _wait_for(job.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert final.status == JobStatus.FAILED
    assert final.return_code == 3


def test_cancel_running_job(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "import time; time.sleep(60)"])
    job = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    running = _wait_for(job.id, {JobStatus.RUNNING})
    assert running.status == JobStatus.RUNNING
    assert job_runner.cancel(job.id) is True
    final = _wait_for(job.id, {JobStatus.CANCELLED})
    assert final.status == JobStatus.CANCELLED


def test_registry_persists_across_reload(tmp_path):
    reg = JobRegistry(builds_dir=tmp_path)
    job = BuildJob(id="abc123", status=JobStatus.SUCCEEDED, params={}, cmd=["x"],
                   cwd=".", output_dir=str(tmp_path / "out"),
                   log_path=str(tmp_path / "build.log"), created_at="2026-01-01T00:00:00Z")
    reg.add(job)

    reloaded = JobRegistry(builds_dir=tmp_path)
    got = reloaded.get("abc123")
    assert got is not None
    assert got.status == JobStatus.SUCCEEDED


def test_resolve_output_dir_dated_when_blank(monkeypatch, tmp_path):
    from backend.core.config import settings
    from backend.core.console.job_runner import resolve_output_dir
    import re
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path))
    req = BuildRequest(species="hsa", dataset="sample")  # no output_dir; writer_type defaults
    out = resolve_output_dir(req, tmp_path / "job")
    assert out.startswith(str(tmp_path))
    # Grouped by writer type: <DATA_ROOT>/<writer>/<species>-<dataset>-<timestamp>
    assert re.search(rf"/{req.writer_type}/hsa-sample-\d{{8}}-\d{{6}}$", out), out


def test_resolve_output_dir_explicit_wins(monkeypatch, tmp_path):
    from backend.core.config import settings
    from backend.core.console.job_runner import resolve_output_dir
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path))
    req = BuildRequest(species="hsa", dataset="sample", output_dir="/custom/out")
    assert resolve_output_dir(req, tmp_path / "job") == "/custom/out"


def test_build_load_argv_targets():
    from backend.core.console.job_runner import build_load_argv
    from backend.core.config import settings
    n = build_load_argv("neo4j", "/out")
    assert "kg-service/neo4j_loader.py" in n
    assert "--output-dir" in n and "/out" in n and "--uri" in n
    # ARCHIVE_BASE is passed verbatim; the loader appends the target subdir itself
    # (…/neo4j, …/mork) — matching what versions.py reads. Do NOT pre-append target.
    assert n[n.index("--archive-dir") + 1] == settings.ARCHIVE_BASE
    m = build_load_argv("mork", "/out")
    assert "kg-service/mork_loader.py" in m
    assert "--data-dir" in m and "/out" in m
    assert m[m.index("--archive-dir") + 1] == settings.ARCHIVE_BASE


def test_launch_load_creates_tracked_load_job(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "pass"])
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out: [sys.executable, "-c", "print('loaded')"])
    src = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    _wait_for(src.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})

    load = job_runner.launch_load(src.id, "neo4j")
    assert load is not None and load.kind == "load-neo4j"
    assert load.output_dir == src.output_dir       # loads the build's output
    final = _wait_for(load.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert final.status == JobStatus.SUCCEEDED
    assert "loaded" in open(final.log_path).read()


def test_retry_load_reruns_same_target_and_dir(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "pass"])
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out: [sys.executable, "-c", "import sys; sys.exit(1)"])
    src = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    _wait_for(src.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})

    load = job_runner.launch_load(src.id, "mork")
    failed = _wait_for(load.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert failed.status == JobStatus.FAILED  # loader exited non-zero

    # Retry a passing loader this time.
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out: [sys.executable, "-c", "print('retried')"])
    retry = job_runner.retry_load(load.id)
    assert retry is not None
    assert retry.id != load.id
    assert retry.kind == "load-mork"
    assert retry.output_dir == load.output_dir
    assert (retry.params or {}).get("retry_of") == load.id
    final = _wait_for(retry.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert final.status == JobStatus.SUCCEEDED
    assert "retried" in open(final.log_path).read()


def test_retry_load_rejects_build_jobs(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "pass"])
    src = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    _wait_for(src.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    # A build job isn't a load job → not retryable.
    assert job_runner.retry_load(src.id) is None
    # Unknown id → None.
    assert job_runner.retry_load("does-not-exist") is None


def test_load_summarized_on_build_not_a_separate_build(monkeypatch):
    """A load is an action on a build: it's a load-kind job (the /builds list filters
    those out), and the build reports it via loads_for()."""
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "pass"])
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out: [sys.executable, "-c", "print('loaded')"])
    src = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    _wait_for(src.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    load = job_runner.launch_load(src.id, "neo4j")
    _wait_for(load.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})

    # The load is a load-kind job → excluded from the build list by the route.
    assert (load.kind or "").startswith("load-")
    # The build reports where it was loaded.
    summary = job_runner.loads_for(src.id)
    assert summary["neo4j"]["job_id"] == load.id
    assert summary["neo4j"]["status"] == "succeeded"


def test_build_argv_resume_flag():
    from backend.core.console.job_runner import build_argv
    req = BuildRequest(species="hsa", dataset="sample")
    assert "--restart" in build_argv(req, "/tmp/out", resume=False)
    assert "--resume" in build_argv(req, "/tmp/out", resume=True)


def test_resume_without_checkpoint_returns_none(monkeypatch, tmp_path):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "pass"])
    req = BuildRequest(species="hsa", dataset="sample")
    req.output_dir = str(tmp_path / "nockpt")
    job = job_runner.launch(req)
    _wait_for(job.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    # no kg_checkpoint.json was written → not resumable
    assert job_runner.resume(job.id) is None


def test_resume_with_checkpoint_launches_new_job(monkeypatch, tmp_path):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, resume=False: [sys.executable, "-c", "pass"])
    out = tmp_path / "withckpt"
    out.mkdir()
    (out / "kg_checkpoint.json").write_text(
        '{"completed_adapters": ["gencode_gene"], "failed_adapter": "uniprot"}'
    )
    req = BuildRequest(species="hsa", dataset="sample")
    req.output_dir = str(out)
    prior = job_runner.launch(req)
    _wait_for(prior.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})

    new_job = job_runner.resume(prior.id)
    assert new_job is not None
    assert new_job.id != prior.id
    assert new_job.output_dir == str(out)  # reuses the checkpoint's dir


def test_prune_keeps_newest_terminal_jobs(tmp_path):
    reg = JobRegistry(builds_dir=tmp_path)
    for i in range(5):
        reg.add(BuildJob(id=f"j{i}", status=JobStatus.SUCCEEDED, params={}, cmd=["x"],
                         cwd=".", output_dir=str(tmp_path / f"j{i}"),
                         log_path=str(tmp_path / f"j{i}.log"),
                         created_at=f"2026-01-0{i}T00:00:00Z"))
    removed = reg.prune(keep=2)
    assert removed == 3
    remaining = {j.id for j in reg.list()}
    assert remaining == {"j4", "j3"}  # newest two kept


def test_prune_never_removes_running(tmp_path):
    reg = JobRegistry(builds_dir=tmp_path)
    reg.add(BuildJob(id="run", status=JobStatus.RUNNING, params={}, cmd=["x"], cwd=".",
                     output_dir=str(tmp_path), log_path=str(tmp_path / "l"),
                     created_at="2026-01-01T00:00:00Z"))
    reg.add(BuildJob(id="done", status=JobStatus.SUCCEEDED, params={}, cmd=["x"], cwd=".",
                     output_dir=str(tmp_path), log_path=str(tmp_path / "l2"),
                     created_at="2026-01-02T00:00:00Z"))
    reg.prune(keep=0)  # would delete all terminal, but never RUNNING
    ids = {j.id for j in reg.list()}
    assert "run" in ids and "done" not in ids


def test_reconcile_marks_orphaned_running_as_failed(tmp_path):
    reg = JobRegistry(builds_dir=tmp_path)
    # a job that was RUNNING with a PID that no longer exists
    dead_pid = 2**31 - 1  # implausible pid
    reg.add(BuildJob(id="orphan", status=JobStatus.RUNNING, params={}, cmd=["x"],
                     cwd=".", output_dir=str(tmp_path), log_path=str(tmp_path / "l.log"),
                     pid=dead_pid, created_at="2026-01-01T00:00:00Z"))
    reg.reconcile()
    got = reg.get("orphan")
    assert got.status == JobStatus.FAILED
    assert "orphaned" in (got.error or "").lower()
