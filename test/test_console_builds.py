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


def _wait_until_idle(timeout=30.0):
    """Wait for every build/load thread to finish, including the clean-up of old
    builds each one runs after succeeding (it happens after the job is marked
    done, so waiting for statuses isn't enough)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        queued = any(j.status in {JobStatus.QUEUED, JobStatus.RUNNING} for j in registry.list())
        if job_runner._semaphore._running == 0 and not queued:
            return
        time.sleep(0.05)


def test_build_runs_to_success(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, **_: [sys.executable, "-c", "print('hello-from-build')"])
    req = BuildRequest(species="hsa", dataset="sample", include_adapters=["gencode_gene"])
    job = job_runner.launch(req)
    final = _wait_for(job.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert final.status == JobStatus.SUCCEEDED
    assert final.return_code == 0
    assert "hello-from-build" in open(final.log_path).read()


def test_build_nonzero_exit_fails(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, **_: [sys.executable, "-c", "import sys; sys.exit(3)"])
    job = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    final = _wait_for(job.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert final.status == JobStatus.FAILED
    assert final.return_code == 3


def test_cancel_running_job(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, **_: [sys.executable, "-c", "import time; time.sleep(60)"])
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
    monkeypatch.setattr(settings, "OUTPUT_ROOT", str(tmp_path))
    req = BuildRequest(species="hsa", dataset="sample")  # no output_dir; writer_type defaults
    out = resolve_output_dir(req, tmp_path / "job")
    assert out.startswith(str(tmp_path.resolve()))
    # Grouped by writer type: <Builds folder>/<writer>/<species>-<dataset>-<timestamp>
    assert re.search(rf"/{req.writer_type}/hsa-sample-\d{{8}}-\d{{6}}$", out), out


def test_resolve_output_dir_explicit_wins(monkeypatch, tmp_path):
    from backend.core.config import settings
    from backend.core.console.job_runner import resolve_output_dir
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path))
    req = BuildRequest(species="hsa", dataset="sample", output_dir="/custom/out")
    assert resolve_output_dir(req, tmp_path / "job") == "/custom/out"


def test_build_load_argv_targets(monkeypatch):
    from backend.core.console import connections, explore_targets as et
    from backend.core.console.job_runner import archive_base_for, build_load_argv
    monkeypatch.setattr(connections, "neo4j_login", lambda sp: {
        "uri": "bolt://lab:7687", "user": "alice", "password": "pw", "database": "kg"})
    monkeypatch.setattr(et, "mork_url_for", lambda sp: "http://mork-hsa:8027")
    n = build_load_argv("neo4j", "/out", "hsa")
    assert "kg-service/neo4j_loader.py" in n
    assert "--output-dir" in n and "/out" in n and "--uri" in n
    # The archive folder is passed as is; the loader appends neo4j/ or mork/ itself.
    assert n[n.index("--archive-dir") + 1] == archive_base_for("hsa")
    m = build_load_argv("mork", "/out", "hsa")
    assert "kg-service/mork_loader.py" in m
    assert "--data-dir" in m and "/out" in m
    assert m[m.index("--archive-dir") + 1] == archive_base_for("hsa")
    # Every load goes to a species' own database: no shared default to fall back on.
    with pytest.raises(ValueError, match="species"):
        build_load_argv("neo4j", "/out")


def test_each_species_keeps_its_own_load_archives(monkeypatch):
    """Each species has its own databases, so its loads keep their own version
    metadata: loading one species must not overwrite another's."""
    from backend.core.config import settings
    from backend.core.console import explore_targets as et

    monkeypatch.setattr(et, "mork_url_for", lambda sp: f"http://mork-{sp}:8027")
    hsa = job_runner.build_load_argv("mork", "/out", "hsa")
    dmel = job_runner.build_load_argv("mork", "/out", "dmel")
    base = f"{settings.DATA_ROOT.rstrip('/')}/archives"  # kept in the console data folder
    assert hsa[hsa.index("--archive-dir") + 1] == f"{base}/hsa"
    assert dmel[dmel.index("--archive-dir") + 1] == f"{base}/dmel"
    monkeypatch.setattr(settings, "DATA_ROOT", "")
    assert job_runner.archive_base_for("hsa") is None  # no data folder, no archives


def test_launch_load_creates_tracked_load_job(monkeypatch):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, **_: [sys.executable, "-c", "pass"])
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out, species=None: [sys.executable, "-c", "print('loaded')"])
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
                        lambda req, out, **_: [sys.executable, "-c", "pass"])
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out, species=None: [sys.executable, "-c", "import sys; sys.exit(1)"])
    src = job_runner.launch(BuildRequest(species="hsa", dataset="sample"))
    _wait_for(src.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})

    load = job_runner.launch_load(src.id, "mork")
    failed = _wait_for(load.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    assert failed.status == JobStatus.FAILED  # loader exited non-zero

    # Retry a passing loader this time.
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out, species=None: [sys.executable, "-c", "print('retried')"])
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
                        lambda req, out, **_: [sys.executable, "-c", "pass"])
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
                        lambda req, out, **_: [sys.executable, "-c", "pass"])
    monkeypatch.setattr(job_runner, "build_load_argv",
                        lambda target, out, species=None: [sys.executable, "-c", "print('loaded')"])
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


def test_prune_output_dirs_keeps_newest_per_writer_and_species(monkeypatch, tmp_path):
    from backend.core.config import settings
    # Builds from earlier tests finish in the background and run their own clean-up
    # of old builds; with DATA_ROOT pointed here that would race this one.
    _wait_until_idle()
    monkeypatch.setattr(settings, "OUTPUT_ROOT", str(tmp_path))
    monkeypatch.setattr(settings, "MAX_OUTPUT_BUILDS", 5)

    def make(rel: str):
        d = tmp_path / rel
        d.mkdir(parents=True)
        (d / "graph_info.json").write_text("{}")

    for i in range(7):   # metta/hsa: 7 → keep 5
        make(f"metta/hsa-sample-202607{i:02d}-000000")
    for i in range(3):   # metta/mmu: 3 → keep all 3
        make(f"metta/mmu-sample-202607{i:02d}-000000")
    for i in range(6):   # neo4j/hsa: 6 → keep 5
        make(f"neo4j/hsa-sample-202607{i:02d}-000000")
    # archives must never be touched
    (tmp_path / "archives" / "mork").mkdir(parents=True)
    (tmp_path / "archives" / "mork" / "version_metadata.json").write_text("{}")

    removed = job_runner.prune_output_dirs()
    assert removed == 3  # 2 from metta/hsa + 1 from neo4j/hsa

    def kept(writer, species):
        return sorted(p.name for p in (tmp_path / writer).iterdir()
                      if p.name.startswith(species + "-"))

    assert len(kept("metta", "hsa")) == 5
    assert kept("metta", "hsa")[0] == "hsa-sample-20260702-000000"  # oldest two removed
    assert len(kept("metta", "mmu")) == 3          # under the cap, all kept
    assert len(kept("neo4j", "hsa")) == 5          # neo4j/hsa capped independently
    assert (tmp_path / "archives" / "mork" / "version_metadata.json").exists()


def test_snapshot_build_config_records_resolved_config_and_hash(tmp_path):
    job = BuildJob(
        id="snap1", status=JobStatus.SUCCEEDED, kind="build",
        params={"species": "hsa", "dataset": "sample", "writer_type": "metta"},
        cmd=["x"], cwd=".", output_dir=str(tmp_path),
        log_path=str(tmp_path / "l.log"), created_at="2026-01-01T00:00:00Z",
    )
    manifest = job_runner.snapshot_build_config(job)
    assert manifest is not None
    assert manifest["species"] == "hsa"
    assert len(manifest["config_hash"]) == 64  # sha256 hex
    snap = tmp_path / "build_config"
    assert (snap / "adapters_config.yaml").read_text().strip()
    assert (snap / "schema_config.yaml").read_text().strip()
    assert (snap / "manifest.json").exists()


def test_snapshot_build_config_none_without_species(tmp_path):
    job = BuildJob(
        id="snap2", status=JobStatus.SUCCEEDED, kind="build", params={},
        cmd=["x"], cwd=".", output_dir=str(tmp_path),
        log_path=str(tmp_path / "l.log"), created_at="2026-01-01T00:00:00Z",
    )
    assert job_runner.snapshot_build_config(job) is None


def test_build_argv_resume_flag():
    from backend.core.console.job_runner import build_argv
    req = BuildRequest(species="hsa", dataset="sample")
    assert "--restart" in build_argv(req, "/tmp/out", resume=False)
    assert "--resume" in build_argv(req, "/tmp/out", resume=True)


def test_resume_without_checkpoint_returns_none(monkeypatch, tmp_path):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, **_: [sys.executable, "-c", "pass"])
    req = BuildRequest(species="hsa", dataset="sample")
    req.output_dir = str(tmp_path / "nockpt")
    job = job_runner.launch(req)
    _wait_for(job.id, {JobStatus.SUCCEEDED, JobStatus.FAILED})
    # no kg_checkpoint.json was written → not resumable
    assert job_runner.resume(job.id) is None


def test_resume_with_checkpoint_launches_new_job(monkeypatch, tmp_path):
    monkeypatch.setattr(job_runner, "build_argv",
                        lambda req, out, **_: [sys.executable, "-c", "pass"])
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


# ── Source-scoped downloads + the console-managed dbSNP cache ──────────────────

def test_build_download_argv_multiple_sources():
    """Several selected sources become repeated --source flags, de-duplicated."""
    from backend.core.console.job_models import DownloadRequest
    argv = job_runner.build_download_argv(
        DownloadRequest(species="hsa", source="reactome", sources=["tflink", "reactome"]),
        "/in/hsa",
    )
    picked = [argv[i + 1] for i, a in enumerate(argv) if a == "--source"]
    assert picked == ["reactome", "tflink"]


def test_dbsnp_status_reports_prepared_variants(monkeypatch, tmp_path):
    from backend.core.config import settings
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path))
    from dbsnp_cache_helper import make_dbsnp_cache

    db = make_dbsnp_cache(tmp_path / "dbsnp" / "common", "common")
    from backend.core.console import runtime_config
    monkeypatch.setattr(runtime_config, "data_locations", _locs())
    status = job_runner.dbsnp_status()
    assert status["root"] == str(tmp_path / "dbsnp")
    assert status["variants"]["common"] == {
        "present": True, "bytes": db.stat().st_size, "built_at": "2026-11-01T02:00:00",
        "problem": None, "root": str(tmp_path / "dbsnp"),
    }
    assert status["variants"]["full"]["present"] is False


def test_dbsnp_status_checks_custom_root(monkeypatch, tmp_path):
    from backend.core.config import settings
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path / "data"))
    from dbsnp_cache_helper import make_dbsnp_cache

    make_dbsnp_cache(tmp_path / "mine" / "full", "full")
    status = job_runner.dbsnp_status(str(tmp_path / "mine"))
    assert status["custom"]["variants"]["full"]["present"] is True
    assert status["custom"]["variants"]["common"]["present"] is False


def test_purge_never_deletes_dbsnp_cache(monkeypatch, tmp_path):
    """The shared dbSNP cache isn't one job's output; purging it must be refused."""
    from backend.core.config import settings
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path))
    common = tmp_path / "dbsnp" / "common"
    common.mkdir(parents=True)
    assert job_runner.purge_output_dir(str(common)) is False
    assert common.is_dir()


def test_data_status_finds_sources_outside_their_key_dir(monkeypatch, tmp_path):
    """Presence follows the files a source declares (gtex_eqtl lands in gtex/eqtl/)
    and versioned dirs (catlas in catlasv1/), not only input/<key>/."""
    from backend.core.config import settings
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path))
    hsa = tmp_path / "input" / "hsa"
    (hsa / "gtex" / "eqtl").mkdir(parents=True)
    (hsa / "gtex" / "eqtl" / "gtex.forgedb.csv.gz").write_bytes(b"x" * 5)
    (hsa / "catlasv1").mkdir(parents=True)
    (hsa / "catlasv1" / "cCRE_hg38.tsv.gz").write_bytes(b"x" * 7)
    by_key = {s["key"]: s for s in job_runner.data_status("hsa")["sources"]}
    assert by_key["gtex_eqtl"]["present"] is True
    assert by_key["gtex_eqtl"]["bytes"] == 5
    assert by_key["catlas"]["present"] is True
    assert by_key["reactome"]["present"] is False


# ── Data you already have: Settings > Data locations ──────────────────────────

def _locs(input_dirs=None, dbsnp_root=None):
    return lambda: {"input_dirs": input_dirs or {}, "dbsnp_root": dbsnp_root}


def test_settings_folder_is_read_first_but_never_the_download_target(monkeypatch, tmp_path):
    from backend.core.config import settings
    from backend.core.console import runtime_config
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setattr(job_runner, "config_input_dir", lambda sp: None)
    mine = tmp_path / "mine" / "hsa"
    monkeypatch.setattr(runtime_config, "data_locations", _locs({"hsa": str(mine)}))
    loc = job_runner.resolve_species_input("hsa")
    downloads = str((tmp_path / "data" / "input" / "hsa").resolve())
    assert loc == {"path": downloads, "origin": "console", "readonly": str(mine.resolve())}
    assert job_runner.species_input_dir("hsa") == downloads
    assert job_runner.species_read_dirs("hsa") == [str(mine.resolve()), downloads]


def test_source_data_folders_are_only_ever_ones_set_in_settings(monkeypatch, tmp_path):
    """No defaults: an adapters config's input_dir that exists here (a server's, say)
    is never used on its own, and without a data folder there's nowhere to download."""
    from backend.core.config import settings
    from backend.core.console import runtime_config
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setattr(runtime_config, "data_locations", _locs())
    server = tmp_path / "server" / "hsa"
    server.mkdir(parents=True)
    monkeypatch.setattr(job_runner, "config_input_dir", lambda sp: str(server))
    assert job_runner.resolve_species_input("hsa") == {
        "path": str((tmp_path / "data" / "input" / "hsa").resolve()), "origin": "console",
        "readonly": None,
    }
    monkeypatch.setattr(settings, "DATA_ROOT", "")
    assert job_runner.resolve_species_input("hsa")["path"] is None
    assert job_runner.species_read_dirs("hsa") == []


def test_effective_input_dirs_read_only_first_and_sample_untouched(monkeypatch, tmp_path):
    from backend.core.config import settings
    from backend.core.console import runtime_config
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setattr(job_runner, "config_input_dir", lambda sp: None)
    mine = tmp_path / "mine" / "hsa"
    mine.mkdir(parents=True)
    downloads = tmp_path / "data" / "input" / "hsa"
    (downloads / "abc").mkdir(parents=True)
    monkeypatch.setattr(runtime_config, "data_locations", _locs({"hsa": str(mine)}))
    assert job_runner.effective_input_dirs(BuildRequest(species="hsa", dataset="sample")) == []
    dirs = job_runner.effective_input_dirs(BuildRequest(species="hsa", dataset="full"))
    assert dirs == [str(mine.resolve()), str(downloads.resolve())]
    argv = job_runner.build_argv(BuildRequest(species="hsa", dataset="full"), "/out")
    i = argv.index("--input-dir")
    assert argv[i:i + 4] == ["--input-dir", dirs[0], "--extra-input-dir", dirs[1]]


def test_data_status_reads_your_folder_first(monkeypatch, tmp_path):
    from backend.core.config import settings
    from backend.core.console import runtime_config
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setattr(job_runner, "config_input_dir", lambda sp: None)
    mine = tmp_path / "mine" / "hsa"
    (mine / "reactome").mkdir(parents=True)
    (mine / "reactome" / "Ensembl2Reactome_All_Levels.txt").write_text("x")
    downloads = tmp_path / "data" / "input" / "hsa"
    (downloads / "abc").mkdir(parents=True)
    (downloads / "abc" / "abc.forgedb.csv.gz").write_bytes(b"x")
    monkeypatch.setattr(runtime_config, "data_locations", _locs({"hsa": str(mine)}))
    status = job_runner.data_status("hsa")
    by_key = {s["key"]: s for s in status["sources"]}
    assert status["readonly_dir"] == str(mine.resolve())
    assert by_key["reactome"]["location"] == "yours"
    assert by_key["abc"]["location"] == "console"
    assert by_key["uniprot"]["present"] is False


def test_download_never_writes_into_a_read_only_folder(monkeypatch, tmp_path):
    """If the download folder would fall inside your read-only data, refuse outright."""
    from backend.core.config import settings
    from backend.core.console import runtime_config
    from backend.core.console.job_models import DownloadRequest
    data = tmp_path / "data"
    monkeypatch.setattr(settings, "DATA_ROOT", str(data))
    monkeypatch.setattr(job_runner, "config_input_dir", lambda sp: None)
    # "Your" folder is the whole data root, so <data>/input/hsa would sit inside it.
    monkeypatch.setattr(runtime_config, "data_locations", _locs({"hsa": str(data)}))
    with pytest.raises(ValueError, match="read-only"):
        job_runner.launch_download(DownloadRequest(species="hsa", sources=["reactome"]))


def test_purge_never_deletes_a_settings_data_folder(monkeypatch, tmp_path):
    """Purge must refuse the user's own data (chosen in Settings), even with no
    DATA_ROOT guard."""
    from backend.core.config import settings
    from backend.core.console import runtime_config
    monkeypatch.setattr(settings, "DATA_ROOT", "")
    mine = tmp_path / "mine" / "hsa"
    mine.mkdir(parents=True)
    monkeypatch.setattr(runtime_config, "data_locations", _locs({"hsa": str(mine)}))
    monkeypatch.setattr(job_runner, "config_input_dir", lambda sp: None)
    assert job_runner.purge_output_dir(str(mine)) is False
    assert job_runner.purge_output_dir(str(tmp_path / "mine")) is False  # its parent
    assert mine.is_dir()


def test_settings_dbsnp_cache_is_read_only(monkeypatch, tmp_path):
    """A dbSNP cache chosen in Settings is used for the variants it has, but Prepare
    writes to the console's own folder."""
    from backend.core.config import settings
    from backend.core.console import runtime_config
    monkeypatch.setattr(settings, "DATA_ROOT", str(tmp_path / "data"))
    from dbsnp_cache_helper import make_dbsnp_cache

    mine = tmp_path / "mine"
    make_dbsnp_cache(mine / "common", "common")
    monkeypatch.setattr(runtime_config, "data_locations", _locs(dbsnp_root=str(mine)))
    status = job_runner.dbsnp_status()
    assert status["variants"]["common"]["root"] == str(mine.resolve())
    assert status["variants"]["full"]["root"] == str(tmp_path / "data" / "dbsnp")
    assert job_runner.dbsnp_root() == tmp_path / "data" / "dbsnp"
