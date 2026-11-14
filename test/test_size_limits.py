"""Size limits: rows per adapter, or a percent of each adapter's known size.

The build caps rows per adapter and records what each adapter wrote, marking
counts a cap cut short; the console learns only complete counts and turns a
percentage into per-adapter caps from them."""
import json

import pytest

import create_knowledge_graph as ckg
from backend.core.console import job_runner, sizes
from backend.core.console.job_models import BuildJob, BuildRequest, JobStatus


@pytest.fixture
def caps(monkeypatch):
    monkeypatch.setattr(ckg, "_ROW_LIMIT", 0)
    monkeypatch.setattr(ckg, "_ADAPTER_LIMITS", {})
    return ckg._ADAPTER_LIMITS


def _run(name, kind, rows):
    capped, read = ckg._limit_rows(iter(range(rows)), name, kind)
    return len(list(capped)), ckg._cut_short(name, kind, read)


def test_per_adapter_caps_and_whether_they_cut_short(caps, monkeypatch):
    monkeypatch.setattr(ckg, "_ROW_LIMIT", 10)
    caps["big"] = {"nodes": 5, "edges": 0}
    assert _run("big", "nodes", 100) == (5, True)    # its own cap, reached
    assert _run("big", "edges", 100) == (100, False)  # 0 = no cap for that kind
    assert _run("other", "nodes", 100) == (10, True)  # not listed: --limit
    assert _run("other", "nodes", 3) == (3, False)    # under the cap: real size


def test_adapter_limits_file_is_checked(tmp_path):
    path = tmp_path / "caps.json"
    path.write_text(json.dumps({"a": {"nodes": 5, "edges": -2}}))
    assert ckg._read_adapter_limits(path) == {"a": {"nodes": 5, "edges": 0}}
    path.write_text(json.dumps({"a": 5}))
    with pytest.raises(Exception):
        ckg._read_adapter_limits(path)


def _build(tmp_path, counts, job_id="j1"):
    out = tmp_path / job_id
    out.mkdir()
    (out / "graph_info.json").write_text(json.dumps({"adapter_counts": counts}))
    return BuildJob(id=job_id, status=JobStatus.SUCCEEDED, output_dir=str(out), cmd=[], cwd=".", log_path="",
                    params={"species": "hsa", "dataset": "full"}, finished_at="2026-11-05")


def test_only_complete_counts_become_known_sizes(tmp_path, monkeypatch):
    monkeypatch.setattr(job_runner.settings, "BUILDS_DIR", str(tmp_path / "builds"))
    full = {"gene": {"nodes": 1000, "edges": None, "complete": True},
            "ppi": {"nodes": None, "edges": 400, "complete": True}}
    assert sizes.record_build(_build(tmp_path, full, "j1")) == 2
    # A later capped build teaches nothing new and overwrites nothing.
    capped = {"gene": {"nodes": 10, "edges": None, "complete": False}}
    assert sizes.record_build(_build(tmp_path, capped, "j2")) == 0
    known = sizes.known("hsa", "full")
    assert known["gene"]["nodes"] == 1000 and known["gene"]["job_id"] == "j1"


def test_a_percentage_becomes_caps_from_known_sizes(tmp_path, monkeypatch):
    monkeypatch.setattr(job_runner.settings, "BUILDS_DIR", str(tmp_path / "builds"))
    sizes.record_build(_build(tmp_path, {
        "gene": {"nodes": 1000, "edges": None, "complete": True},
        "ppi": {"nodes": None, "edges": 15, "complete": True}}))
    req = BuildRequest(species="hsa", dataset="full", include_adapters=["gene", "ppi"], percent=10)
    assert sizes.resolve(req) == {"gene": {"nodes": 100, "edges": 0}, "ppi": {"nodes": 0, "edges": 2}}
    # The caps reach the build command through a file.
    req.adapter_limits = sizes.resolve(req)
    argv = job_runner.build_argv(req, "/out", limits_file="/job/adapter_limits.json")
    assert argv[argv.index("--adapter-limits") + 1] == "/job/adapter_limits.json"
    # An adapter with no known size: the percentage can't be honoured.
    with pytest.raises(sizes.SizeError, match="tfbs"):
        sizes.resolve(BuildRequest(species="hsa", dataset="full",
                                   include_adapters=["gene", "tfbs"], percent=10))
    with pytest.raises(sizes.SizeError, match="not both"):
        sizes.resolve(BuildRequest(species="hsa", dataset="full", include_adapters=["gene"],
                                   percent=10, limit=100))
