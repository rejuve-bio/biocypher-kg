"""The Builds folder (Settings): where builds are saved, what clean-up may delete
there, and how the databases the console runs follow it. No Docker needed."""
import re

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app
from backend.core.config import settings
from backend.core.console import instances, job_runner, runtime_config
from backend.core.console.job_models import BuildRequest

local = TestClient(app, base_url="http://localhost:9000")
URL = "/api/console/settings"


@pytest.fixture
def folders(monkeypatch, tmp_path):
    """A private settings store with tmp/data as the console data folder."""
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setattr(runtime_config, "store_path", lambda: tmp_path / "console_settings.json")
    monkeypatch.setitem(runtime_config._DEFAULTS, "DATA_ROOT", str(data))
    monkeypatch.setitem(runtime_config._DEFAULTS, "OUTPUT_ROOT", "")
    monkeypatch.setattr(settings, "DATA_ROOT", str(data))
    monkeypatch.setattr(settings, "OUTPUT_ROOT", "")
    return tmp_path


def _build(folder, name, writer="metta"):
    d = folder / writer / name
    d.mkdir(parents=True)
    (d / "graph_info.json").write_text("{}")
    return d


def test_builds_go_to_the_builds_folder_and_nowhere_without_one(folders):
    builds = folders / "builds"
    builds.mkdir()
    r = local.put(URL, json={"build": {"output_root": str(builds)}})
    assert r.status_code == 200, r.text
    assert r.json()["build"]["output_root"] == str(builds)
    out = job_runner.resolve_output_dir(BuildRequest(species="hsa", dataset="sample"), folders / "job")
    assert re.fullmatch(rf"{builds.resolve()}/metta/hsa-sample-\d{{8}}-\d{{6}}", out), out

    # Cleared: no fallback (not the data folder, not the console's own volume).
    r = local.put(URL, json={"build": {"output_root": None}})
    assert r.json()["build"]["output_root"] is None
    assert r.json()["build"]["missing"] == ["Builds folder"]
    with pytest.raises(runtime_config.SetupError, match="Builds folder"):
        job_runner.resolve_output_dir(BuildRequest(species="hsa", dataset="sample"), folders / "job")


def test_nothing_runs_until_its_folders_are_set(folders, monkeypatch):
    """Builds, downloads, the dbSNP cache and loads each wait for their folders."""
    from backend.core.console.job_models import DownloadRequest
    from backend.core.console.validation import validate_build

    monkeypatch.setattr(settings, "DATA_ROOT", "")
    req = BuildRequest(species="hsa", dataset="sample")
    errors = validate_build(req, run_check_only=False)["static_errors"]
    assert "set in Settings first: console data folder, Builds folder" in errors
    with pytest.raises(runtime_config.SetupError):
        job_runner.launch(req)
    with pytest.raises(runtime_config.SetupError, match="console data folder"):
        job_runner.launch_download(DownloadRequest(species="hsa"))
    with pytest.raises(runtime_config.SetupError, match="console data folder"):
        job_runner.launch_dbsnp_prepare("common")
    with pytest.raises(runtime_config.SetupError, match="console data folder"):
        job_runner.build_load_argv("mork", "/out", "hsa")


def test_a_new_folder_typed_in_settings_is_made(folders):
    """On a server there's no Finder: typing a new path for either folder makes it."""
    new = folders / "kg" / "builds"
    r = local.put(URL, json={"build": {"output_root": str(new), "data_root": str(folders / "kg" / "data")}})
    assert r.status_code == 200, r.text
    assert new.is_dir() and (folders / "kg" / "data").is_dir()


def test_the_builds_folder_must_be_made_for_builds(folders):
    mine = folders / "documents"
    mine.mkdir()
    (mine / "thesis.docx").write_text("x")
    r = local.put(URL, json={"build": {"output_root": str(mine)}})
    assert r.status_code == 400 and "already holds other things" in r.json()["detail"]
    r = local.put(URL, json={"build": {"output_root": "/Users"}})
    assert r.status_code == 400 and "can't be the Builds folder" in r.json()["detail"]
    _build(mine, "hsa-sample-20260101-000000")  # a folder that already has builds is fine
    assert local.put(URL, json={"build": {"output_root": str(mine)}}).status_code == 200


def test_clean_up_only_ever_removes_console_builds(folders, monkeypatch):
    """The Builds folder can be anywhere, so a dated folder that isn't a console
    build (no build files, or not in a writer folder) is never deleted."""
    builds = folders / "builds"
    monkeypatch.setattr(settings, "OUTPUT_ROOT", str(builds))
    monkeypatch.setattr(settings, "MAX_OUTPUT_BUILDS", 1)
    old, new = (_build(builds, f"hsa-sample-2026010{i}-000000") for i in (1, 2))
    notes = builds / "metta" / "hsa-notes-20250101-000000"  # yours: no build files
    notes.mkdir()
    loose = builds / "trip-photos-20250101-000000"  # yours: not in a writer folder
    loose.mkdir()
    (loose / "graph_info.json").write_text("{}")

    assert job_runner.prune_output_dirs() == 1
    assert not old.exists() and new.exists() and notes.exists() and loose.exists()
    assert job_runner.purge_output_dir(str(notes)) is False and notes.exists()
    assert job_runner.purge_output_dir(str(new)) is True and not new.exists()


# ── The databases the console runs read builds from these folders ───────────────

CTX = {"project": "biocypher-console", "network": "net"}
LOGIN = {"user": "neo4j", "password": "s3cretpass"}


def _binds(spec):
    return sorted(b for b in spec["HostConfig"]["Binds"] if b.startswith("/"))


def test_databases_read_the_data_and_builds_folders_with_set_memory(monkeypatch):
    monkeypatch.setattr(instances.et, "species_list", lambda: ["hsa"])
    monkeypatch.setattr(settings, "DATA_ROOT", "/data/kg")
    monkeypatch.setattr(settings, "OUTPUT_ROOT", "/Volumes/Big/builds")
    monkeypatch.setattr(settings, "NEO4J_MEMORY", "4G")
    neo4j = instances.container_spec("hsa", "neo4j", CTX, "neo4j:5.21", LOGIN)
    assert _binds(neo4j) == ["/Volumes/Big/builds:/Volumes/Big/builds:ro", "/data/kg:/data/kg:ro"]
    assert "NEO4J_server_memory_heap_max__size=4G" in neo4j["Env"]
    mork = instances.container_spec("hsa", "mork", CTX, "biocypher-console-mork")
    assert _binds(mork) == _binds(neo4j)
    # A Builds folder inside the data folder is already shared with it.
    monkeypatch.setattr(settings, "OUTPUT_ROOT", "/data/kg/builds")
    assert _binds(instances.container_spec("hsa", "mork", CTX, "m")) == ["/data/kg:/data/kg:ro"]


def test_a_database_made_with_other_folders_or_memory_is_out_of_date(monkeypatch):
    monkeypatch.setattr(instances.et, "species_list", lambda: ["hsa"])
    monkeypatch.setattr(settings, "DATA_ROOT", "/data/kg")
    monkeypatch.setattr(settings, "OUTPUT_ROOT", "")
    spec = instances.container_spec("hsa", "neo4j", CTX, "neo4j:5.21", LOGIN)
    made = {"HostConfig": {"Binds": spec["HostConfig"]["Binds"]}, "Config": {"Env": spec["Env"]}}
    assert instances.out_of_date(made) is False
    # Docker Desktop reports a shared folder's source as /host_mnt/...: still up to date.
    desktop = {"HostConfig": {"Binds": ["v:/data", "/host_mnt/data/kg:/data/kg:ro"]},
               "Config": {"Env": spec["Env"]}}
    assert instances.out_of_date(desktop) is False
    monkeypatch.setattr(settings, "OUTPUT_ROOT", "/Volumes/Big/builds")
    assert instances.out_of_date(made) is True
    monkeypatch.setattr(settings, "OUTPUT_ROOT", "")
    monkeypatch.setattr(settings, "NEO4J_MEMORY", "2G")
    assert instances.out_of_date(made) is True


def test_loading_waits_for_a_database_to_see_the_builds_folder(monkeypatch):
    refreshed = []
    monkeypatch.setattr(instances, "docker_available", lambda: True)
    monkeypatch.setattr(instances, "_containers", lambda: [
        {"Names": ["/neo4j-hsa"], "Labels": {"kg-console.managed": "true"}, "State": "running"}])
    monkeypatch.setattr(instances, "_inspect", lambda name: {
        "HostConfig": {"Binds": ["vol:/data", "/host_mnt/data/kg:/data/kg:ro"]}})
    monkeypatch.setattr(instances, "refresh_in_background", lambda: refreshed.append(True))
    assert instances.cannot_read("hsa", "neo4j", "/data/kg/metta/hsa-sample-1") is None
    problem = instances.cannot_read("hsa", "neo4j", "/Volumes/Big/builds/metta/hsa-sample-1")
    assert "updating to read the Builds folder" in problem and refreshed == [True]
