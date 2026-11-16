"""MORK loader safety: nothing is cleared unless there's something to load, and a
failed reload doesn't leave stale version records behind."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "kg-service"))
import mork_loader  # noqa: E402


def test_files_are_found_by_absolute_path_even_from_a_relative_dir(tmp_path, monkeypatch):
    (tmp_path / "out" / "gencode").mkdir(parents=True)
    (tmp_path / "out" / "gencode" / "nodes.metta").write_text("(gene x)")
    monkeypatch.chdir(tmp_path)
    files = mork_loader.find_metta_files("out")  # relative, like the CLI default
    assert files == [tmp_path.resolve() / "out" / "gencode" / "nodes.metta"]
    assert all(f.is_absolute() for f in files)


def test_a_wrong_or_empty_dir_fails_before_anything_is_cleared(tmp_path):
    with pytest.raises(ValueError, match="not found"):
        mork_loader.find_metta_files(tmp_path / "missing")
    (tmp_path / "empty").mkdir()
    with pytest.raises(ValueError, match="No .metta files"):
        mork_loader.find_metta_files(tmp_path / "empty")


def test_a_failed_reload_forgets_the_old_version_record(tmp_path):
    vm = mork_loader.MORKVersionManager(server=None, data_dir=tmp_path, archive_dir=tmp_path)
    vm.metadata_file.write_text(json.dumps({"atomspace_version": "v1", "dataset_hashes": {}}))
    vm.invalidate_metadata()
    assert not vm.metadata_file.exists()  # next load of the last good build reloads it
    vm.invalidate_metadata()  # nothing there: fine
