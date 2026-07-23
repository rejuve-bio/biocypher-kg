"""Regression test for the Neo4j version-finalize poisoning bug.

`finalize_version` must persist dataset hashes ONLY for datasets that actually
loaded this run — never for failed ones. Storing a failed dataset's hash makes the
next run see it as "unchanged" and skip loading it, so the graph stays empty while
the version claims success. (This is the tadmap failure that silently "succeeded"
on retry.)
"""
from version_manager import VersionManager


def _bare_manager(tmp_path):
    """A VersionManager without a live Neo4j driver (bypasses __init__)."""
    vm = object.__new__(VersionManager)
    vm.driver = None
    vm.archive_dir = tmp_path
    vm.output_dir = None
    vm.db_type = "neo4j"
    return vm


def test_finalize_stores_hashes_only_for_loaded_datasets(tmp_path, monkeypatch):
    vm = _bare_manager(tmp_path)

    # Two datasets exist on disk, but only `gencode` loaded successfully this run.
    monkeypatch.setattr(vm, "hash_all_datasets",
                        lambda: {"gencode": "hashA", "tadmap": "hashB"})
    monkeypatch.setattr(vm, "discover_all_sources", lambda: {})
    monkeypatch.setattr(vm, "store_folder_source_mapping", lambda *a, **k: None)
    monkeypatch.setattr(vm, "create_dataset_version_nodes", lambda *a, **k: None)
    monkeypatch.setattr(vm, "read_source_provenance", lambda *a, **k: {})
    monkeypatch.setattr(vm, "create_version_node", lambda *a, **k: None)

    captured = {}
    monkeypatch.setattr(vm, "store_hashes",
                        lambda dataset_hashes, dataset_versions: captured.update(dataset_hashes))

    vm.finalize_version(
        output_dir=str(tmp_path),
        atomspace_version="v5",
        dataset_versions={"gencode": "v5", "tadmap": "v5"},
        changed_datasets=["gencode"],   # only gencode loaded; tadmap failed → excluded
        build_id="build-test",
    )

    # gencode's hash is persisted; tadmap's is NOT, so tadmap stays "changed" and a
    # retry re-attempts it instead of being skipped as "unchanged".
    assert captured == {"gencode": "hashA"}
    assert "tadmap" not in captured
