"""Tests for the Console build-validation layer."""
import shutil

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app
from backend.core.config import settings

client = TestClient(app)

_HAS_UV = shutil.which(settings.UV_BIN) is not None
requires_uv = pytest.mark.skipif(not _HAS_UV, reason="uv not on PATH; skips --check-only subprocess")


def test_validate_missing_mode_is_error():
    r = client.post("/api/console/builds/validate", json={})
    body = r.json()
    assert body["valid"] is False
    assert any("Provide either" in e for e in body["static_errors"])


def test_validate_unknown_writer():
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "sample", "writer_type": "bogus",
    })
    body = r.json()
    assert body["valid"] is False
    assert any("writer_type" in e for e in body["static_errors"])


def test_validate_unknown_adapter():
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "sample", "include_adapters": ["not_an_adapter"],
    })
    body = r.json()
    assert body["valid"] is False
    assert any("Unknown adapters" in e for e in body["static_errors"])


def test_validate_full_requires_dbsnp():
    """Non-sample run without dbSNP cache/variant must be flagged invalid."""
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "full", "include_adapters": ["gencode_gene"],
    })
    body = r.json()
    assert body["valid"] is False
    joined = " ".join(body["static_errors"])
    assert "dbSNP cache root is required" in joined
    assert "dbSNP variant is required" in joined


def test_validate_full_with_dbsnp_clears_dbsnp_errors():
    """Providing cache root + variant removes the dbSNP errors (paths may still fail)."""
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "full", "include_adapters": ["gencode_gene"],
        "dbsnp_cache_root": "/tmp/dbsnp", "dbsnp_variant": "common",
    })
    joined = " ".join(r.json()["static_errors"])
    assert "dbSNP" not in joined


def test_validate_bad_dbsnp_variant():
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "full", "include_adapters": ["gencode_gene"],
        "dbsnp_cache_root": "/tmp/dbsnp", "dbsnp_variant": "rare",
    })
    assert any("must be 'common' or 'full'" in e for e in r.json()["static_errors"])


def test_validate_all_species_skips_config_introspection():
    """species='all' must not try to resolve a single config; it validates lightly."""
    r = client.post("/api/console/builds/validate", json={
        "species": "all", "dataset": "sample",
    })
    body = r.json()
    # no "unknown species"/config errors; a note about per-species validation
    assert not any("Unknown species" in e for e in body["static_errors"])
    assert any("All-species run" in w for w in body["static_warnings"])
    preview = body["resolved"]["cmd_preview"]
    assert "--species" in preview and "all" in preview


def test_validate_cmd_preview_present():
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "sample", "include_adapters": ["gencode_gene"],
    })
    preview = r.json()["resolved"]["cmd_preview"]
    assert "create_knowledge_graph.py" in preview
    assert "--species" in preview and "hsa" in preview


@requires_uv
def test_validate_sample_passes():
    """Sample data is committed, so --check-only should pass."""
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "sample", "include_adapters": ["gencode_gene"],
    })
    body = r.json()
    assert body["checked_paths"] is True
    assert body["missing_paths"] == {}
    assert body["valid"] is True


@requires_uv
def test_validate_full_reports_missing_paths():
    """Full dataset input files aren't present locally; expect missing paths.

    dbSNP cache/variant are supplied so validation gets past the dbSNP requirement
    and actually runs the --check-only path check.
    """
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "full", "include_adapters": ["gencode_gene"],
        "dbsnp_cache_root": "/tmp/dbsnp", "dbsnp_variant": "common",
    })
    body = r.json()
    assert body["valid"] is False
    assert "gencode_gene" in body["missing_paths"]
