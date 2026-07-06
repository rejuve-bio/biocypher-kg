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
    """Full dataset input files aren't present locally; expect missing paths."""
    r = client.post("/api/console/builds/validate", json={
        "species": "hsa", "dataset": "full", "include_adapters": ["gencode_gene"],
    })
    body = r.json()
    assert body["valid"] is False
    assert "gencode_gene" in body["missing_paths"]
