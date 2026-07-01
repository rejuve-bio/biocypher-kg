"""Unit tests for build-time dataset provenance (biocypher_metta/provenance.py)."""

import json

from biocypher_metta.provenance import ProvenanceLookup, source_id_for_adapter


def test_source_id_from_outdir_first_segment():
    assert source_id_for_adapter({"outdir": "gencode/gene"}) == "gencode"
    assert source_id_for_adapter({"outdir": "reactome"}) == "reactome"
    assert source_id_for_adapter({"outdir": "/gtex/eqtl/"}) == "gtex"


def test_explicit_source_id_overrides_outdir():
    assert source_id_for_adapter({"source_id": "string", "outdir": "x/y"}) == "string"


def test_source_id_none_when_unset():
    assert source_id_for_adapter({}) is None
    assert source_id_for_adapter({"outdir": ""}) is None


def _write_manifest(tmp_path):
    manifest = {
        "sources": {
            "gencode": {
                "version": "v49",
                "source_url": "https://www.gencodegenes.org/",
                "date": "2026-01-01",
                "citation": "Frankish 2021",
                "license": "EBI",
                "files": [{"rel_path": "gencode/x.gtf", "sha256": "abc123"}],
            }
        }
    }
    path = tmp_path / "download_manifest.json"
    path.write_text(json.dumps(manifest))
    return path


def test_for_source_returns_full_provenance(tmp_path):
    lookup = ProvenanceLookup(_write_manifest(tmp_path))
    prov = lookup.for_source("gencode")
    assert prov["version"] == "v49"
    assert prov["source_url"] == "https://www.gencodegenes.org/"
    assert prov["citation"] == "Frankish 2021"
    assert prov["checksums"] == {"gencode/x.gtf": "abc123"}


def test_for_source_unknown_returns_none(tmp_path):
    lookup = ProvenanceLookup(_write_manifest(tmp_path))
    assert lookup.for_source("not-a-source") is None
    assert lookup.for_source(None) is None


def test_attach_matches_by_source_id(tmp_path):
    lookup = ProvenanceLookup(_write_manifest(tmp_path))
    adapters = {
        "gencode_gene": {"outdir": "gencode/gene"},
        "gencode_exon": {"outdir": "gencode/exon"},
        "unknown_src": {"outdir": "nope"},
    }
    matched = lookup.attach(adapters)
    assert matched == 2  # both gencode adapters resolve to the same source
    assert adapters["gencode_gene"]["provenance"]["version"] == "v49"
    assert adapters["unknown_src"]["provenance"] is None


def test_missing_manifest_is_falsy_and_safe(tmp_path):
    lookup = ProvenanceLookup(tmp_path / "does-not-exist.json")
    assert not lookup
    assert lookup.for_source("gencode") is None
    # attach must not raise even with no sources
    assert lookup.attach({"a": {"outdir": "x"}}) == 0


def test_invalid_manifest_json_is_falsy(tmp_path):
    bad = tmp_path / "download_manifest.json"
    bad.write_text("{not valid json")
    lookup = ProvenanceLookup(bad)
    assert not lookup
