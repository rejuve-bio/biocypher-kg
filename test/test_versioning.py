"""Unit tests for the dataset version-resolution framework.

Covers the three resolver strategies and the graceful-failure contract (getters
never raise; errors land on ``VersionInfo.error``).
"""

import json

import pytest

from biocypher_dataset_downloader.versioning import resolve_source, iter_source_urls
from biocypher_dataset_downloader.versioning.registry import build_getter
from biocypher_dataset_downloader.versioning.strategies import (
    StaticGetter,
    UrlRegexGetter,
    HttpHeadGetter,
)


def test_url_regex_extracts_gencode_version():
    cfg = {
        "name": "GENCODE v49",
        "url": "https://ftp.ebi.ac.uk/.../gencode.v49.annotation.gtf.gz",
        "version": {"strategy": "url_regex", "pattern": r"gencode\.(v\d+)\.", "vtype": "sequential"},
    }
    vi = resolve_source("gencode", cfg)
    assert vi.version == "v49"
    assert vi.vtype == "sequential"
    assert vi.strategy == "url_regex"
    assert vi.error is None


def test_url_regex_extracts_flybase_release_from_filename():
    # Reproduces dmel's extract_date_string() behaviour declaratively.
    cfg = {
        "url": ["https://s3ftp.flybase.org/releases/current/precomputed_files/alleles/fbal_to_fbgn_fb_2026_01.tsv.gz"],
        "version": {"strategy": "url_regex", "pattern": r"fb_(\d{4}_\d{2})"},
    }
    vi = resolve_source("flybase", cfg)
    assert vi.version == "2026_01"


def test_url_regex_no_match_captures_error_without_raising():
    cfg = {"url": "https://x/y.txt", "version": {"strategy": "url_regex", "pattern": r"(v\d+)"}}
    vi = resolve_source("bad", cfg)
    assert vi.version is None
    assert vi.error is not None


def test_static_getter_returns_declared_value():
    cfg = {"url": "https://x/y", "version": {"strategy": "static", "value": "v11"}}
    vi = resolve_source("hocomoco", cfg)
    assert vi.version == "v11"
    assert vi.vtype == "static"


def test_static_without_value_errors_gracefully():
    cfg = {"url": "https://x/y", "version": {"strategy": "static"}}
    vi = resolve_source("x", cfg)
    assert vi.version is None
    assert vi.error is not None


def test_iter_source_urls_handles_str_list_and_dict_shapes():
    assert list(iter_source_urls({"url": "https://a/x.gz  # comment"})) == ["https://a/x.gz"]
    assert list(iter_source_urls({"url": ["https://a/1", "https://a/2"]})) == ["https://a/1", "https://a/2"]
    dict_cfg = {"url": {"f1.gz": "https://a/f1.gz", "f2.tsv": "https://a/f2.tsv"}}
    assert sorted(iter_source_urls(dict_cfg)) == ["https://a/f1.gz", "https://a/f2.tsv"]


def test_iter_source_urls_includes_special_keys():
    cfg = {
        "bed_url": "https://a/bed.tar.gz",
        "zip_extract": [{"url": "https://a/z.zip", "files": ["x"]}],
        "directories": {"d": "https://a/dir/"},
    }
    urls = set(iter_source_urls(cfg))
    assert {"https://a/bed.tar.gz", "https://a/z.zip", "https://a/dir/"} <= urls


def test_default_strategy_is_http_head():
    g = build_getter("x", {"url": "https://a/x"})
    assert g.strategy == "http_head"
    assert isinstance(g, HttpHeadGetter)


def test_unknown_strategy_falls_back_to_default():
    g = build_getter("x", {"version": {"strategy": "does-not-exist"}})
    assert isinstance(g, HttpHeadGetter)


def test_http_head_signature_from_mocked_session():
    class _Resp:
        headers = {"ETag": '"abc123"', "Last-Modified": "Wed, 01 Jan 2026 00:00:00 GMT"}

        def raise_for_status(self):
            pass

    class _Session:
        def head(self, url, timeout=None, allow_redirects=None):
            return _Resp()

    cfg = {"url": "https://a/current/x.gz", "version": {"strategy": "http_head"}}
    vi = build_getter("bgee", cfg, session=_Session()).get()
    assert vi.signature == '"abc123"'
    assert vi.version is None
    assert vi.error is None


# ---------------------------------------------------------------------------
# Stage 2: DownloadManager provenance manifest
# ---------------------------------------------------------------------------

import yaml

from biocypher_dataset_downloader.download_manager import DownloadManager


def _manager(tmp_path, sources, **kwargs):
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(sources))
    out = tmp_path / "out"
    out.mkdir()
    mgr = DownloadManager(str(cfg_path), out, **kwargs)
    mgr._head_metadata = lambda url: None  # no network in tests
    return mgr, out


def _gencode_cfg():
    return {
        "name": "GENCODE v49",
        "url": "https://ftp.ebi.ac.uk/x/gencode.v49.annotation.gtf.gz",
        "version": {"strategy": "url_regex", "pattern": r"gencode\.(v\d+)\.", "vtype": "sequential"},
        "source_url": "https://www.gencodegenes.org/",
        "citation": "Frankish 2021",
    }


def test_manifest_records_version_and_checksum(tmp_path):
    cfg = {"gencode": _gencode_cfg()}
    mgr, out = _manager(tmp_path, cfg)
    (out / "gencode").mkdir()
    (out / "gencode" / "gencode.v49.annotation.gtf").write_text("chr1\tx\n" * 100)

    mgr._record_source_manifest("gencode", cfg["gencode"])
    mgr._write_manifest()

    manifest = json.loads((out / "download_manifest.json").read_text())
    g = manifest["sources"]["gencode"]
    assert g["version"] == "v49"
    assert g["vtype"] == "sequential"
    assert g["source_url"] == "https://www.gencodegenes.org/"
    assert g["citation"] == "Frankish 2021"
    assert g["files"][0]["rel_path"] == "gencode/gencode.v49.annotation.gtf"
    assert g["files"][0]["sha256"]

    versions = json.loads((out / "versions.json").read_text())
    assert len(versions["gencode"]) == 1
    assert versions["gencode"][0]["version"] == "v49"


def test_manifest_carries_forward_unchanged_hashes(tmp_path):
    cfg = {"gencode": _gencode_cfg()}
    mgr, out = _manager(tmp_path, cfg)
    (out / "gencode").mkdir()
    (out / "gencode" / "gencode.v49.annotation.gtf").write_text("chr1\tx\n" * 100)
    mgr._record_source_manifest("gencode", cfg["gencode"])
    mgr._write_manifest()
    first_hash = json.loads((out / "download_manifest.json").read_text())["sources"]["gencode"]["files"][0]["sha256"]

    # Second run: same file -> must reuse the recorded hash, never re-hash.
    mgr2 = DownloadManager(str(tmp_path / "cfg.yaml"), out)
    mgr2._head_metadata = lambda url: None

    def _boom(_p):
        raise AssertionError("should not re-hash an unchanged file")

    mgr2._sha256 = _boom
    mgr2._record_source_manifest("gencode", cfg["gencode"])
    mgr2._write_manifest()

    assert json.loads((out / "download_manifest.json").read_text())["sources"]["gencode"]["files"][0]["sha256"] == first_hash
    # Unchanged version -> history not duplicated.
    assert len(json.loads((out / "versions.json").read_text())["gencode"]) == 1


def test_no_checksum_skips_sha256(tmp_path):
    cfg = {"gencode": _gencode_cfg()}
    mgr, out = _manager(tmp_path, cfg, compute_checksums=False)
    (out / "gencode").mkdir()
    (out / "gencode" / "gencode.v49.annotation.gtf").write_text("x\n")
    mgr._record_source_manifest("gencode", cfg["gencode"])
    assert mgr.manifest_sources["gencode"]["files"][0]["sha256"] is None


def test_sample_subtree_excluded_from_manifest(tmp_path):
    cfg = {"gencode": _gencode_cfg()}
    mgr, out = _manager(tmp_path, cfg)
    (out / "gencode").mkdir()
    (out / "gencode" / "gencode.v49.annotation.gtf").write_text("x\n")
    (out / "sample" / "gencode").mkdir(parents=True)
    (out / "sample" / "gencode" / "gencode.v49.annotation.gtf").write_text("x\n")

    mgr._record_source_manifest("gencode", cfg["gencode"])
    rels = [f["rel_path"] for f in mgr.manifest_sources["gencode"]["files"]]
    assert rels == ["gencode/gencode.v49.annotation.gtf"]


# ---------------------------------------------------------------------------
# Stage 5: staleness checker
# ---------------------------------------------------------------------------

from biocypher_dataset_downloader.versioning.cli import evaluate_source, check_versions_for_config


def test_evaluate_pinned_source_matches_recorded_is_up_to_date():
    cfg = {"url": "https://x/gencode.v49.x.gz", "version": {"strategy": "url_regex", "pattern": r"(v\d+)"}}
    row = evaluate_source("gencode", cfg, {"version": "v49"})
    assert row["current"] == "v49"
    assert row["status"] == "up-to-date"


def test_evaluate_pinned_source_version_mismatch_is_drift():
    # Config URL now says v50 but the recorded download was v49 -> config drifted.
    cfg = {"url": "https://x/gencode.v50.x.gz", "version": {"strategy": "url_regex", "pattern": r"(v\d+)"}}
    row = evaluate_source("gencode", cfg, {"version": "v49"})
    assert row["current"] == "v50"
    assert row["status"] == "drift"


def test_evaluate_no_baseline_is_unknown():
    cfg = {"url": "https://x/gencode.v49.x.gz", "version": {"strategy": "url_regex", "pattern": r"(v\d+)"}}
    row = evaluate_source("gencode", cfg, {})
    assert row["status"] == "unknown"


def test_evaluate_resolution_error_is_error():
    cfg = {"url": "https://x/y.txt", "version": {"strategy": "url_regex", "pattern": r"(v\d+)"}}
    row = evaluate_source("bad", cfg, {"version": "v1"})
    assert row["status"] == "error"


def test_evaluate_http_head_signature_change_is_changed():
    class _Resp:
        headers = {"ETag": '"new"'}

        def raise_for_status(self):
            pass

    class _Session:
        def head(self, url, timeout=None, allow_redirects=None):
            return _Resp()

    cfg = {"url": "https://x/current/y.gz", "version": {"strategy": "http_head"}}
    row = evaluate_source("bgee", cfg, {"signature": '"old"'}, session=_Session())
    assert row["status"] == "CHANGED"
    row2 = evaluate_source("bgee", cfg, {"signature": '"new"'}, session=_Session())
    assert row2["status"] == "up-to-date"


def test_check_versions_for_config_reads_history(tmp_path):
    cfg = {
        "gencode": {"url": "https://x/gencode.v49.gz", "version": {"strategy": "url_regex", "pattern": r"(v\d+)"}},
        "hocomoco": {"url": "https://x/h.txt", "version": {"strategy": "static", "value": "v11"}},
        "name": "ignored-scalar",
    }
    cfg_path = tmp_path / "hsa_data_source_config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    versions = {"gencode": [{"version": "v49"}], "hocomoco": [{"version": "v10"}]}
    vpath = tmp_path / "versions.json"
    vpath.write_text(json.dumps(versions))

    rows = {r["source"]: r for r in check_versions_for_config(cfg_path, vpath)}
    assert "name" not in rows  # scalar config keys skipped
    assert rows["gencode"]["status"] == "up-to-date"
    assert rows["hocomoco"]["status"] == "drift"  # config says v11, recorded v10
