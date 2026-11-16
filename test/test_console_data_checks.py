"""Folders chosen in the console must be what the code reads (data_checks)."""
import gzip
import json
import pickle
import sqlite3

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app
from backend.core.console import data_checks as dc
from dbsnp_cache_helper import make_dbsnp_cache

local = TestClient(app, base_url="http://localhost:9000")


# ── dbSNP cache ────────────────────────────────────────────────────────────────

def test_a_real_cache_passes(tmp_path):
    make_dbsnp_cache(tmp_path / "common", "common")
    make_dbsnp_cache(tmp_path / "full", "full")
    assert dc.check_dbsnp_root(tmp_path) == (["common", "full"], None)


def _sqlite(path, *sql):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    for s in sql:
        conn.execute(s)
    conn.commit()
    conn.close()


@pytest.mark.parametrize("make,expected", [
    (lambda d: (d / "dbsnp_mapping.db").write_text("just text, renamed"), "wrong kind of file"),
    (lambda d: _sqlite(d / "dbsnp_mapping.db", "CREATE TABLE other (x)"), "no dbSNP table"),
    (lambda d: _sqlite(d / "dbsnp_mapping.db", "CREATE TABLE rsid_to_pos (a, b)"), "wrong layout"),
    (lambda d: make_dbsnp_cache(d, rows=[]), "never finished"),
    (lambda d: make_dbsnp_cache(d, rows=[("gene1", "chr1", 5)]), "doesn't hold dbSNP entries"),
    (lambda d: make_dbsnp_cache(d, index=False), "position index"),
    (lambda d: make_dbsnp_cache(d, "full"), "holds the full cache"),  # under common/
])
def test_fake_or_broken_caches_are_refused(tmp_path, make, expected):
    d = tmp_path / "common"
    d.mkdir()
    make(d)
    ok, problem = dc.check_dbsnp_root(tmp_path)
    assert ok == [] and expected in problem


def test_a_damaged_version_file_is_refused(tmp_path):
    make_dbsnp_cache(tmp_path / "common", "common")
    (tmp_path / "common" / "dbsnp_version.json").write_text(json.dumps({"processor": "gwas"}))
    assert "doesn't describe a dbSNP cache" in dc.check_dbsnp_root(tmp_path)[1]


def test_a_pickle_is_never_accepted(tmp_path):
    """Loading a pickle from someone else's folder can run code."""
    (tmp_path / "common").mkdir()
    (tmp_path / "common" / "dbsnp_mapping.pkl").write_bytes(pickle.dumps({"rs1": ("1", 2)}))
    assert dc.check_dbsnp_root(tmp_path)[0] == []


def test_choosing_the_variant_folder_itself_points_one_level_up(tmp_path):
    make_dbsnp_cache(tmp_path / "common", "common")
    problem = dc.check_dbsnp_root(tmp_path / "common")[1]
    assert "Choose the folder that holds it" in problem and str(tmp_path) in problem


def test_an_unrelated_folder_isnt_a_cache(tmp_path):
    (tmp_path / "photos").mkdir()
    assert "No dbSNP cache here" in dc.check_dbsnp_root(tmp_path)[1]


# ── Species source data ────────────────────────────────────────────────────────

def _gz(path, text=b"chr\tstart\n"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(text))


def test_a_folder_with_hsa_sources_passes(tmp_path):
    _gz(tmp_path / "abc" / "abc.forgedb.csv.gz")
    _gz(tmp_path / "bgee" / "Homo_sapiens_expr_simple_all_conditions.tsv.gz")
    out = dc.check_species_folder("hsa", tmp_path)
    assert out["problem"] is None and len(out["found"]) == 2 and out["total"] > 2


def test_an_unrelated_folder_isnt_species_data(tmp_path):
    (tmp_path / "holiday").mkdir()
    (tmp_path / "holiday" / "beach.jpg").write_bytes(b"\xff\xd8")
    problem = dc.check_species_folder("hsa", tmp_path)["problem"]
    assert "none of the files or folders the hsa adapters read" in problem and "abc/" in problem


def test_choosing_one_sources_folder_points_one_level_up(tmp_path):
    _gz(tmp_path / "abc" / "abc.forgedb.csv.gz")
    problem = dc.check_species_folder("hsa", tmp_path / "abc")["problem"]
    assert "one source's folder" in problem and str(tmp_path) in problem


def test_a_file_that_isnt_what_its_name_says_is_refused(tmp_path):
    (tmp_path / "abc").mkdir()
    (tmp_path / "abc" / "abc.forgedb.csv.gz").write_text("not gzip")
    problem = dc.check_species_folder("hsa", tmp_path)["problem"]
    assert "isn't a real .gz file" in problem


# ── Console data folder ────────────────────────────────────────────────────────

def test_the_data_folder_is_empty_or_a_console_data_folder(tmp_path):
    assert dc.check_data_folder(tmp_path) is None  # empty: the console sets it up
    (tmp_path / "input").mkdir()
    assert dc.check_data_folder(tmp_path) is None  # already a console data folder
    other = tmp_path / "other"
    other.mkdir()
    (other / "thesis.docx").write_text("x")
    assert "already holds other things" in dc.check_data_folder(other)


# ── Saving Settings applies the same checks ────────────────────────────────────

def test_settings_refuse_a_folder_that_isnt_what_the_field_is_for(tmp_path):
    (tmp_path / "photos").mkdir()
    r = local.put("/api/console/settings", json={"data": {"dbsnp_root": str(tmp_path / "photos")}})
    assert r.status_code == 400 and "dbSNP" in r.json()["detail"]
    r = local.put("/api/console/settings", json={"data": {"input_dirs": {"hsa": str(tmp_path / "photos")}}})
    assert r.status_code == 400 and "hsa source data" in r.json()["detail"]
    make_dbsnp_cache(tmp_path / "cache" / "common", "common")
    r = local.put("/api/console/settings", json={"data": {"dbsnp_root": str(tmp_path / "cache")}})
    assert r.status_code == 200
    local.put("/api/console/settings", json={"data": {"dbsnp_root": None}})  # tidy up
