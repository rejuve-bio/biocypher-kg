"""Unit tests for the parallel adapter-execution machinery.

These guard the concurrency-sensitive pieces of the parallel path without running a
full pipeline: dbSNP wrapper picklability + per-process connection reopen, the
checkpoint schema change (failed_adapters + backward-compat), deterministic
provenance/graph-info assembly, and the output-collision key used for coalescing.
"""

import json
import pickle
import sqlite3

import pytest

import create_knowledge_graph as ck
from biocypher_metta.processors.dbsnp_processor import (
    _SQLiteConnCache,
    _SQLitePosToRsidWrapper,
    _SQLiteRsidToPosWrapper,
)
from checkpoint_manager import CheckpointManager, prompt_resume_or_restart


# --------------------------------------------------------------------------- #
# dbSNP wrappers: picklable, and reopen a working read-only connection per process
# --------------------------------------------------------------------------- #

def _make_dbsnp_db(path):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE rsid_to_pos (rsid TEXT PRIMARY KEY, chr TEXT, pos INTEGER)")
    conn.executemany(
        "INSERT INTO rsid_to_pos VALUES (?, ?, ?)",
        [("rs1", "1", 100), ("rs2", "2", 200)],
    )
    conn.commit()
    conn.close()


def test_dbsnp_rsid_wrapper_pickle_roundtrip_and_query(tmp_path):
    db = tmp_path / "dbsnp_mapping.db"
    _make_dbsnp_db(db)
    _SQLiteConnCache._conns = {}  # reset cache for a clean check
    _SQLiteConnCache._pid = None

    w = _SQLiteRsidToPosWrapper(str(db))
    assert w.get("rs1") == {"chr": "1", "pos": 100}

    # Survives pickling (as it must to reach a ProcessPoolExecutor worker) and the
    # unpickled copy resolves its own connection and answers correctly.
    w2 = pickle.loads(pickle.dumps(w))
    assert w2._db_path == str(db)
    assert w2.get("rs2") == {"chr": "2", "pos": 200}
    assert w2.get("missing") is None


def test_dbsnp_pos_wrapper_pickle_roundtrip_and_query(tmp_path):
    db = tmp_path / "dbsnp_mapping.db"
    _make_dbsnp_db(db)
    w = pickle.loads(pickle.dumps(_SQLitePosToRsidWrapper(str(db))))
    assert w.get("1_100") == "rs1"
    assert w.get("2:200") == "rs2"
    assert w.get("9_999") is None


def test_conn_cache_reopens_after_pid_change(tmp_path):
    """The PID guard must drop fork-inherited connections and reopen in the child."""
    db = tmp_path / "dbsnp_mapping.db"
    _make_dbsnp_db(db)
    _SQLiteConnCache._conns = {}
    _SQLiteConnCache._pid = 111  # simulate a different (parent) pid
    conn = _SQLiteConnCache.get(str(db))
    assert conn.execute("SELECT COUNT(*) FROM rsid_to_pos").fetchone()[0] == 2
    assert _SQLiteConnCache._pid != 111  # reset to the current pid


# --------------------------------------------------------------------------- #
# Checkpoint: failed_adapters list + backward-compat with the old singular key
# --------------------------------------------------------------------------- #

def _ckpt(tmp_path):
    return CheckpointManager(tmp_path, pipeline_id="pid::x")


def test_checkpoint_saves_failed_adapters_as_list(tmp_path):
    from collections import Counter, defaultdict

    cm = _ckpt(tmp_path)
    cm.save(
        completed_adapters=["a"],
        nodes_count=Counter({"gene": 1}),
        nodes_props=defaultdict(set),
        edges_count=Counter(),
        datasets_dict={},
        failed_adapters=["b", "c"],
    )
    state = json.loads((tmp_path / cm.checkpoint_path.name).read_text())
    assert state["failed_adapters"] == ["b", "c"]
    assert "failed_adapter" not in state


def test_prompt_reads_new_failed_adapters(tmp_path, monkeypatch):
    cm = _ckpt(tmp_path)
    cm._state = {"completed_adapters": ["a"], "failed_adapters": ["broken"]}
    monkeypatch.setattr("builtins.input", lambda *_: "y")
    assert prompt_resume_or_restart(cm) is True  # runs the display path without error


def test_prompt_falls_back_to_legacy_failed_adapter(tmp_path, monkeypatch):
    cm = _ckpt(tmp_path)
    # Pre-upgrade checkpoint: old singular string key, no failed_adapters.
    cm._state = {"completed_adapters": ["a"], "failed_adapter": "old_broken"}
    monkeypatch.setattr("builtins.input", lambda *_: "y")
    assert prompt_resume_or_restart(cm) is True


# --------------------------------------------------------------------------- #
# Deterministic provenance + graph_info assembly (order-independent)
# --------------------------------------------------------------------------- #

def _result(name, dataset, node_labels=(), edge_keys=()):
    return {
        "adapter_name": name,
        "dataset_name": dataset,
        "version": "v1",
        "source_url": "http://x",
        "prov": {},
        "freq_nodes": {lbl: 1 for lbl in node_labels},
        "freq_edges": {k: 1 for k in edge_keys},
    }


def test_rebuild_datasets_dict_is_order_independent(tmp_path):
    r1 = _result("a_adapter", "dsA", node_labels=["gene"])
    r2 = _result("b_adapter", "dsA", node_labels=["protein"])
    schema = {}
    forward = ck._rebuild_datasets_dict({}, [r1, r2], schema)
    reverse = ck._rebuild_datasets_dict({}, [r2, r1], schema)
    assert forward["dsA"]["nodes"] == reverse["dsA"]["nodes"] == {"gene", "protein"}
    # Metadata is deterministic (smallest adapter name touched first).
    assert forward["dsA"]["version"] == reverse["dsA"]["version"] == "v1"


def test_gather_graph_info_is_deterministic(tmp_path):
    from collections import Counter, defaultdict

    nc1 = Counter({"gene": 2, "protein": 1})
    np1 = defaultdict(set, {"gene": {"z", "a"}, "protein": {"m"}})
    nc2 = Counter({"protein": 1, "gene": 2})  # different insertion order
    np2 = defaultdict(set, {"protein": {"m"}, "gene": {"a", "z"}})

    a = ck.gather_graph_info(nc1, np1, Counter(), {}, tmp_path)
    b = ck.gather_graph_info(nc2, np2, Counter(), {}, tmp_path)
    assert a["top_entities"] == b["top_entities"]
    assert a["schema"]["nodes"] == b["schema"]["nodes"]
    # Properties are sorted, so the gene node's props are stable.
    gene = next(n for n in a["schema"]["nodes"] if n["data"]["id"] == "gene")
    assert gene["data"]["properties"] == ["a", "z"]


# --------------------------------------------------------------------------- #
# Output-collision key used to coalesce adapters onto one worker
# --------------------------------------------------------------------------- #

def _entry(outdir, label=None):
    args = {} if label is None else {"label": label}
    return {"adapter": {"args": args}, "outdir": outdir}


def test_collision_key_same_outdir_and_label_collide():
    k1 = ck._adapter_output_key("uniprot_dbxref_bgee_gene", _entry("uniprot/has_xref_gene", "protein_has_xref_gene"))
    k2 = ck._adapter_output_key("uniprot_dbxref_ensembl_gene", _entry("uniprot/has_xref_gene", "protein_has_xref_gene"))
    assert k1 == k2  # coalesced


def test_collision_key_distinct_labels_do_not_collide():
    k1 = ck._adapter_output_key("reactome_a", _entry("reactome", "pathway"))
    k2 = ck._adapter_output_key("reactome_b", _entry("reactome", "reaction"))
    assert k1 != k2  # stay parallel


def test_collision_key_labelless_adapters_never_collide():
    k1 = ck._adapter_output_key("adapter_a", _entry("shared"))
    k2 = ck._adapter_output_key("adapter_b", _entry("shared"))
    assert k1 != k2
