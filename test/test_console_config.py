"""Tests for the Console config-introspection layer."""
import pytest

from backend.core.console import config_introspect as ci


def test_list_species_and_datasets():
    species = ci.list_species_and_datasets()
    names = {s["species"] for s in species}
    assert {"hsa", "dmel", "cel", "mmu", "rno"}.issubset(names)

    by_name = {s["species"]: s for s in species}
    rno_datasets = {d["name"] for d in by_name["rno"]["datasets"]}
    assert rno_datasets == {"sample", "full"}  # rno now ships a sample dataset too

    hsa_sample = next(d for d in by_name["hsa"]["datasets"] if d["name"] == "sample")
    assert hsa_sample["adapters_config_exists"] is True
    assert hsa_sample["schema_config_exists"] is True


def test_list_adapters_hsa_sample():
    result = ci.list_adapters("hsa", "sample")
    assert result["count"] > 0
    adapters = {a["name"]: a for a in result["adapters"]}

    gencode = adapters["gencode_gene"]
    assert gencode["module"] == "biocypher_metta.adapters.gencode_gene_adapter"
    assert gencode["cls"] == "GencodeGeneAdapter"
    assert gencode["nodes"] is True
    assert gencode["edges"] is False
    # The sample config gives input_dir once and file paths relative to it, so the
    # file arg isn't an explicitly rooted path (declared_paths holds only those).
    assert result["input_dir"] == "samples/hsa"
    assert gencode["args"]["filepath"] == "ensembl/gencode.gtf.gz"
    assert gencode["declared_paths"] == []

    # an edge adapter is correctly flagged
    assert adapters["gencode_transcribes_to"]["edges"] is True
    assert adapters["gencode_transcribes_to"]["nodes"] is False


def test_include_resolves():
    # hsa_adapters_config_sample.yaml is parsed via load_yaml_with_includes; if
    # !include failed to resolve we'd get zero adapters or an exception.
    result = ci.list_adapters("hsa", "sample")
    assert result["adapters_config"].endswith("hsa_adapters_config_sample.yaml")
    assert result["count"] >= 10


def test_list_schema_splits_nodes_and_edges():
    schema = ci.list_schema("hsa", "sample")
    assert len(schema["node_types"]) > 0
    assert len(schema["edge_types"]) > 0
    assert schema["schema_config"].endswith("hsa_schema_config.yaml")


def test_writers_and_flags():
    assert ci.list_writers() == ["metta", "prolog", "neo4j", "parquet", "networkx", "kgx"]
    flag_names = {f["name"] for f in ci.list_flags()}
    assert "write_properties" in flag_names
    assert "generate_data_source_schemas" in flag_names


def test_unknown_species_raises():
    with pytest.raises(ci.ConfigError):
        ci.list_adapters("nope", "sample")


def test_read_and_save_config_roundtrip(tmp_path, monkeypatch):
    """read_config_text returns raw text; save validates via the YAML loader and writes
    atomically. Uses a temp config file so the repo's configs are untouched."""
    cfg = tmp_path / "hsa" / "adapters.yaml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("gencode_gene:\n  nodes: true\n")
    monkeypatch.setattr(ci, "_config_path", lambda s, d, k: cfg)

    assert "gencode_gene" in ci.read_config_text("hsa", "sample", "adapters")["content"]

    # Invalid YAML is rejected and the file is left untouched.
    with pytest.raises(ci.ConfigError):
        ci.save_config_text("hsa", "sample", "adapters", "key: [unbalanced")
    assert cfg.read_text() == "gencode_gene:\n  nodes: true\n"

    # Valid save writes the new content atomically.
    ci.save_config_text("hsa", "sample", "adapters", "gencode_gene:\n  nodes: false\n")
    assert "nodes: false" in cfg.read_text()


def test_group_adapters_by_source_hsa_full():
    """Sources carry what the wizard needs: which need the dbSNP cache, which can't be
    downloaded, and overrides/versioned dirs attribute adapters to the right source."""
    groups = {g["source"]: g for g in ci.group_adapters_by_source("hsa", "full")["groups"]}
    assert groups["abc"]["needs_dbsnp"] is True
    assert groups["reactome"]["needs_dbsnp"] is False
    # adapter_source_map.yaml pins the Roadmap adapters (forgedb/roadmap/...) here.
    roadmap = groups["roadmap_dhs"]
    assert {a["name"] for a in roadmap["adapters"]} == {
        "roadmap_dhs", "roadmap_chromatin_state", "roadmap_h3_mark",
    }
    assert roadmap["downloadable"] is False  # local_files only, nothing to fetch
    assert groups["_shared"]["needs_dbsnp"] is False
    assert groups["catlas"]["adapter_count"] > 0  # catlasv1/ resolves to catlas


def test_localhost_addresses_reach_this_computer_from_docker(monkeypatch):
    """People type localhost for a database on their computer. In Docker, localhost
    is the console itself, so connections go to host.docker.internal instead;
    nothing else changes."""
    from pathlib import Path
    from backend.core.console import connections
    from backend.core.console import explore_targets as et

    real_exists = Path.exists
    monkeypatch.setattr(Path, "exists", lambda self: True if str(self) == "/.dockerenv" else real_exists(self))
    assert et.connect_address("bolt://localhost:7687") == "bolt://host.docker.internal:7687"
    assert et.connect_address("http://127.0.0.1:8027") == "http://host.docker.internal:8027"
    assert et.connect_address("bolt://neo4j-hsa:7687") == "bolt://neo4j-hsa:7687"
    assert et.connect_address(None) is None
    # Typed without a scheme: Neo4j gets bolt://, MORK http://.
    assert connections.normalize_address("neo4j", " localhost:7687 ") == "bolt://localhost:7687"
    assert connections.normalize_address("mork", "myserver:8027/") == "http://myserver:8027"
    assert connections.normalize_address("neo4j", "neo4j+s://x.databases.neo4j.io") == \
        "neo4j+s://x.databases.neo4j.io"
