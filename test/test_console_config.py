"""Tests for the Console config-introspection layer."""
import pytest

from backend.core.console import config_introspect as ci


def test_list_species_and_datasets():
    species = ci.list_species_and_datasets()
    names = {s["species"] for s in species}
    assert {"hsa", "dmel", "cel", "mmu", "rno"}.issubset(names)

    by_name = {s["species"]: s for s in species}
    rno_datasets = {d["name"] for d in by_name["rno"]["datasets"]}
    assert rno_datasets == {"full"}  # rno ships only a full dataset

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
    # declared_paths picks up the ./samples path arg
    assert any("samples" in p for p in gencode["declared_paths"])

    # an edge adapter is correctly flagged
    assert adapters["transcribes_to"]["edges"] is True
    assert adapters["transcribes_to"]["nodes"] is False


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
