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


def _groups(species, dataset):
    return {g["source"]: g for g in ci.group_adapters_by_source(species, dataset)["groups"]}


def _keys(group):
    return sorted(d["key"] for d in group["downloads"])


def test_sources_are_the_ones_the_schema_folder_lists():
    """The wizard's sources are data_source_schemas/<species>/'s: each adapter goes
    under the source its code declares, with the downloads its files come from."""
    from pathlib import Path
    groups = _groups("hsa", "full")
    files = {p.stem for p in (Path(__file__).resolve().parents[1] / "data_source_schemas" / "hsa").glob("*.yaml")}
    with_adapters = {k for k, g in groups.items() if g["adapters"]}
    assert with_adapters <= files and {"GTEx", "CATLAS", "ENCODE", "OBO_Foundry"} <= with_adapters

    assert _keys(groups["ENCODE"]) == ["cCRE", "tfbs"]          # one source, two downloads
    assert groups["OBO_Foundry"]["downloads"] == []              # the build fetches ontologies
    assert groups["OBO_Foundry"]["adapter_count"] > 10
    roadmap = groups["Roadmap_Epigenomics_Project"]
    assert {a["name"] for a in roadmap["adapters"]} == {
        "roadmap_dhs", "roadmap_chromatin_state", "roadmap_h3_mark",
    }
    assert _keys(roadmap) == ["forgedb"] and roadmap["downloadable"] is True
    assert groups["TopLD"]["downloadable"] is False  # its download folder is gone
    assert groups["TopLD"]["local_paths"]            # so it says where to put the files
    assert groups["ABC"]["needs_dbsnp"] is True
    assert groups["REACTOME"]["needs_dbsnp"] is False
    assert _keys(groups["GTEx"]) == ["gtex"] and groups["GTEx"]["adapter_count"] == 2
    # A download no adapter reads is still listed, so it can be fetched.
    assert groups["download:dbvar"]["adapters"] == [] and _keys(groups["download:dbvar"]) == ["dbvar"]


def test_the_list_follows_the_schema_folder(monkeypatch, tmp_path):
    """A build rewrites data_source_schemas/<species>/: the stamp the wizard watches
    changes, and a source only the folder lists shows up (not buildable), with the
    types its file describes."""
    import shutil
    from pathlib import Path
    folder = tmp_path / "hsa"
    shutil.copytree(Path(__file__).resolve().parents[1] / "data_source_schemas" / "hsa", folder)
    monkeypatch.setattr(ci, "_schema_folder", lambda species: folder)

    before = ci.sources_stamp("hsa", "full")
    (folder / "New_Source.yaml").write_text(
        "name: New Source\nnodes:\n  thing:\n    input_label: thing\n"
        "relationships:\n  thing to gene:\n    input_label: thing_gene\n"
        "    output_label: binds\n    source: thing\n    target: gene\n")
    assert ci.sources_stamp("hsa", "full") != before

    groups = _groups("hsa", "full")
    new = groups["New_Source"]
    assert new["name"] == "New Source" and new["in_folder"] and new["adapters"] == []
    edge = next(t for t in new["types"] if t["kind"] == "edge")
    assert (edge["source"], edge["verb"], edge["target"]) == ("thing", "binds", "gene")

    # A source with adapters: each type says what writes it and what it connects.
    gtex = {t["name"]: t for t in groups["GTEx"]["types"]}
    eqtl = gtex["gtex variant to gene expression association"]
    assert (eqtl["source"], eqtl["target"], eqtl["adapters"]) == ("snp", "gene", ["gtex_eqtl"])
    assert "p_value" in eqtl["properties"]

    # Without an output_label the edge is named after its input_label, so gene to
    # disease relations stay told apart.
    alliance = {t["name"]: t["verb"] for t in groups["Alliance_for_Genome_Resources"]["types"]}
    assert alliance["gene to disease marker association"] == "is_marker_for"
    assert alliance["gene biomarker via orthology disease"] == "biomarker_via_orthology"


def test_adapters_are_credited_only_with_the_types_they_write():
    """The GO adapters take ontology: 'go'; that once matched every 'go_*' edge label
    (go gene, go gene product), which no adapter writes. Likewise the CL adapters
    (ontology: 'cl') were credited with each other's edges."""
    obo = {t["name"]: t for t in _groups("hsa", "full")["OBO_Foundry"]["types"]}
    assert "go gene" not in obo and "go gene product" not in obo
    assert all(t["source"] and t["target"] for t in obo.values() if t["kind"] == "edge")
    assert obo["cl part of"]["adapters"] == ["cl_part_of"]


def test_source_lists_are_worked_out_at_start_up():
    """The slow part of a species' source list (each adapter's schema types) is done
    in the background when the console starts, so the first switch is quick."""
    import threading
    ci._types_cache.clear()
    ci.warm_source_caches()
    for t in threading.enumerate():
        if t.name == "warm-source-caches":
            t.join(timeout=120)
    assert {("hsa", "sample"), ("dmel", "full"), ("cel", "sample")} <= set(ci._types_cache)


def test_fly_sources_share_and_name_their_downloads():
    groups = _groups("dmel", "full")
    assert _keys(groups["GOA"]) == ["flybase"]                   # fly GO annotations ship with FlyBase
    assert "flybase" in _keys(groups["FLYBASE"])
    assert groups["EVA_Release_9"]["name"] == "EVA Release 9"    # not "EVA Release 9 –"


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


def test_adapters_land_under_their_real_source():
    """Sample data is bundled under other paths (GENCODE's under ensembl/), and some
    adapters list their files (filepaths: [...]): neither hides where their data comes
    from."""
    gencode = _groups("hsa", "sample")["GENCODE"]
    assert "gencode_gene" in [a["name"] for a in gencode["adapters"]]
    assert _keys(gencode) == ["gencode"]
    flybase = _groups("dmel", "full")["FLYBASE"]
    assert "dmel_ptp_physical_interaction" in [a["name"] for a in flybase["adapters"]]
    assert "flybase" in _keys(flybase)
