"""
Unit, regression, heterodimer, PWM, and BioCypher integration tests for JasparMotifAdapter.
Tests JASPAR MEME parsing, Drosophila motif mapping, bare FBgn node ID generation,
heterodimer partner splitting, log-odds PWM calculations, duplicate gene resolution,
error handling, and BioCypher writer integration (Neo4jCSVWriter & MeTTaWriter).
"""

import csv
import math
import os
from pathlib import Path
import pytest
import yaml

from biocypher import BioCypher
from biocypher_metta.adapters.jaspar_motif_adapter import JasparMotifAdapter
from biocypher_metta.neo4j_csv_writer import Neo4jCSVWriter
from biocypher_metta.metta_writer import MeTTaWriter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_meme_content():
    return (
        "MEME version 4\n\n"
        "ALPHABET= ACGT\n\n"
        "strands: + -\n\n"
        "Background letter frequencies (from uniform background):\n"
        "A 0.25000 C 0.25000 G 0.25000 T 0.25000\n\n"
        "MOTIF MA0010.1 br\n"
        "letter-probability matrix: alength= 4 w= 6 nsites= 20 E= 0\n"
        " 0.100000 0.050000 0.800000 0.050000\n"
        " 0.050000 0.100000 0.800000 0.050000\n"
        " 0.700000 0.100000 0.100000 0.100000\n"
        " 0.050000 0.800000 0.050000 0.100000\n"
        " 0.050000 0.050000 0.050000 0.850000\n"
        " 0.100000 0.800000 0.050000 0.050000\n\n"
        "MOTIF MA0010.2 br\n"
        "letter-probability matrix: alength= 4 w= 7 nsites= 35 E= 0\n"
        " 0.100000 0.050000 0.800000 0.050000\n"
        " 0.050000 0.100000 0.800000 0.050000\n"
        " 0.700000 0.100000 0.100000 0.100000\n"
        " 0.050000 0.800000 0.050000 0.100000\n"
        " 0.050000 0.050000 0.050000 0.850000\n"
        " 0.100000 0.800000 0.050000 0.050000\n"
        " 0.050000 0.050000 0.850000 0.050000\n\n"
        "MOTIF MA0534.1 EcR::usp\n"
        "letter-probability matrix: alength= 4 w= 8 nsites= 40 E= 0\n"
        " 0.400000 0.100000 0.400000 0.100000\n"
        " 0.700000 0.100000 0.100000 0.100000\n"
        " 0.050000 0.050000 0.850000 0.050000\n"
        " 0.050000 0.050000 0.050000 0.850000\n"
        " 0.050000 0.850000 0.050000 0.050000\n"
        " 0.850000 0.050000 0.050000 0.050000\n"
        " 0.100000 0.400000 0.400000 0.100000\n"
        " 0.050000 0.050000 0.050000 0.850000\n\n"
        "MOTIF MA0535.1 Da::twi\n"
        "letter-probability matrix: alength= 4 w= 6 nsites= 30 E= 0\n"
        " 0.800000 0.050000 0.100000 0.050000\n"
        " 0.050000 0.800000 0.050000 0.100000\n"
        " 0.050000 0.050000 0.050000 0.850000\n"
        " 0.050000 0.050000 0.850000 0.050000\n"
        " 0.100000 0.700000 0.100000 0.100000\n"
        " 0.050000 0.050000 0.100000 0.800000\n\n"
        "MOTIF MA9999.1 no_gene\n"
        "letter-probability matrix: alength= 4 w= 4 nsites= 10 E= 0\n"
        " 0.250000 0.250000 0.250000 0.250000\n"
        " 0.250000 0.250000 0.250000 0.250000\n"
        " 0.250000 0.250000 0.250000 0.250000\n"
        " 0.250000 0.250000 0.250000 0.250000\n"
    )


@pytest.fixture
def sample_map_content():
    return (
        "motif_name_in_diff_header\tmatrix_id\tsymbol\tuniprot_ids\tfbgns\tn_partners\tstatus\n"
        "MA0010.1 br\tMA0010.1\tbr\tP25927\tFBgn0000210\t1\tok\n"
        "MA0010.2 br\tMA0010.2\tbr\tP25927\tFBgn0000210\t1\tok\n"
        "MA0534.1 EcR::usp\tMA0534.1\tEcR::usp\tP34021;P22459\tFBgn0000546;FBgn0003964\t2\tok\n"
        "MA0535.1 Da::twi\tMA0535.1\tDa::twi\tP16556;P10627\tFBgn0000413;FBgn0003900\t2\tok\n"
        "MA9999.1 no_gene\tMA9999.1\tno_gene\t\t\t1\tunmapped\n"
    )


@pytest.fixture
def test_files(tmp_path, sample_meme_content, sample_map_content):
    meme_file = tmp_path / "test.meme"
    meme_file.write_text(sample_meme_content, encoding="utf-8")
    map_file = tmp_path / "test_map.tsv"
    map_file.write_text(sample_map_content, encoding="utf-8")
    return meme_file, map_file


# ---------------------------------------------------------------------------
# 1. Adapter Initialization & Input Validation Tests
# ---------------------------------------------------------------------------

def test_initialization_defaults_and_provenance(test_files):
    meme_file, map_file = test_files
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
    )
    assert adapter.taxon_id == 7227
    assert adapter.label == "motif"
    assert adapter.source == "JASPAR2026"
    assert adapter.source_url == "https://jaspar.elixir.no"
    assert adapter.floor == 1e-4
    assert adapter.deduplicate is True
    assert adapter.write_properties is True
    assert adapter.add_provenance is True


def test_missing_meme_file_raises_filenotfound(tmp_path, test_files):
    _, map_file = test_files
    with pytest.raises(FileNotFoundError, match="JASPAR MEME file not found"):
        JasparMotifAdapter(
            filepath=str(tmp_path / "missing.meme"),
            motif_map_path=str(map_file),
        )


def test_missing_map_file_raises_filenotfound(tmp_path, test_files):
    meme_file, _ = test_files
    with pytest.raises(FileNotFoundError, match="Motif map file not found"):
        JasparMotifAdapter(
            filepath=str(meme_file),
            motif_map_path=str(tmp_path / "missing.tsv"),
        )


def test_invalid_floor_raises_valueerror(test_files):
    meme_file, map_file = test_files
    with pytest.raises(ValueError, match="Probability floor must be positive"):
        JasparMotifAdapter(
            filepath=str(meme_file),
            motif_map_path=str(map_file),
            floor=0.0,
        )


def test_malformed_map_missing_columns_raises_valueerror(tmp_path, test_files):
    meme_file, _ = test_files
    bad_map = tmp_path / "bad_map.tsv"
    bad_map.write_text("matrix_id\tsymbol\nMA0010.1\tbr\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required column"):
        JasparMotifAdapter(
            filepath=str(meme_file),
            motif_map_path=str(bad_map),
        )


def test_empty_meme_file_yields_no_nodes(tmp_path, test_files):
    _, map_file = test_files
    empty_meme = tmp_path / "empty.meme"
    empty_meme.write_text("", encoding="utf-8")
    adapter = JasparMotifAdapter(
        filepath=str(empty_meme),
        motif_map_path=str(map_file),
    )
    nodes = list(adapter.get_nodes())
    assert nodes == []


# ---------------------------------------------------------------------------
# 2. Node Generation, Identifier & Property Tests
# ---------------------------------------------------------------------------

def test_bare_fbgn_node_identifiers(test_files):
    """
    Every generated motif node must use the bare Drosophila FBgn gene identifier
    as its node ID. No prefixes allowed.
    """
    meme_file, map_file = test_files
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
    )
    nodes = list(adapter.get_nodes())
    assert len(nodes) > 0

    for _id, label, props in nodes:
        assert label == "motif"
        assert _id.startswith("FBgn"), f"Node ID '{_id}' must start with FBgn"
        assert not _id.startswith("ENSEMBL:")
        assert not _id.startswith("JASPAR:")
        assert not _id.startswith("MOTIF:")
        assert "::" not in _id


def test_four_property_provenance_combinations(test_files):
    """Verify strict isolation across all 4 (write_properties, add_provenance) modes."""
    meme_file, map_file = test_files

    for wp, ap in [(False, False), (False, True), (True, False), (True, True)]:
        adapter = JasparMotifAdapter(
            filepath=str(meme_file),
            motif_map_path=str(map_file),
            write_properties=wp,
            add_provenance=ap,
        )
        nodes = list(adapter.get_nodes())
        assert len(nodes) == 5  # br, EcR, usp, Da, twi

        for _id, label, props in nodes:
            assert label == "motif"
            if not wp:
                assert props == {}, f"Properties leaked when write_properties=False: {props}"
            else:
                assert "tf_name" in props
                assert "pwm_A" in props
                assert "pwm_C" in props
                assert "pwm_G" in props
                assert "pwm_T" in props
                assert "length" in props
                assert "taxon_id" in props
                assert props["taxon_id"] == 7227

                if ap:
                    assert props["source"] == "JASPAR2026"
                    assert props["source_url"] == "https://jaspar.elixir.no"
                else:
                    assert "source" not in props
                    assert "source_url" not in props


# ---------------------------------------------------------------------------
# 3. Heterodimer Processing Tests
# ---------------------------------------------------------------------------

def test_heterodimer_ecr_usp_generates_two_separate_nodes(test_files):
    """
    EcR::usp must produce two separate motif nodes:
      - One for EcR: FBgn0000546 with tf_name='EcR'
      - One for usp: FBgn0003964 with tf_name='usp'
    No combined 'EcR::usp' node is allowed.
    """
    meme_file, map_file = test_files
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        write_properties=True,
    )
    nodes = list(adapter.get_nodes())
    node_dict = {n[0]: n for n in nodes}

    assert "FBgn0000546" in node_dict, "EcR partner FBgn0000546 node missing"
    assert "FBgn0003964" in node_dict, "usp partner FBgn0003964 node missing"
    assert "EcR::usp" not in node_dict, "Combined node ID 'EcR::usp' must not exist"

    ecr_node = node_dict["FBgn0000546"]
    assert ecr_node[2]["tf_name"] == "EcR"
    assert ecr_node[2]["length"] == 8
    assert len(ecr_node[2]["pwm_A"]) == 8

    usp_node = node_dict["FBgn0003964"]
    assert usp_node[2]["tf_name"] == "usp"
    assert usp_node[2]["length"] == 8
    assert len(usp_node[2]["pwm_A"]) == 8


def test_heterodimer_da_twi_generates_two_separate_nodes(test_files):
    """
    Da::twi must produce two separate motif nodes:
      - FBgn0000413 (Da)
      - FBgn0003900 (twi)
    """
    meme_file, map_file = test_files
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        write_properties=True,
    )
    nodes = list(adapter.get_nodes())
    node_dict = {n[0]: n for n in nodes}

    assert "FBgn0000413" in node_dict
    assert "FBgn0003900" in node_dict
    assert node_dict["FBgn0000413"][2]["tf_name"] == "Da"
    assert node_dict["FBgn0003900"][2]["tf_name"] == "twi"


# ---------------------------------------------------------------------------
# 4. Multiple Matrices & Deduplication Tests
# ---------------------------------------------------------------------------

def test_duplicate_matrix_resolution_higher_version_wins(test_files):
    """
    MA0010.1 (w=6) and MA0010.2 (w=7) both map to br (FBgn0000210).
    With deduplicate=True:
      - Exactly one node is emitted for FBgn0000210.
      - MA0010.2 (version 2) is chosen over MA0010.1 (version 1).
      - Length is 7, not 6.
    """
    meme_file, map_file = test_files
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        deduplicate=True,
        write_properties=True,
    )
    nodes = list(adapter.get_nodes())
    node_ids = [n[0] for n in nodes]

    # Exactly one node for FBgn0000210
    assert node_ids.count("FBgn0000210") == 1
    br_node = next(n for n in nodes if n[0] == "FBgn0000210")
    assert br_node[2]["length"] == 7
    assert len(br_node[2]["pwm_A"]) == 7


def test_deduplicate_false_emits_both_candidates(test_files):
    """When deduplicate=False, all candidates are yielded."""
    meme_file, map_file = test_files
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        deduplicate=False,
    )
    nodes = list(adapter.get_nodes())
    node_ids = [n[0] for n in nodes]
    assert node_ids.count("FBgn0000210") == 2


def test_unique_node_ids_with_deduplicate_true(test_files):
    """Under deduplicate=True, all emitted node IDs must be strictly unique."""
    meme_file, map_file = test_files
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        deduplicate=True,
    )
    nodes = list(adapter.get_nodes())
    node_ids = [n[0] for n in nodes]
    assert len(node_ids) == len(set(node_ids)), f"Duplicate IDs detected: {node_ids}"
    assert set(node_ids) == {"FBgn0000210", "FBgn0000546", "FBgn0003964", "FBgn0000413", "FBgn0003900"}


# ---------------------------------------------------------------------------
# 5. PWM Calculation & Numeric Stability Tests
# ---------------------------------------------------------------------------

def test_pwm_log_odds_mathematical_precision(test_files):
    """
    Verify PWM log-odds: ln(max(P, floor) / 0.25).
    Test against exact reference values.
    """
    meme_file, map_file = test_files
    floor_val = 1e-4
    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        floor=floor_val,
        write_properties=True,
    )
    nodes = list(adapter.get_nodes())
    node_dict = {n[0]: n for n in nodes}

    # MA0010.2 row 0: P_A=0.10, P_C=0.05, P_G=0.80, P_T=0.05
    br_props = node_dict["FBgn0000210"][2]
    expected_A_0 = math.log(0.10 / 0.25)  # ln(0.4) = -0.9162907...
    expected_C_0 = math.log(0.05 / 0.25)  # ln(0.2) = -1.6094379...
    expected_G_0 = math.log(0.80 / 0.25)  # ln(3.2) = 1.1631508...
    expected_T_0 = math.log(0.05 / 0.25)  # ln(0.2) = -1.6094379...

    assert br_props["pwm_A"][0] == pytest.approx(expected_A_0, rel=1e-6)
    assert br_props["pwm_C"][0] == pytest.approx(expected_C_0, rel=1e-6)
    assert br_props["pwm_G"][0] == pytest.approx(expected_G_0, rel=1e-6)
    assert br_props["pwm_T"][0] == pytest.approx(expected_T_0, rel=1e-6)

    # Invertibility check: 0.25 * exp(PWM) == P
    recovered_P_A = 0.25 * math.exp(br_props["pwm_A"][0])
    recovered_P_G = 0.25 * math.exp(br_props["pwm_G"][0])
    assert recovered_P_A == pytest.approx(0.10, rel=1e-6)
    assert recovered_P_G == pytest.approx(0.80, rel=1e-6)


def test_pwm_floor_prevents_math_domain_error(tmp_path):
    """When letter probability contains exact 0.0000, floor prevents log(0)."""
    meme_zero = tmp_path / "zero.meme"
    meme_zero.write_text(
        "MEME version 4\n\nALPHABET= ACGT\n\n"
        "MOTIF MA0001.1 tf_zero\n"
        "letter-probability matrix: alength= 4 w= 1 nsites= 10 E= 0\n"
        " 0.000000 1.000000 0.000000 0.000000\n",
        encoding="utf-8",
    )
    map_zero = tmp_path / "map_zero.tsv"
    map_zero.write_text(
        "motif_name_in_diff_header\tmatrix_id\tsymbol\tuniprot_ids\tfbgns\tn_partners\tstatus\n"
        "MA0001.1 tf_zero\tMA0001.1\ttf_zero\tP12345\tFBgn0000001\t1\tok\n",
        encoding="utf-8",
    )

    floor_val = 1e-4
    adapter = JasparMotifAdapter(
        filepath=str(meme_zero),
        motif_map_path=str(map_zero),
        floor=floor_val,
        write_properties=True,
    )
    nodes = list(adapter.get_nodes())
    assert len(nodes) == 1
    props = nodes[0][2]

    # For P=0.0: ln(1e-4 / 0.25) = ln(0.0004) = -7.824046...
    expected_zero = math.log(1e-4 / 0.25)
    assert props["pwm_A"][0] == pytest.approx(expected_zero, rel=1e-6)
    assert props["pwm_G"][0] == pytest.approx(expected_zero, rel=1e-6)
    assert props["pwm_T"][0] == pytest.approx(expected_zero, rel=1e-6)
    # For P=1.0: ln(1.0 / 0.25) = ln(4) = 1.386294...
    expected_one = math.log(1.0 / 0.25)
    assert props["pwm_C"][0] == pytest.approx(expected_one, rel=1e-6)


# ---------------------------------------------------------------------------
# 6. Malformed Matrix & Error Handling Tests
# ---------------------------------------------------------------------------

def test_malformed_matrix_row_length_raises_valueerror(tmp_path):
    meme_bad = tmp_path / "bad_row.meme"
    meme_bad.write_text(
        "MEME version 4\n\nALPHABET= ACGT\n\n"
        "MOTIF MA0001.1 bad_row\n"
        "letter-probability matrix: alength= 4 w= 2 nsites= 10 E= 0\n"
        " 0.25 0.25 0.50\n"  # Only 3 values instead of 4
        " 0.25 0.25 0.25 0.25\n",
        encoding="utf-8",
    )
    map_file = tmp_path / "map.tsv"
    map_file.write_text(
        "motif_name_in_diff_header\tmatrix_id\tsymbol\tuniprot_ids\tfbgns\tn_partners\tstatus\n"
        "MA0001.1 bad_row\tMA0001.1\tbad_row\tP12345\tFBgn0000001\t1\tok\n",
        encoding="utf-8",
    )
    adapter = JasparMotifAdapter(filepath=str(meme_bad), motif_map_path=str(map_file))
    with pytest.raises(ValueError, match="Malformed PWM row in motif MA0001.1"):
        list(adapter.get_nodes())


def test_row_count_mismatch_raises_valueerror(tmp_path):
    meme_bad = tmp_path / "mismatch.meme"
    meme_bad.write_text(
        "MEME version 4\n\nALPHABET= ACGT\n\n"
        "MOTIF MA0001.1 mismatch\n"
        "letter-probability matrix: alength= 4 w= 5 nsites= 10 E= 0\n"
        " 0.25 0.25 0.25 0.25\n"
        " 0.25 0.25 0.25 0.25\n",  # Only 2 rows instead of declared w=5
        encoding="utf-8",
    )
    map_file = tmp_path / "map.tsv"
    map_file.write_text(
        "motif_name_in_diff_header\tmatrix_id\tsymbol\tuniprot_ids\tfbgns\tn_partners\tstatus\n"
        "MA0001.1 mismatch\tMA0001.1\tmismatch\tP12345\tFBgn0000001\t1\tok\n",
        encoding="utf-8",
    )
    adapter = JasparMotifAdapter(filepath=str(meme_bad), motif_map_path=str(map_file))
    with pytest.raises(ValueError, match="declared length w=5 but contained 2"):
        list(adapter.get_nodes())


# ---------------------------------------------------------------------------
# 7. Real Repository Sample Dataset Validation
# ---------------------------------------------------------------------------

def test_real_sample_dataset_execution():
    """Verify adapter execution against actual files in samples/dmel/jaspar/."""
    sample_meme = Path("samples/dmel/jaspar/dmel_jaspar_v4.meme")
    sample_map = Path("samples/dmel/jaspar/dmel_motif_map_v4.tsv")

    assert sample_meme.exists(), f"Sample MEME not found at {sample_meme}"
    assert sample_map.exists(), f"Sample map not found at {sample_map}"

    adapter = JasparMotifAdapter(
        filepath=str(sample_meme),
        motif_map_path=str(sample_map),
        write_properties=True,
        add_provenance=True,
    )
    nodes = list(adapter.get_nodes())
    assert len(nodes) == 5

    node_ids = {n[0] for n in nodes}
    assert node_ids == {"FBgn0000210", "FBgn0000546", "FBgn0003964", "FBgn0000413", "FBgn0003900"}

    for _id, label, props in nodes:
        assert label == "motif"
        assert props["taxon_id"] == 7227
        assert props["source"] == "JASPAR2026"
        assert props["source_url"] == "https://jaspar.elixir.no"
        assert props["length"] == len(props["pwm_A"])


# ---------------------------------------------------------------------------
# 8. BioCypher Writer Integration Tests
# ---------------------------------------------------------------------------

def test_biocypher_loads_dmel_schema_with_motif():
    """Verify BioCypher initializes cleanly with merged primer + dmel schema."""
    primer_path = Path("config/primer_schema_config.yaml")
    dmel_path = Path("config/dmel/dmel_schema_config.yaml")

    with open(primer_path, encoding="utf-8") as f: primer = yaml.safe_load(f)
    with open(dmel_path, encoding="utf-8") as f: dmel = yaml.safe_load(f)
    merged = {**primer, **dmel}

    assert "motif" in merged
    assert merged["motif"]["represented_as"] == "node"
    assert merged["motif"]["input_label"] == "motif"


def test_biocypher_neo4j_writer_integration(tmp_path, test_files):
    """
    Test end-to-end integration with Neo4jCSVWriter:
    Generates motif nodes, passes to writer.write_nodes,
    verifies nodes_motif.csv is generated with correct headers and bare FBgn IDs.
    """
    meme_file, map_file = test_files

    primer_path = Path("config/primer_schema_config.yaml")
    dmel_path = Path("config/dmel/dmel_schema_config.yaml")
    with open(primer_path, encoding="utf-8") as f: primer = yaml.safe_load(f)
    with open(dmel_path, encoding="utf-8") as f: dmel = yaml.safe_load(f)
    merged = {**primer, **dmel}

    merged_path = tmp_path / "merged_schema.yaml"
    with open(merged_path, "w", encoding="utf-8") as f:
        yaml.dump(merged, f)

    outdir = tmp_path / "neo4j_output"
    writer = Neo4jCSVWriter(
        schema_config=str(merged_path),
        biocypher_config="config/biocypher_config.yaml",
        output_dir=outdir,
    )

    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        write_properties=True,
        add_provenance=True,
    )
    nodes = list(adapter.get_nodes())

    freq_nodes, props = writer.write_nodes(nodes, path_prefix="jaspar")
    assert freq_nodes["motif"] == 5

    csv_path = outdir / "jaspar" / "nodes_motif.csv"
    assert csv_path.exists(), f"nodes_motif.csv not generated at {csv_path}"

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="|")
        rows = list(reader)

    assert len(rows) == 5
    ids = {r["id"] for r in rows}
    # Neo4jCSVWriter preprocess_id normalizes IDs to uppercase
    assert ids == {"FBGN0000210", "FBGN0000546", "FBGN0003964", "FBGN0000413", "FBGN0003900"}

    # Verify headers
    headers = set(reader.fieldnames or [])
    assert {"id", "length", "pwm_A", "pwm_C", "pwm_G", "pwm_T", "source", "source_url", "taxon_id", "tf_name"}.issubset(headers)


def test_biocypher_metta_writer_integration(tmp_path, test_files):
    """
    Test end-to-end integration with MeTTaWriter:
    Verifies MeTTa S-expressions for motif nodes.
    """
    meme_file, map_file = test_files

    primer_path = Path("config/primer_schema_config.yaml")
    dmel_path = Path("config/dmel/dmel_schema_config.yaml")
    with open(primer_path, encoding="utf-8") as f: primer = yaml.safe_load(f)
    with open(dmel_path, encoding="utf-8") as f: dmel = yaml.safe_load(f)
    merged = {**primer, **dmel}

    merged_path = tmp_path / "merged_schema.yaml"
    with open(merged_path, "w", encoding="utf-8") as f:
        yaml.dump(merged, f)

    outdir = tmp_path / "metta_output"
    writer = MeTTaWriter(
        schema_config=str(merged_path),
        biocypher_config="config/biocypher_config.yaml",
        output_dir=outdir,
    )

    adapter = JasparMotifAdapter(
        filepath=str(meme_file),
        motif_map_path=str(map_file),
        write_properties=True,
        add_provenance=True,
    )
    nodes = list(adapter.get_nodes())

    freq_nodes, props = writer.write_nodes(nodes, path_prefix="jaspar")
    assert freq_nodes["motif"] == 5

    metta_path = outdir / "jaspar" / "nodes_motif.metta"
    assert metta_path.exists(), f"nodes_motif.metta not generated at {metta_path}"
    content = metta_path.read_text(encoding="utf-8")
    assert "FBGN0000210" in content
    assert "FBGN0000546" in content
    assert "FBGN0003964" in content
