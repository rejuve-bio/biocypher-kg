"""Genomic coordinate convention checks.

The KG stores every position as 1-based closed. Each adapter that writes a
position (start/end/pos/overlap_*) or builds a region ID from coordinates must
declare the source convention in a ``COORDINATES`` class constant (see the
README section "Genomic coordinates"). These tests enforce that and check the
conversions that used to be wrong.
"""

import ast
import gzip
import pickle
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ADAPTERS_DIR = REPO_ROOT / "biocypher_metta" / "adapters"

ZERO_BASED = "0-based half-open"
ONE_BASED = "1-based closed"
EXPECTED_OFFSETS = {ZERO_BASED: (1, 0), ONE_BASED: (0, 0)}
POSITION_KEYS = {"start", "end", "pos", "overlap_start", "overlap_end"}
ALLOWED_KEYS = {"source_convention", "source_assembly", "start_offset", "end_offset",
                "components", "note", "liftover"}


def _writes_positions(tree):
    """True if a function in the module writes a position key or builds a region ID."""
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(func):
            if isinstance(node, ast.Dict):
                if any(isinstance(k, ast.Constant) and k.value in POSITION_KEYS for k in node.keys):
                    return True
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if (isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant)
                            and target.slice.value in POSITION_KEYS):
                        return True
            elif isinstance(node, ast.Call):
                func_node = node.func
                name = getattr(func_node, "id", None) or getattr(func_node, "attr", None)
                if name == "build_regulatory_region_id":
                    return True
    return False


def _coordinates(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if (isinstance(item, ast.Assign)
                        and any(isinstance(t, ast.Name) and t.id == "COORDINATES" for t in item.targets)):
                    return ast.literal_eval(item.value)
    return None


def _adapter_files():
    return sorted(p for p in ADAPTERS_DIR.rglob("*_adapter.py") if "__pycache__" not in p.parts)


POSITION_ADAPTERS = [p for p in _adapter_files() if _writes_positions(ast.parse(p.read_text()))]


def _check_convention_offsets(where, entry):
    convention = entry.get("source_convention")
    assert convention in EXPECTED_OFFSETS, f"{where}: unknown source_convention {convention!r}"
    offsets = (entry.get("start_offset"), entry.get("end_offset"))
    assert offsets == EXPECTED_OFFSETS[convention], (
        f"{where}: {convention} needs (start_offset, end_offset) = "
        f"{EXPECTED_OFFSETS[convention]}, got {offsets}")


def test_position_adapters_found():
    # Guards the detector itself: these adapters are known to write positions.
    names = {p.name for p in POSITION_ADAPTERS}
    assert {"dbsnp_adapter.py", "tfbs_adapter.py", "peregrine_adapter.py",
            "candidate_cis_regulatory_enhancer_adapter.py"} <= names


@pytest.mark.parametrize("path", POSITION_ADAPTERS, ids=lambda p: str(p.relative_to(ADAPTERS_DIR)))
def test_adapter_declares_coordinates(path):
    rel = path.relative_to(ADAPTERS_DIR)
    coords = _coordinates(ast.parse(path.read_text()))
    assert coords is not None, (
        f"{rel} writes genomic positions but has no COORDINATES class constant "
        f"(see README, 'Genomic coordinates')")
    assert set(coords) <= ALLOWED_KEYS, f"{rel}: unexpected keys {set(coords) - ALLOWED_KEYS}"

    assembly = coords.get("source_assembly")
    assert assembly, f"{rel}: source_assembly is required"
    if isinstance(assembly, dict):
        assert all(isinstance(k, int) and isinstance(v, str) and v for k, v in assembly.items()), (
            f"{rel}: a per-species source_assembly must map int taxon ids to assembly names")
    else:
        assert isinstance(assembly, str)

    if coords["source_convention"] == "mixed":
        components = coords.get("components")
        assert components, f"{rel}: a mixed source_convention needs components"
        assert "start_offset" not in coords and "end_offset" not in coords, (
            f"{rel}: put offsets in each component of a mixed source")
        for name, component in components.items():
            assert set(component) <= {"source_convention", "start_offset", "end_offset"}
            _check_convention_offsets(f"{rel} [{name}]", component)
    else:
        assert "components" not in coords
        _check_convention_offsets(str(rel), coords)


def test_peregrine_offsets_match_coordinates():
    from biocypher_metta.adapters.hsa.peregrine_adapter import PEREGRINEAdapter

    components = PEREGRINEAdapter.COORDINATES["components"]
    assert PEREGRINEAdapter.START_OFFSET_BY_SOURCE == {
        name: c["start_offset"] for name, c in components.items()}


# --- Conversions that used to be wrong -------------------------------------


class _FakeHGNC:
    def get_ensembl_id(self, symbol):
        return "ENSG00000000001"


def _gz(path, text):
    with gzip.open(path, "wt") as f:
        f.write(text)
    return str(path)


def test_ccre_enhancer_keeps_bed_end(tmp_path):
    from biocypher_metta.adapters.candidate_cis_regulatory_enhancer_adapter import EnhancercCREAdapter

    bed = _gz(tmp_path / "ccre.bed.gz", "chr1\t10033\t10250\tEH38D1\tEH38E1\tdELS\n")
    adapter = EnhancercCREAdapter(bed, taxon_id=9606, label="enhancer")
    [(node_id, _, props)] = list(adapter.get_nodes())
    assert (props["start"], props["end"]) == (10034, 10250)
    assert node_id == "ENCODE_SCREEN:chr1_10034_10250_GRCh38"


def test_ccre_promoter_keeps_bed_end(tmp_path):
    from biocypher_metta.adapters.candidate_cis_regulatory_promoter_adapter import PromotercCREAdapter

    line = "\t".join(["chr1", "10033", "10250", "EH38D1", "EH38E1", "PLS", "chr1", "11869", "11869",
                      "ENST1", ".", "+", "ENSG00000223972.5"])
    bed = _gz(tmp_path / "ccre.bed.gz", line + "\n")
    adapter = PromotercCREAdapter(bed, None, taxon_id=9606, label="promoter", edge_type="nearest")
    [(node_id, _, props)] = list(adapter.get_nodes())
    assert (props["start"], props["end"]) == (10034, 10250)
    assert node_id == "ENCODE_SCREEN:chr1_10034_10250_GRCh38"


def test_tfbs_start_is_one_based(tmp_path):
    from biocypher_metta.adapters.tfbs_adapter import TfbsAdapter

    table = _gz(tmp_path / "tfbs.txt.gz", "1\tchr1\t8388344\t8388800\tZBTB33\t341\t1\t471\t341\n")
    adapter = TfbsAdapter(True, False, table, label="tfbs", hgnc_processor=_FakeHGNC())
    [(node_id, _, props)] = list(adapter.get_nodes())
    assert (props["start"], props["end"]) == (8388345, 8388800)
    [(_, edge_target, _, _)] = list(adapter.get_edges())
    # Both get_nodes() and get_edges() convert the start; check each, so a node/edge mismatch fails.
    assert node_id == "ENCODE_TFBS:chr1_8388345_8388800_GRCh38"
    assert edge_target == node_id


def test_tadmap_start_is_one_based(tmp_path):
    from biocypher_metta.adapters.tadmap_adapter import TADMapAdapter

    csv_path = tmp_path / "tadmap.csv"
    csv_path.write_text("header\n1|chr1|800000|1350000,SAMD11|Ensembl:ENSG00000187634|HGNC:SAMD11\n")
    adapter = TADMapAdapter(str(csv_path), True, False, 9606, "tad")
    [(node_id, _, props)] = list(adapter.get_nodes())
    assert (props["start"], props["end"]) == (800001, 1350000)
    [(_, edge_tad, _, _)] = list(adapter.get_edges())
    assert edge_tad == node_id == "SO:chr1_800001_1350000_GRCh38"


def test_peregrine_offsets_by_sub_source(tmp_path):
    from biocypher_metta.adapters.hsa.peregrine_adapter import PEREGRINEAdapter

    enhancers = _gz(tmp_path / "enh.gz",
                    "chr1\t100\t200\t1\nchr1\t300\t400\tEH1\nchr1\t501\t600\t87657\nchr1\t701\t800\ths1\n")
    sources = _gz(tmp_path / "src.gz", "1\tFANTOM\nEH1\tENCODE\n87657\tEnsembl\nhs1\tVISTA\n")
    links = _gz(tmp_path / "links.gz",
                "enhancer\tgene\tlinkID\tassay\ttissue\tp-value\teQTL_SNP_ID\tscore\n"
                "1\tHUMAN|HGNC=15846|UniProtKB=Q9NP74\t1\t3\t64\t\t\t\n")
    tissue_map = tmp_path / "tissues.pkl"
    with open(tissue_map, "wb") as f:
        pickle.dump({"64": ["UBERON:0000001"]}, f)

    adapter = PEREGRINEAdapter(enhancers, links, sources, tissue_ontology_map=str(tissue_map),
                               write_properties=True, add_provenance=False, hgnc_processor=_FakeHGNC())
    starts = {props["data_source"]: (props["start"], props["end"]) for _, _, props in adapter.get_nodes()}
    assert starts == {"FANTOM": (101, 200), "ENCODE": (301, 400),
                      "Ensembl": (501, 600), "VISTA": (701, 800)}
    [(edge_source, _, _, _)] = list(adapter.get_edges())
    assert edge_source == "PEREGRINE:chr1_101_200_GRCh38"


def test_schema_generator_resolves_coordinates():
    from schema_generator.generate_data_source_schemas import SchemaGenerator as Gen

    coords = {"source_convention": ZERO_BASED, "source_assembly": {9606: "hg38", 7227: "dm6"},
              "start_offset": 1, "end_offset": 0}
    props = {"chr": "str", "start": "int", "end": "int"}
    entry = Gen.resolve_coordinates(coords, {"taxon_id": 7227}, props)
    assert "kg_convention" not in entry  # stated once per file, at the top level
    assert entry["source_assembly"] == "dm6"
    assert "source_assembly" not in Gen.resolve_coordinates(coords, {"taxon_id": 10090}, props)
    assert Gen.resolve_coordinates(coords, {"taxon_id": 9606}, {"chr": "str"}) is None


def test_peregrine_check_script_counts_ties_with_duplicate_references():
    """A reference interval listed twice must not hide a tie with another interval."""
    import importlib.util

    path = REPO_ROOT / "scripts" / "hsa" / "check_peregrine_coords.py"
    spec = importlib.util.spec_from_file_location("check_peregrine_coords", path)
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)

    peregrine = [("chr1", 101, 200, "x")]
    # (100, 200) is offset (+1, 0) and (101, 201) is (0, -1): equally close, so ambiguous.
    refs = [("chr1", 100, 200, "a"), ("chr1", 101, 201, "b"), ("chr1", 101, 201, "b")]
    tally, ambiguous, unmatched = script.positional_test(peregrine, refs)
    assert (sum(tally.values()), ambiguous, unmatched) == (0, 1, 0)
