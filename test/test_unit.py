"""
Unit tests for biocypher-kg utilities and processors.

These tests cover code paths that are NOT exercised by the schema-validation
integration tests in test.py, providing standalone, fast tests that require
no real data files or network access.

Coverage areas:
- biocypher_metta/adapters/helpers.py
    build_variant_id, build_regulatory_region_id, build_variant_id_from_hgvs,
    to_float, check_genomic_location, convert_genome_reference
- biocypher_metta/processors/dbsnp_processor.py
    lookup logic (nested/flat formats), get_dict_wrappers, get_position, get_rsid
- biocypher_metta/processors/go_subontology_processor.py
    _uri_to_go_id, subontology predicates, filter_by_subontology
- biocypher_metta/processors/base_mapping_processor.py
    save/load roundtrip, version info, check_update_needed, get_mapping
- test.py utility functions
    convert_input_labels, merge_schemas, validate_node_type,
    validate_edge_type_compatibility, should_skip_adapter, is_ontology_adapter,
    print_profile_summary
"""

import gzip
import importlib.util
import json
import pickle
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# ---------------------------------------------------------------------------
# Source-code imports
# ---------------------------------------------------------------------------
from biocypher_metta.adapters.helpers import (
    build_variant_id,
    build_regulatory_region_id,
    build_variant_id_from_hgvs,
    check_genomic_location,
    to_float,
    convert_genome_reference,
)
from biocypher_metta.processors.dbsnp_processor import DBSNPProcessor
from biocypher_metta.processors.go_subontology_processor import GOSubontologyProcessor
from biocypher_metta.processors.base_mapping_processor import BaseMappingProcessor

# ---------------------------------------------------------------------------
# Load test.py utilities via importlib to avoid the name clash with stdlib 'test'
# ---------------------------------------------------------------------------
_test_py_path = Path(__file__).parent / "test.py"
_spec = importlib.util.spec_from_file_location("_biocypher_test_utils", _test_py_path)
_test_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_test_utils)

convert_input_labels = _test_utils.convert_input_labels
merge_schemas = _test_utils.merge_schemas
delete_temp_schema = _test_utils.delete_temp_schema
validate_node_type = _test_utils.validate_node_type
validate_edge_type_compatibility = _test_utils.validate_edge_type_compatibility
should_skip_adapter = _test_utils.should_skip_adapter
is_ontology_adapter = _test_utils.is_ontology_adapter
print_profile_summary = _test_utils.print_profile_summary
SMOKE_SKIP_MODULE_PATTERNS = _test_utils.SMOKE_SKIP_MODULE_PATTERNS


# ===========================================================================
# Helpers — build_variant_id
# ===========================================================================

class TestBuildVariantId:

    def test_returns_expected_format(self):
        result = build_variant_id("1", 100, "A", "T")
        assert result == "1_100_A_T_GRCh38"

    def test_lowercases_chromosome(self):
        result = build_variant_id("CHR1", 100, "A", "T")
        assert result.startswith("chr1_")

    def test_x_chromosome(self):
        result = build_variant_id("X", 500, "G", "C")
        assert result == "x_500_G_C_GRCh38"

    def test_custom_assembly_in_output(self):
        result = build_variant_id("2", 200, "C", "G", assembly="GRCh38")
        assert result.endswith("_GRCh38")

    def test_invalid_assembly_raises(self):
        with pytest.raises(ValueError, match="Assembly not supported"):
            build_variant_id("1", 100, "A", "T", assembly="hg19")


# ===========================================================================
# Helpers — build_regulatory_region_id
# ===========================================================================

class TestBuildRegulatoryRegionId:

    def test_returns_expected_format(self):
        result = build_regulatory_region_id("chr3", 1000, 2000)
        assert result == "chr3_1000_2000_GRCh38"

    def test_default_assembly_in_output(self):
        result = build_regulatory_region_id("chrX", 500, 600)
        assert result.endswith("_GRCh38")

    def test_invalid_assembly_raises(self):
        with pytest.raises(ValueError, match="Assembly not supported"):
            build_regulatory_region_id("chr1", 100, 200, assembly="hg38")


# ===========================================================================
# Helpers — build_variant_id_from_hgvs (validate=False, no network required)
# ===========================================================================

class TestBuildVariantIdFromHgvs:

    def test_standard_substitution_chromosome_1_to_22(self):
        # NC_000003.12 -> chromosome 3
        result = build_variant_id_from_hgvs(
            "NC_000003.12:g.183917980C>T", validate=False
        )
        assert result == "3_183917980_C_T_GRCh38"

    def test_chromosome_23_maps_to_x(self):
        result = build_variant_id_from_hgvs(
            "NC_000023.11:g.1000A>G", validate=False
        )
        assert result == "x_1000_A_G_GRCh38"

    def test_chromosome_24_maps_to_y(self):
        result = build_variant_id_from_hgvs(
            "NC_000024.10:g.2000T>C", validate=False
        )
        assert result == "y_2000_T_C_GRCh38"

    def test_unsupported_chromosome_returns_none(self):
        result = build_variant_id_from_hgvs(
            "NC_000025.99:g.100A>G", validate=False
        )
        assert result is None

    def test_non_nc_prefix_returns_none(self):
        result = build_variant_id_from_hgvs(
            "NM_000001.1:c.100A>G", validate=False
        )
        assert result is None

    def test_intronic_position_returns_none(self):
        # Intronic notation (100+5) is non-numeric -> should return None
        result = build_variant_id_from_hgvs(
            "NC_000001.11:g.100+5A>G", validate=False
        )
        assert result is None


# ===========================================================================
# Helpers — to_float
# ===========================================================================

class TestToFloat:

    def test_zero(self):
        assert to_float("0") == 0.0

    def test_normal_positive(self):
        assert to_float("1.5") == pytest.approx(1.5)

    def test_normal_negative(self):
        assert to_float("-2.5") == pytest.approx(-2.5)

    def test_positive_infinity_clamped(self):
        # to_float explicitly returns float('1e307') for +inf to stay within
        # a 64-bit-safe range for ArangoDB.
        result = to_float("inf")
        assert result == 1e307

    def test_negative_infinity_clamped(self):
        # The implementation returns float('1e-307') (a small positive number)
        # for negative infinity; this matches the source code's intended clamping
        # behaviour for ArangoDB compatibility.
        result = to_float("-inf")
        assert result == 1e-307

    def test_large_string_float(self):
        # 1e300 is finite and within float range, should pass through unchanged
        result = to_float("1e300")
        assert result == pytest.approx(1e300)


# ===========================================================================
# Helpers — check_genomic_location
# ===========================================================================

class TestCheckGenomicLocation:

    def test_no_chr_filter_returns_true(self):
        assert check_genomic_location(None, None, None, "1", 100, 200) is True

    def test_chr_mismatch_returns_false(self):
        assert check_genomic_location("1", None, None, "2", 100, 200) is False

    def test_chr_match_no_range_filter(self):
        assert check_genomic_location("1", None, None, "1", 100, 200) is True

    def test_start_filter_in_range(self):
        assert check_genomic_location("1", 100, None, "1", 150, 200) is True

    def test_start_filter_out_of_range(self):
        assert check_genomic_location("1", 100, None, "1", 50, 200) is False

    def test_end_filter_in_range(self):
        assert check_genomic_location("1", None, 200, "1", 100, 150) is True

    def test_end_filter_out_of_range(self):
        assert check_genomic_location("1", None, 200, "1", 100, 250) is False

    def test_start_and_end_filter_in_range(self):
        assert check_genomic_location("1", 100, 200, "1", 150, 180) is True

    def test_start_and_end_filter_start_out_of_range(self):
        assert check_genomic_location("1", 100, 200, "1", 50, 180) is False

    def test_start_and_end_filter_end_out_of_range(self):
        assert check_genomic_location("1", 100, 200, "1", 150, 250) is False


# ===========================================================================
# Helpers — convert_genome_reference (validation only; no liftover download)
# ===========================================================================

class TestConvertGenomeReference:

    def test_same_build_raises(self):
        with pytest.raises(ValueError, match="Invalid reference build versions"):
            convert_genome_reference("chr1", 1000, from_build="hg38", to_build="hg38")

    def test_invalid_from_build_raises(self):
        with pytest.raises(ValueError, match="Invalid reference build versions"):
            convert_genome_reference("chr1", 1000, from_build="hg17", to_build="hg38")

    def test_invalid_to_build_raises(self):
        with pytest.raises(ValueError, match="Invalid reference build versions"):
            convert_genome_reference("chr1", 1000, from_build="hg19", to_build="hg36")


# ===========================================================================
# DBSNPProcessor — pure logic (no file I/O)
# ===========================================================================

class TestDBSNPProcessorIsNestedFormat:

    def _processor_with_mapping(self, mapping):
        proc = DBSNPProcessor.__new__(DBSNPProcessor)
        proc.name = "dbsnp"
        proc.mapping = mapping
        return proc

    def test_nested_format_rsid_to_pos(self):
        proc = self._processor_with_mapping({"rsid_to_pos": {}, "pos_to_rsid": {}})
        assert proc._is_nested_format() is True

    def test_nested_format_pos_to_rsid_only(self):
        proc = self._processor_with_mapping({"pos_to_rsid": {}})
        assert proc._is_nested_format() is True

    def test_flat_format(self):
        proc = self._processor_with_mapping({"rs123": {"chr": "1", "pos": 100}})
        assert proc._is_nested_format() is False

    def test_empty_mapping_flat(self):
        proc = self._processor_with_mapping({})
        assert proc._is_nested_format() is False


class TestDBSNPProcessorGetPosition:

    def _processor(self, mapping):
        proc = DBSNPProcessor.__new__(DBSNPProcessor)
        proc.name = "dbsnp"
        proc.mapping = mapping
        proc.mapping_file = Path("/nonexistent/path")
        return proc

    def test_nested_format_known_rsid(self):
        rsid_data = {"rs123": {"chr": "1", "pos": 500}}
        proc = self._processor({"rsid_to_pos": rsid_data, "pos_to_rsid": {}})
        assert proc.get_position("rs123") == {"chr": "1", "pos": 500}

    def test_nested_format_unknown_rsid(self):
        proc = self._processor({"rsid_to_pos": {}, "pos_to_rsid": {}})
        assert proc.get_position("rs999") is None

    def test_flat_format_known_rsid(self):
        proc = self._processor({"rs456": {"chr": "2", "pos": 1000}})
        assert proc.get_position("rs456") == {"chr": "2", "pos": 1000}

    def test_empty_mapping_no_file_returns_none(self):
        proc = self._processor({})
        assert proc.get_position("rs123") is None


class TestDBSNPProcessorGetRsid:

    def _processor(self, mapping):
        proc = DBSNPProcessor.__new__(DBSNPProcessor)
        proc.name = "dbsnp"
        proc.mapping = mapping
        proc.mapping_file = Path("/nonexistent/path")
        return proc

    def test_nested_format_known_position(self):
        pos_data = {"1:500": "rs123"}
        proc = self._processor({"rsid_to_pos": {}, "pos_to_rsid": pos_data})
        assert proc.get_rsid("1", 500) == "rs123"

    def test_nested_format_adds_chr_prefix(self):
        pos_data = {"chr1:500": "rs123"}
        proc = self._processor({"rsid_to_pos": {}, "pos_to_rsid": pos_data})
        assert proc.get_rsid("1", 500) == "rs123"

    def test_nested_format_unknown_position(self):
        proc = self._processor({"rsid_to_pos": {}, "pos_to_rsid": {}})
        assert proc.get_rsid("1", 9999) is None

    def test_flat_format_returns_none(self):
        # Legacy flat format has no pos_to_rsid
        proc = self._processor({"rs123": {"chr": "1", "pos": 500}})
        assert proc.get_rsid("1", 500) is None


class TestDBSNPProcessorGetDictWrappers:

    def _processor(self, mapping):
        proc = DBSNPProcessor.__new__(DBSNPProcessor)
        proc.name = "dbsnp"
        proc.mapping = mapping
        return proc

    def test_nested_format_returns_both_sub_dicts(self):
        rsid_map = {"rs1": {"chr": "1", "pos": 10}}
        pos_map = {"1:10": "rs1"}
        proc = self._processor({"rsid_to_pos": rsid_map, "pos_to_rsid": pos_map})
        rsid_out, pos_out = proc.get_dict_wrappers()
        assert rsid_out == rsid_map
        assert pos_out == pos_map

    def test_flat_format_returns_mapping_and_empty_pos(self):
        flat_map = {"rs1": {"chr": "1", "pos": 10}}
        proc = self._processor(flat_map)
        rsid_out, pos_out = proc.get_dict_wrappers()
        assert rsid_out == flat_map
        assert pos_out == {}

    def test_unloaded_mapping_raises_runtime_error(self):
        proc = self._processor({})
        with pytest.raises(RuntimeError, match="Mapping not loaded"):
            proc.get_dict_wrappers()


# ===========================================================================
# GOSubontologyProcessor — pure logic
# ===========================================================================

class TestGOSubontologyProcessorUriConversion:

    def setup_method(self):
        self.proc = GOSubontologyProcessor.__new__(GOSubontologyProcessor)

    def test_valid_uri_returns_go_id(self):
        uri = "http://purl.obolibrary.org/obo/GO_0008150"
        assert self.proc._uri_to_go_id(uri) == "GO:0008150"

    def test_uri_without_go_prefix_returns_none(self):
        uri = "http://purl.obolibrary.org/obo/CHEBI_12345"
        assert self.proc._uri_to_go_id(uri) is None

    def test_empty_string_returns_none(self):
        assert self.proc._uri_to_go_id("") is None


class TestGOSubontologyProcessorPredicates:

    def _processor_with_mapping(self, mapping):
        proc = GOSubontologyProcessor.__new__(GOSubontologyProcessor)
        proc.mapping = mapping
        proc.name = "go_subontology"
        proc.cache_dir = Path("/tmp/test_go")
        proc.mapping_file = proc.cache_dir / "go_subontology_mapping.pkl"
        return proc

    def test_is_biological_process_true(self):
        proc = self._processor_with_mapping({"GO:0008150": "biological_process"})
        assert proc.is_biological_process("GO:0008150") is True

    def test_is_biological_process_false_wrong_subontology(self):
        proc = self._processor_with_mapping({"GO:0003674": "molecular_function"})
        assert proc.is_biological_process("GO:0003674") is False

    def test_is_molecular_function_true(self):
        proc = self._processor_with_mapping({"GO:0003674": "molecular_function"})
        assert proc.is_molecular_function("GO:0003674") is True

    def test_is_cellular_component_true(self):
        proc = self._processor_with_mapping({"GO:0005575": "cellular_component"})
        assert proc.is_cellular_component("GO:0005575") is True

    def test_unknown_go_id_returns_false_for_all(self):
        proc = self._processor_with_mapping({})
        assert proc.is_biological_process("GO:9999999") is False
        assert proc.is_molecular_function("GO:9999999") is False
        assert proc.is_cellular_component("GO:9999999") is False

    def test_filter_by_subontology(self):
        mapping = {
            "GO:0008150": "biological_process",
            "GO:0003674": "molecular_function",
            "GO:0005575": "cellular_component",
            "GO:0006915": "biological_process",
        }
        proc = self._processor_with_mapping(mapping)
        bp = proc.filter_by_subontology(list(mapping.keys()), "biological_process")
        assert set(bp) == {"GO:0008150", "GO:0006915"}

    def test_filter_by_subontology_empty_input(self):
        proc = self._processor_with_mapping({"GO:0008150": "biological_process"})
        assert proc.filter_by_subontology([], "biological_process") == []


# ===========================================================================
# BaseMappingProcessor — concrete subclass for testing
# ===========================================================================

class _SimpleMappingProcessor(BaseMappingProcessor):
    """Minimal concrete subclass for testing BaseMappingProcessor."""

    def fetch_data(self):
        return {"key1": "value1", "key2": "value2"}

    def process_data(self, raw_data):
        return raw_data


class TestBaseMappingProcessorSaveLoad:

    def test_save_and_load_mapping_roundtrip(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        proc.mapping = {"a": "1", "b": "2"}
        proc.save_mapping()
        assert proc.mapping_file.exists()

        # Clear in-memory mapping and reload from disk
        proc.mapping = {}
        proc.load_mapping()
        assert proc.mapping == {"a": "1", "b": "2"}

    def test_load_mapping_uncompressed_fallback(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        data = {"x": "y"}
        # Write as a plain (uncompressed) pickle
        with open(proc.mapping_file, "wb") as f:
            pickle.dump(data, f)

        proc.load_mapping()
        assert proc.mapping == data
        # After loading, it should be re-saved as gzip-compressed
        with gzip.open(proc.mapping_file, "rb") as f:
            reloaded = pickle.load(f)
        assert reloaded == data


class TestBaseMappingProcessorVersionInfo:

    def test_save_and_load_version_info(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        proc.mapping = {"k": "v"}
        proc.save_version_info()

        info = proc._load_version_info()
        assert info is not None
        assert info["processor"] == "test_proc"
        assert "timestamp" in info
        assert info["entries"] == 1

    def test_load_version_info_missing_file(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        assert proc._load_version_info() is None

    def test_load_version_info_invalid_json(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        proc.version_file.write_text("NOT VALID JSON {{")
        assert proc._load_version_info() is None


class TestBaseMappingProcessorUpdateCheck:

    def test_check_update_needed_when_no_files(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        # No mapping_file or version_file => update required
        assert proc.check_update_needed() is True

    def test_check_update_not_needed_within_interval(self, tmp_path):
        proc = _SimpleMappingProcessor(
            name="test_proc",
            cache_dir=str(tmp_path),
            update_interval_hours=24,
        )
        proc.mapping = {"a": "b"}
        proc.save_mapping()
        proc.save_version_info()

        # Patch has_remote_update to return False (no remote update)
        with patch.object(proc, "has_remote_update", return_value=False):
            assert proc.check_update_needed() is False


class TestBaseMappingProcessorGetMapping:

    def test_get_mapping_existing_key(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        proc.mapping = {"gene1": "ENSG001"}
        assert proc.get_mapping("gene1") == "ENSG001"

    def test_get_mapping_missing_key_returns_default(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        proc.mapping = {}
        assert proc.get_mapping("missing", default="N/A") == "N/A"

    def test_get_mapping_missing_key_returns_none_by_default(self, tmp_path):
        proc = _SimpleMappingProcessor(name="test_proc", cache_dir=str(tmp_path))
        proc.mapping = {}
        assert proc.get_mapping("missing") is None


# ===========================================================================
# test.py utility functions
# ===========================================================================

class TestConvertInputLabels:

    def test_spaces_replaced_with_underscore(self):
        assert convert_input_labels("gene expression") == "gene_expression"

    def test_no_spaces_unchanged(self):
        assert convert_input_labels("gene_expression") == "gene_expression"

    def test_empty_string(self):
        assert convert_input_labels("") == ""

    def test_custom_replace_char(self):
        assert convert_input_labels("gene expression", replace_char="-") == "gene-expression"


class TestMergeSchemas:

    def _write_yaml(self, path, data):
        with open(path, "w") as f:
            yaml.dump(data, f)

    def test_species_overrides_primer(self, tmp_path):
        primer = tmp_path / "primer.yaml"
        species = tmp_path / "species.yaml"
        self._write_yaml(primer, {"TermA": {"represented_as": "node", "label": "a"}})
        self._write_yaml(
            species, {"TermA": {"represented_as": "edge", "label": "a_overridden"}}
        )
        merged_path = merge_schemas(str(primer), str(species))
        try:
            with open(merged_path) as f:
                merged = yaml.safe_load(f)
            assert merged["TermA"]["represented_as"] == "edge"
        finally:
            delete_temp_schema(merged_path)

    def test_primer_only_key_retained(self, tmp_path):
        primer = tmp_path / "primer.yaml"
        species = tmp_path / "species.yaml"
        self._write_yaml(primer, {"OnlyInPrimer": {"represented_as": "node"}})
        self._write_yaml(species, {"OnlyInSpecies": {"represented_as": "edge"}})
        merged_path = merge_schemas(str(primer), str(species))
        try:
            with open(merged_path) as f:
                merged = yaml.safe_load(f)
            assert "OnlyInPrimer" in merged
            assert "OnlyInSpecies" in merged
        finally:
            delete_temp_schema(merged_path)

    def test_missing_primer_file_raises(self, tmp_path):
        species = tmp_path / "species.yaml"
        self._write_yaml(species, {"TermA": {}})
        with pytest.raises(FileNotFoundError):
            merge_schemas(str(tmp_path / "nonexistent.yaml"), str(species))


class TestDeleteTempSchema:

    def test_deletes_existing_file(self, tmp_path):
        f = tmp_path / "tmp_schema.yaml"
        f.write_text("dummy")
        assert f.exists()
        delete_temp_schema(f)
        assert not f.exists()

    def test_missing_file_does_not_raise(self, tmp_path):
        # Should not raise even if file is already gone
        delete_temp_schema(tmp_path / "does_not_exist.yaml")


class TestValidateNodeType:

    NODE_LABELS = {"Gene", "Protein", "Disease"}

    def test_tuple_id_type_in_schema(self):
        assert validate_node_type(("Gene", "id123"), "Gene", self.NODE_LABELS) is True

    def test_tuple_id_type_not_in_schema(self):
        assert validate_node_type(("Variant", "id123"), "Variant", self.NODE_LABELS) is False

    def test_string_id_label_in_schema(self):
        assert validate_node_type("id123", "Protein", self.NODE_LABELS) is True

    def test_string_id_label_not_in_schema(self):
        assert validate_node_type("id123", "Unknown", self.NODE_LABELS) is False

    def test_label_with_spaces_normalized(self):
        labels = {"gene_expression"}
        assert validate_node_type("id123", "gene expression", labels) is True


class TestValidateEdgeTypeCompatibility:

    EDGES_SCHEMA = {
        "transcribes_to": {
            "source": "gene",
            "target": "transcript",
            "output_label": None,
        },
        "interacts_with": {
            "source": ["protein", "gene"],
            "target": ["protein", "gene"],
            "output_label": None,
        },
    }

    def test_valid_edge_single_source_target(self):
        ok, msg = validate_edge_type_compatibility(
            ("Gene", "g1"), ("Transcript", "t1"), "transcribes_to", self.EDGES_SCHEMA
        )
        assert ok is True

    def test_edge_label_not_in_schema(self):
        ok, msg = validate_edge_type_compatibility(
            ("Gene", "g1"), ("Protein", "p1"), "unknown_edge", self.EDGES_SCHEMA
        )
        assert ok is False
        assert "not found in schema" in msg

    def test_incompatible_source_type(self):
        ok, msg = validate_edge_type_compatibility(
            ("Variant", "v1"), ("Transcript", "t1"), "transcribes_to", self.EDGES_SCHEMA
        )
        assert ok is False
        assert "variant" in msg.lower()

    def test_incompatible_target_type(self):
        ok, msg = validate_edge_type_compatibility(
            ("Gene", "g1"), ("Disease", "d1"), "transcribes_to", self.EDGES_SCHEMA
        )
        assert ok is False
        assert "disease" in msg.lower()

    def test_valid_edge_list_source_target(self):
        ok, msg = validate_edge_type_compatibility(
            ("Protein", "p1"), ("Gene", "g1"), "interacts_with", self.EDGES_SCHEMA
        )
        assert ok is True

    def test_non_tuple_source_id_skips_validation(self):
        ok, _ = validate_edge_type_compatibility(
            "plain_id", ("Transcript", "t1"), "transcribes_to", self.EDGES_SCHEMA
        )
        assert ok is True

    def test_non_tuple_target_id_skips_validation(self):
        ok, _ = validate_edge_type_compatibility(
            ("Gene", "g1"), "plain_id", "transcribes_to", self.EDGES_SCHEMA
        )
        assert ok is True


class TestShouldSkipAdapter:

    def test_full_mode_never_skips(self):
        skip, _ = should_skip_adapter(
            "any_adapter", "ontologies_adapter", "full"
        )
        assert skip is False

    def test_smoke_mode_skips_ontology_module(self):
        skip, reason = should_skip_adapter(
            "go_nodes", "gene_ontology_adapter", "smoke"
        )
        assert skip is True
        assert reason != ""

    def test_smoke_mode_does_not_skip_regular_module(self):
        skip, _ = should_skip_adapter(
            "string_ppi", "string_ppi_adapter", "smoke"
        )
        assert skip is False

    def test_smoke_mode_skips_all_patterns_in_skip_list(self):
        for pattern in SMOKE_SKIP_MODULE_PATTERNS:
            skip, _ = should_skip_adapter("adapter", pattern, "smoke")
            assert skip is True, f"Expected '{pattern}' to be skipped in smoke mode"


class TestIsOntologyAdapter:

    def test_ontology_module_recognised(self):
        assert is_ontology_adapter("gene_ontology_adapter") is True

    def test_ontology_base_module_recognised(self):
        assert is_ontology_adapter("ontologies_adapter") is True

    def test_regular_module_not_recognised(self):
        assert is_ontology_adapter("string_ppi_adapter") is False

    def test_uberon_recognised(self):
        assert is_ontology_adapter("uberon_adapter") is True


class TestPrintProfileSummary:

    def test_empty_timings_does_not_raise(self, capsys):
        print_profile_summary("nodes", [])
        # No output expected and no exception
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_non_empty_timings_prints_summary(self, capsys):
        timings = [("adapter_a", 1.5), ("adapter_b", 0.3), ("adapter_c", 2.1)]
        print_profile_summary("edges", timings)
        captured = capsys.readouterr()
        assert "Total runtime" in captured.out
        assert "Slowest adapters" in captured.out
