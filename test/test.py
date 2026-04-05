from biocypher import BioCypher
from biocypher_metta.processors import DBSNPProcessor
import pytest
import yaml
import importlib
import logging
import os
import sys
from pathlib import Path
import tempfile
import time


logging.basicConfig(level=logging.INFO)

SMOKE_SKIP_MODULE_PATTERNS = (
    "ontologies_adapter",
    "gene_ontology_adapter",
    "uberon_adapter",
    "cell_ontology_adapter",
    "cell_line_ontology_adapter",
    "experimental_factor_ontology_adapter",
    "brenda_tissue_ontology_adapter",
    "human_phenotype_ontology_adapter",
    "chebi_ontology_adapter",
    "disease_ontology_adapter",
)

def convert_input_labels(label, replace_char="_"):
    return label.replace(" ", replace_char)

def parse_schema(bcy):
    schema = bcy._get_ontology_mapping()._extend_schema()
    edges_schema = {}
    node_labels = set()

    for k, v in schema.items():
        if v["represented_as"] == "edge":
            edge_type = convert_input_labels(k)
            source_type = v.get("source", None)
            target_type = v.get("target", None)
            if source_type is not None and target_type is not None:
                # Set label from input_label (list or scalar)
                if isinstance(v["input_label"], list):
                    label = convert_input_labels(v["input_label"][0])
                else:
                    label = convert_input_labels(v["input_label"])

                # Process source_type
                if isinstance(source_type, list):
                    source_type = [convert_input_labels(st) for st in source_type]
                    source_type_lower = [st.lower() for st in source_type]
                else:
                    source_type = convert_input_labels(source_type)
                    source_type_lower = source_type.lower()

                # Process target_type
                if isinstance(target_type, list):
                    target_type = [convert_input_labels(tt) for tt in target_type]
                    target_type_lower = [tt.lower() for tt in target_type]
                else:
                    target_type = convert_input_labels(target_type)
                    target_type_lower = target_type.lower()

                output_label = v.get("output_label", None)
                edges_schema[label.lower()] = {
                    "source": source_type_lower,
                    "target": target_type_lower,
                    "output_label": output_label.lower() if output_label is not None else None
                }

        elif v["represented_as"] == "node":
            label = v["input_label"]
            if isinstance(label, list):
                label = label[0]
            label = convert_input_labels(label)
            node_labels.add(label)

    return node_labels, edges_schema


def merge_schemas(primer_schema_path, species_schema_path):
    """
    Merges two BioCypher schema YAML files into a single dictionary and writes it to a temporary file.

    Args:
        primer_schema_path (str): Path to the primer schema YAML file.
        species_schema_path (str): Path to the species-specific schema YAML file.

    Returns:
        Path: Path to the temporary merged schema file.
    """
    primer_schema_path  = Path(primer_schema_path)
    species_schema_path = Path(species_schema_path)

    with open(primer_schema_path, 'r') as f:
        primer_schema = yaml.safe_load(f)

    with open(species_schema_path, 'r') as f:
        species_schema = yaml.safe_load(f)

    # Merge: species schemas override primer schemas on conflict
    merged_schema = {**primer_schema, **species_schema}

    # Write to a temporary file in the same directory as the primer schema
    temp_fd, temp_path = tempfile.mkstemp(
        prefix='merged_schema_',
        suffix='.yaml',
        dir=primer_schema_path.parent
    )

    try:
        with os.fdopen(temp_fd, 'w') as f:
            items = list(merged_schema.items())
            for i, (schema_name, schema_content) in enumerate(items):
                yaml.dump(
                    {schema_name: schema_content},
                    f,
                    default_flow_style=False,
                    sort_keys=False
                )
                if i < len(items) - 1:
                    f.write('\n')
    except Exception as e:
        Path(temp_path).unlink(missing_ok=True)
        raise RuntimeError(f"Error writing merged schema: {e}")

    return Path(temp_path)


def delete_temp_schema(temp_path):
    """
    Deletes the temporary file created by merge_schemas.
    Accepts both str and Path objects as argument.
    """
    try:
        Path(temp_path).unlink(missing_ok=True)
    except OSError:
        pass


@pytest.fixture(scope="session")
def setup_class(request):
    schema_config = None
    try:
        primer_schema  = request.config.getoption("--primer-schema-config")
        species_schema = request.config.getoption("--species-schema-config")
        schema_config  = merge_schemas(primer_schema, species_schema)
        request.addfinalizer(lambda: delete_temp_schema(schema_config))

        bcy = BioCypher(
            schema_config_path=str(schema_config),
            biocypher_config_path='config/biocypher_config.yaml'
        )
        node_labels, edges_schema = parse_schema(bcy)
    except FileNotFoundError as e:
        pytest.fail(f"Configuration file not found: {e}")
    except yaml.YAMLError as e:
        pytest.fail(f"Error parsing YAML file: {e}")
    except Exception as e:
        pytest.fail(f"Error initializing BioCypher: {e}")

    # Load adapters config
    adapters_config_path = request.config.getoption("--adapters-config")

    # Use DBSNPProcessor to load dbSNP mappings
    dbsnp_processor = DBSNPProcessor(cache_dir="aux_files/hsa/sample_dbsnp")
    dbsnp_processor.load_mapping()
    dbsnp_rsids_dict, dbsnp_pos_dict = dbsnp_processor.get_dict_wrappers()

    with open(adapters_config_path, 'r') as f:
        adapters_config = yaml.safe_load(f)

    test_options = {
        "mode": request.config.getoption("--adapter-test-mode"),
        "max_adapters": request.config.getoption("--adapter-max-adapters"),
        "profile": request.config.getoption("--adapter-profile"),
    }

    yield node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict, test_options


def validate_node_type(node_id, node_label, schema_node_labels):
    """
    Validate if a node type matches the schema, handling tuple IDs.
    """
    if isinstance(node_id, tuple):
        node_type = node_id[0]
        return node_type in schema_node_labels
    else:
        label = convert_input_labels(node_label)
        return label in schema_node_labels

def validate_edge_type_compatibility(source_id, target_id, edge_label, edges_schema):
    """
    Validate if source and target types are compatible with edge schema.
    Handles both single types and list types.
    """
    if edge_label.lower() not in edges_schema:
        return False, f"Edge label '{edge_label}' not found in schema"

    edge_def = edges_schema[edge_label.lower()]
    valid_source_types = edge_def["source"]
    valid_target_types = edge_def["target"]

    # Extract source type
    if isinstance(source_id, tuple):
        source_type = source_id[0].lower()
    else:
        return True, "Cannot validate source type for non-tuple ID"

    # Extract target type
    if isinstance(target_id, tuple):
        target_type = target_id[0].lower()
    else:
        return True, "Cannot validate target type for non-tuple ID"

    # Validate source type
    if isinstance(valid_source_types, list):
        if source_type not in valid_source_types:
            return False, f"Source type '{source_type}' not in valid types {valid_source_types}"
    else:
        if source_type != valid_source_types:
            return False, f"Source type '{source_type}' does not match required '{valid_source_types}'"

    # Validate target type
    if isinstance(valid_target_types, list):
        if target_type not in valid_target_types:
            return False, f"Target type '{target_type}' not in valid types {valid_target_types}"
    else:
        if target_type != valid_target_types:
            return False, f"Target type '{target_type}' does not match required '{valid_target_types}'"

    return True, "Valid"


def should_skip_adapter(adapter_name, module_name, test_mode):
    if test_mode != "smoke":
        return False, ""

    if any(pattern in module_name for pattern in SMOKE_SKIP_MODULE_PATTERNS):
        return True, "heavy ontology adapter in smoke mode"

    return False, ""


def is_ontology_adapter(module_name):
    return any(pattern in module_name for pattern in SMOKE_SKIP_MODULE_PATTERNS)


def print_profile_summary(kind, timings):
    if not timings:
        return
    total_elapsed = sum(elapsed for _, elapsed in timings)
    print(f"[{kind}] Total runtime: {total_elapsed:.2f}s across {len(timings)} adapters")
    print(f"[{kind}] Slowest adapters:")
    for adapter_name, elapsed in sorted(timings, key=lambda x: x[1], reverse=True)[:10]:
        print(f"[{kind}]   {adapter_name}: {elapsed:.2f}s")


@pytest.mark.filterwarnings("ignore")
class TestBiocypherKG:

    def test_schema_loaded_successfully(self, setup_class):
        """
        Sanity check: verifies that the merged schema was parsed and contains
        at least one node label and one edge definition.
        """
        node_labels, edges_schema, *_ = setup_class
        assert len(node_labels) > 0, "Schema contains no node labels — schema may be empty or malformed"
        assert len(edges_schema) > 0, "Schema contains no edge definitions — schema may be empty or malformed"

    def test_adapters_config_structure(self, setup_class):
        """
        Validates that each adapter entry in the config has the required keys:
        'adapter' (with 'module', 'cls', 'args'), 'nodes', and 'edges'.
        Also ensures that at least one of 'nodes' or 'edges' is True.
        """
        _, _, adapters_config, *_ = setup_class
        required_adapter_keys = {"module", "cls", "args"}
        for adapter_name, config in adapters_config.items():
            assert "adapter" in config, f"Adapter '{adapter_name}' missing 'adapter' key"
            missing = required_adapter_keys - set(config["adapter"].keys())
            assert not missing, f"Adapter '{adapter_name}' missing adapter sub-keys: {missing}"
            assert "nodes" in config, f"Adapter '{adapter_name}' missing 'nodes' key"
            assert "edges" in config, f"Adapter '{adapter_name}' missing 'edges' key"
            assert config["nodes"] or config["edges"], (
                f"Adapter '{adapter_name}' has both 'nodes' and 'edges' set to False — it produces nothing"
            )

    def test_sample_files_exist(self, setup_class):
        """
        Checks that every 'filepath' argument referenced in the adapters config
        actually exists on disk. Missing sample files would cause silent failures
        when adapters yield no data.
        """
        _, _, adapters_config, *_ = setup_class
        missing_files = []
        for adapter_name, config in adapters_config.items():
            filepath = config["adapter"]["args"].get("filepath")
            if filepath and not Path(filepath).exists():
                missing_files.append(f"  {adapter_name}: {filepath}")
        assert not missing_files, "Missing sample files referenced in adapters config:\n" + "\n".join(missing_files)

    def test_adapter_nodes_in_schema(self, setup_class):
        """
        What it tests: This test verifies that the node labels generated by the adapters are included within
        the predefined schema.

        Expected Output: It expects that for each adapter, a sample node can be retrieved,
        and the label of this node should be found in the node_labels set derived from the schema.
        If any adapter produces a node label not present in the schema, the test will fail with an assertion error.
        """
        node_labels, _, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict, test_options = setup_class
        tested_adapters = 0
        timings = []
        for adapter_name, config in adapters_config.items():
            if not config["nodes"]:
                continue

            if (
                test_options["mode"] == "smoke"
                and test_options["max_adapters"] is not None
                and tested_adapters >= test_options["max_adapters"]
            ):
                print(f"[nodes] Reached smoke cap ({test_options['max_adapters']} adapters), stopping early.")
                break

            module_name = config["adapter"]["module"]
            should_skip, reason = should_skip_adapter(adapter_name, module_name, test_options["mode"])
            if should_skip:
                print(f"[nodes] Skipping adapter: {adapter_name} ({reason})")
                continue

            print(f"[nodes] Running adapter: {adapter_name}")
            t0 = time.perf_counter()
            try:
                adapter_module = importlib.import_module(module_name)
            except Exception as e:
                error_message = (
                    f"Adapter '{adapter_name}' could not import module "
                    f"'{module_name}' ({type(e).__name__}: {e})"
                )
                if test_options["mode"] == "smoke":
                    logging.warning(f"Skipping adapter in smoke mode: {error_message}")
                    continue
                pytest.fail(error_message, pytrace=True)

            adapter_class = getattr(adapter_module, config['adapter']['cls'])

            adapter_args = config['adapter']['args'].copy()
            if "dbsnp_rsid_map" in adapter_args:
                adapter_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
            if "dbsnp_pos_map" in adapter_args:
                adapter_args["dbsnp_pos_map"] = dbsnp_pos_dict
            adapter_args['write_properties'] = True
            adapter_args['add_provenance'] = True

            adapter = adapter_class(**adapter_args)

            # Ontology adapters pre-build a full property cache before yielding any node.
            # For the test we only need label/id — skip the cache to avoid scanning all triples.
            if is_ontology_adapter(module_name) and hasattr(adapter, 'cache_node_properties'):
                adapter.cache_node_properties = lambda: None

            sample_node = next(adapter.get_nodes(), None)
            assert sample_node, f"No nodes found for adapter '{adapter_name}'"

            node_id, node_label, _ = sample_node

            is_valid = validate_node_type(node_id, node_label, node_labels)

            if isinstance(node_id, tuple):
                node_type = node_id[0]
                assert is_valid, f"Node type '{node_type}' from adapter '{adapter_name}' not found in schema"
            else:
                label = convert_input_labels(node_label)
                assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"

            tested_adapters += 1
            elapsed = time.perf_counter() - t0
            if test_options["profile"]:
                print(f"[nodes] Runtime: {adapter_name} -> {elapsed:.2f}s")
            timings.append((adapter_name, elapsed))

        assert tested_adapters > 0, "No node adapters were tested. Adjust smoke-mode filters/cap."
        if test_options["profile"]:
            print_profile_summary("nodes", timings)

    def test_adapter_edges_in_schema(self, setup_class):
        """
        What it tests: Similar to the node test, this one ensures that the edge labels produced by the adapters
        are also part of the defined schema. Additionally, it validates that source and target node types
        are compatible with the edge definition, supporting both single types and list types.

        Expected Output: It anticipates that for each adapter, a sample edge can be obtained,
        its label should be present in the edges_schema dictionary, and the source/target types
        should be compatible with the schema definition.
        A failure occurs if an adapter generates an edge label that's missing from the schema
        or if the source/target types are incompatible.
        """
        _, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict, test_options = setup_class
        tested_adapters = 0
        timings = []
        for adapter_name, config in adapters_config.items():
            if not config['edges']:
                continue

            if (
                test_options["mode"] == "smoke"
                and test_options["max_adapters"] is not None
                and tested_adapters >= test_options["max_adapters"]
            ):
                print(f"[edges] Reached smoke cap ({test_options['max_adapters']} adapters), stopping early.")
                break

            module_name = config["adapter"]["module"]
            should_skip, reason = should_skip_adapter(adapter_name, module_name, test_options["mode"])
            if should_skip:
                print(f"[edges] Skipping adapter: {adapter_name} ({reason})")
                continue

            print(f"[edges] Running adapter: {adapter_name}")
            t0 = time.perf_counter()
            try:
                adapter_module = importlib.import_module(module_name)
            except Exception as e:
                error_message = (
                    f"Adapter '{adapter_name}' could not import module "
                    f"'{module_name}' ({type(e).__name__}: {e})"
                )
                if test_options["mode"] == "smoke":
                    logging.warning(f"Skipping adapter in smoke mode: {error_message}")
                    continue
                pytest.fail(error_message, pytrace=True)

            adapter_class = getattr(adapter_module, config['adapter']['cls'])

            adapter_args = config['adapter']['args'].copy()
            if "dbsnp_rsid_map" in adapter_args:
                adapter_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
            if "dbsnp_pos_map" in adapter_args:
                adapter_args["dbsnp_pos_map"] = dbsnp_pos_dict
            adapter_args['write_properties'] = True
            adapter_args['add_provenance'] = True

            adapter = adapter_class(**adapter_args)

            if is_ontology_adapter(module_name):
                if hasattr(adapter, 'cache_node_properties'):
                    adapter.cache_node_properties = lambda: None
                if hasattr(adapter, 'cache_edge_properties'):
                    adapter.cache_edge_properties = lambda: None

            sample_edge = next(adapter.get_edges(), None)

            # Sparse adapters are expected to be empty with small sample data
            sparse_adapters = ["overlap", "uniprot_has_xref", "uniprot_dbxref", "uniprot_chebi"]
            if not sample_edge and any(sparse in adapter_name for sparse in sparse_adapters):
                logging.warning(f"No edges found for sparse adapter '{adapter_name}'. This is expected with the current sample data.")
                continue

            assert sample_edge, f"No edges found for adapter '{adapter_name}'"

            source_id, target_id, edge_label, _ = sample_edge
            assert edge_label.lower() in edges_schema, f"Edge label '{edge_label}' from adapter '{adapter_name}' not found in schema"

            is_valid, message = validate_edge_type_compatibility(source_id, target_id, edge_label, edges_schema)
            if not is_valid:
                assert is_valid, f"Edge '{edge_label}' from adapter '{adapter_name}': {message}"

            tested_adapters += 1
            elapsed = time.perf_counter() - t0
            if test_options["profile"]:
                print(f"[edges] Runtime: {adapter_name} -> {elapsed:.2f}s")
            timings.append((adapter_name, elapsed))

        assert tested_adapters > 0, "No edge adapters were tested. Adjust smoke-mode filters/cap."
        if test_options["profile"]:
            print_profile_summary("edges", timings)
