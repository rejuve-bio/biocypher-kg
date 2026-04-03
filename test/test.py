from biocypher import BioCypher
import pytest
import yaml
import importlib
import logging
import os
import sys
from pathlib import Path
import tempfile


logging.basicConfig(level=logging.INFO)

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
                if isinstance(v["input_label"], list):
                    label = convert_input_labels(v["input_label"][0])
                    if isinstance(source_type, list):
                        source_type = [convert_input_labels(st) for st in source_type]
                        source_type_lower = [st.lower() for st in source_type]
                    else:
                        source_type = convert_input_labels(source_type)
                        source_type_lower = source_type.lower()
                    
                    if isinstance(target_type, list):
                        target_type = [convert_input_labels(tt) for tt in target_type]
                        target_type_lower = [tt.lower() for tt in target_type]
                    else:
                        target_type = convert_input_labels(target_type)
                        target_type_lower = target_type.lower()
                else:
                    label = convert_input_labels(v["input_label"])
                    if isinstance(source_type, list):
                        source_type = [convert_input_labels(st) for st in source_type]
                        source_type_lower = [st.lower() for st in source_type]
                    else:
                        source_type = convert_input_labels(source_type)
                        source_type_lower = source_type.lower()
                    
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
        prime_schema_config_path (str): Path to the prime schema YAML file.
        species_schema_config_path (str): Path to the species-specific schema YAML file.
    
    Returns:
        str: Path to the temporary merged schema file.    
    """

    primer_schema_path   = Path(primer_schema_path)
    species_schema_path = Path(species_schema_path)

    # Load both YAML files
    with open(primer_schema_path, 'r') as f:
        primer_schema = yaml.safe_load(f)

    with open(species_schema_path, 'r') as f:
        species_schema = yaml.safe_load(f)

    # Merge: species schemas override primer schemas on conflict
    merged_schema = {**primer_schema, **species_schema}

    # Write to a temporary file in the same directory as the prime schema
    temp_fd, temp_path = tempfile.mkstemp(
        suffix='.yaml',
        dir=primer_schema_path.parent  # same dir as prime schema
    )

    try:
        with os.fdopen(temp_fd, 'w') as f:
            # Write each schema with blank line separation
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
        # Clean up temp file if write fails
        Path(temp_path).unlink(missing_ok=True)
        raise RuntimeError(f"Error writing merged schema: {e}")

    return Path(temp_path)  # always returns a Path object


def delete_temp_schema(temp_path):
    """
    Deletes the temporary file created by merge_schemas.
    Accepts both str and Path objects as argument.
    """
    try:
        Path(temp_path).unlink(missing_ok=True)
    except OSError:
        pass  # File may already be deleted or inaccessible

@pytest.fixture(scope="session")
def setup_class(request):
    try:
        # merge species schema with primer schema ---> species schemas with same name prevails over primer schemas
        schema_config = merge_schemas('config/primer_schema_config.yaml', 'config/hsa/hsa_schema_config.yaml')
        bcy = BioCypher(
            # schema_config_path='config/hsa/hsa_schema_config.yaml',
            schema_config_path = schema_config,
            biocypher_config_path = 'config/biocypher_config.yaml'
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
   
    # Load adapters config
    with open(adapters_config_path, 'r') as f:
        adapters_config = yaml.safe_load(f)

    return node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict

def validate_node_type(node_id, node_label, schema_node_labels):
    """
    Validate if a node type matches the schema, handling tuple IDs.
    """
    if isinstance(node_id, tuple):
        node_type = node_id[0]
        return node_type in schema_node_labels
    else:
        # For non-tuple IDs, check if the label is in schema
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
        # For non-tuple source IDs, we can't validate type compatibility
        return True, "Cannot validate source type for non-tuple ID"
    
    # Extract target type
    if isinstance(target_id, tuple):
        target_type = target_id[0].lower()
    else:
        # For non-tuple target IDs, we can't validate type compatibility
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

@pytest.mark.filterwarnings("ignore")
class TestBiocypherKG:
    def test_adapter_nodes_in_schema(self, setup_class):
        """
        What it tests: This test verifies that the node labels generated by the adapters are included within 
        the predefined schema.

        Expected Output: It expects that for each adapter, a sample node can be retrieved, 
        and the label of this node should be found in the node_labels set derived from the schema. 
        If any adapter produces a node label not present in the schema, the test will fail with an assertion error.
        """
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        for adapter_name, config in adapters_config.items():
            if config["nodes"]:
                adapter_module = importlib.import_module(config['adapter']['module'])
                adapter_class = getattr(adapter_module, config['adapter']['cls'])
                    
                # Add write_properties and add_provenance to the arguments
                adapter_args = config['adapter']['args'].copy()
                if "dbsnp_rsid_map" in adapter_args: #this for dbs that use grch37 assembly and to map grch37 to grch38
                        adapter_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
                if "dbsnp_pos_map" in adapter_args:
                    adapter_args["dbsnp_pos_map"] = dbsnp_pos_dict
                adapter_args['write_properties'] = True
                adapter_args['add_provenance'] = True
                    
                adapter = adapter_class(**adapter_args)
                
                # Get a sample node from the adapter
                sample_node = next(adapter.get_nodes(), None)
                assert sample_node, f"No nodes found for adapter '{adapter_name}'"
                
                node_id, node_label, node_props = sample_node
                
                # Validate node type
                is_valid = validate_node_type(node_id, node_label, node_labels)
                
                if isinstance(node_id, tuple):
                    node_type = node_id[0]
                    assert is_valid, f"Node type '{node_type}' from adapter '{adapter_name}' not found in schema"
                else:
                    label = convert_input_labels(node_label)
                    assert label in node_labels, f"Node label '{label}' from adapter '{adapter_name}' not found in schema"
                
                #TODO Check if node properties are defined in schema
                # schema_props = schema[label].get('properties', {})
                # for prop in node_props:
                #     assert prop in schema_props, f"Property '{prop}' of node '{node_label}' from adapter '{adapter_name}' not found in schema"

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
        node_labels, edges_schema, adapters_config, dbsnp_rsids_dict, dbsnp_pos_dict = setup_class
        for adapter_name, config in adapters_config.items():
            if config['edges']:

                adapter_module = importlib.import_module(config['adapter']['module'])
                adapter_class = getattr(adapter_module, config['adapter']['cls'])
                    
                    # Add write_properties and add_provenance to the arguments
                adapter_args = config['adapter']['args'].copy()
                if "dbsnp_rsid_map" in adapter_args: #this for dbs that use grch37 assembly and to map grch37 to grch38
                    adapter_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
                if "dbsnp_pos_map" in adapter_args:
                    adapter_args["dbsnp_pos_map"] = dbsnp_pos_dict
                adapter_args['write_properties'] = True
                adapter_args['add_provenance'] = True
                
                adapter = adapter_class(**adapter_args)
                
                # Get a sample edge from the adapter
                sample_edge = next(adapter.get_edges(), None)

                #rule for sparse adapters - expected to be empty with small sample data
                sparse_adapters = ["overlap", "uniprot_has_xref", "uniprot_dbxref", "uniprot_chebi"]
                if not sample_edge and any(sparse in adapter_name for sparse in sparse_adapters):
                    logging.warning(f"No edges found for sparse adapter '{adapter_name}'. This is expected with the current sample data.")
                    continue

                assert sample_edge, f"No edges found for adapter '{adapter_name}'"
                
                source_id, target_id, edge_label, edge_props = sample_edge
                assert edge_label.lower() in edges_schema, f"Edge label '{edge_label}' from adapter '{adapter_name}' not found in schema"
                
                # Validate source and target type compatibility
                is_valid, message = validate_edge_type_compatibility(source_id, target_id, edge_label, edges_schema)
                
                # Only assert if validation failed (not just warning messages)
                if not is_valid:
                    assert is_valid, f"Edge '{edge_label}' from adapter '{adapter_name}': {message}"
                
                #TODO Check if edge properties are defined in schema
                # schema_props = schema[edge_label].get('properties', {})
                # for prop in edge_props:
                #     assert prop in schema_props, f"Property '{prop}' of edge '{edge_label}' from adapter '{adapter_name}' not found in schema"

# Additional tests can be added here