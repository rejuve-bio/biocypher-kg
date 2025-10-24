from collections import Counter, defaultdict
import json
import csv
from biocypher._logger import logger
import networkx as nx
import rdflib
from pathlib import Path
from biocypher_metta import BaseWriter

class Neo4jCSVWriter(BaseWriter):
    def __init__(self, schema_config, biocypher_config, output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.csv_delimiter = '|'
        self.array_delimiter = ';'
        self.translation_table = str.maketrans({
            self.csv_delimiter: '',
            self.array_delimiter: ' ',
            "'": "",
            '"': ""
        })
        self.ontologies = set(['go', 'bto', 'efo', 'cl', 'clo', 'uberon', 'so', 'do', 'mi', 'fbbt', 'fbdv', 'fbcv'])

        # --- MODIFICATION START: KGX INTEGRATION ---
        self._node_headers = defaultdict(set)
        self._edge_headers = defaultdict(set)
        self._temp_files = {}
        self.batch_size = 10000
        self.temp_buffer = defaultdict(list)

        # Schema validation structures from KGXWriter
        self.node_schema_properties = defaultdict(set)
        self.edge_schema_properties = defaultdict(set)
        self.edge_configs = {}  # Store full edge configs
        self.node_configs = {}   # Store full node configs
        self._initialize_schema_validation() # New call
        self.create_node_types() # New call
        # --- MODIFICATION END: KGX INTEGRATION ---

        self.create_edge_types() # Existing call, will be modified

    # --- MODIFICATION START: KGX INTEGRATION ---
    def _normalize_label(self, label):
        """Normalize label for schema lookup (from KGXWriter)."""
        if not label:
            return label
        if isinstance(label, list):
            return [l.lower().replace(" ", "_") for l in label]
        return label.lower().replace(" ", "_")

    def _initialize_schema_validation(self):
        """Initialize schema validation structures from the schema configuration (from KGXWriter)."""
        schema = self.bcy._get_ontology_mapping()._extend_schema()

        for label, config in schema.items():
            # Use original label for config lookup, but normalized for schema properties
            input_label = config.get("input_label")
            normalized_labels = self._normalize_label(input_label)

            if config.get("represented_as") == "node":
                if isinstance(normalized_labels, list):
                    for nl in normalized_labels:
                        self._process_node_schema(nl, config)
                else:
                    self._process_node_schema(normalized_labels, config)

            elif config.get("represented_as") == "edge":
                if isinstance(normalized_labels, list):
                    for nl in normalized_labels:
                        self._process_edge_schema(nl, config)
                else:
                    self._process_edge_schema(normalized_labels, config)

    def _process_node_schema(self, label, config):
        """Extract valid properties from node schema including inherited properties (from KGXWriter)."""
        self.node_schema_properties[label].update(['id']) # Required KGX property 'id'

        if 'properties' in config:
            props = config['properties']
            if isinstance(props, list):
                self.node_schema_properties[label].update(props)
            elif isinstance(props, dict):
                self.node_schema_properties[label].update(props.keys())

        kgx_props = config.get('kgx_properties', {})
        if kgx_props:
            self.node_schema_properties[label].update(kgx_props.keys())
            self.node_schema_properties[label].add('category') # KGX requires category

        if config.get('inherit_properties', False):
            self._add_inherited_properties(label, config)

    def _process_edge_schema(self, label, config):
        """Extract valid properties from edge schema including KGX properties (from KGXWriter)."""
        # Note: KGX requires 'id', 'subject', 'object', 'label', 'source_type', 'target_type'
        # For Neo4j output, we map subject/object to source_id/target_id.
        self.edge_schema_properties[label].update([
            'id', 'subject', 'object', 'label', 'source_type', 'target_type'
        ])

        if 'biolink_predicate' in config:
            self.edge_schema_properties[label].add('predicate')

        if 'properties' in config:
            props = config['properties']
            if isinstance(props, list):
                self.edge_schema_properties[label].update(props)
            elif isinstance(props, dict):
                self.edge_schema_properties[label].update(props.keys())

        kgx_props = config.get('kgx_properties', {})
        if kgx_props:
            self.edge_schema_properties[label].update(kgx_props.keys())

    def _add_inherited_properties(self, label, config):
        """Recursively add properties from parent classes (from KGXWriter)."""
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        parent_classes = config.get('is_a', [])
        if not isinstance(parent_classes, list):
            parent_classes = [parent_classes]

        for parent in parent_classes:
            parent_config = None
            for k, v in schema.items():
                if v.get('input_label') == parent or k == parent:
                    parent_config = v
                    break

            if parent_config:
                if 'properties' in parent_config:
                    props = parent_config['properties']
                    if isinstance(props, list):
                        self.node_schema_properties[label].update(props)
                    elif isinstance(props, dict):
                        self.node_schema_properties[label].update(props.keys())

                if parent_config.get('inherit_properties', False):
                    self._add_inherited_properties(label, parent_config)

    def _validate_node_properties(self, label, properties):
        """Validate node properties against schema and return only valid ones (from KGXWriter)."""
        normalized_label = self._normalize_label(label)
        if isinstance(normalized_label, list): # handle cases where label itself could be a list
            normalized_label = normalized_label[0] # take the first one for validation lookup

        valid_properties = {}
        schema_props = self.node_schema_properties.get(normalized_label, set())

        for prop, value in properties.items():
            if prop in schema_props:
                valid_properties[prop] = value

        return valid_properties

    def _validate_edge_properties(self, label, properties):
        """Validate edge properties against schema and return only valid ones (from KGXWriter)."""
        normalized_label = self._normalize_label(label)
        if isinstance(normalized_label, list): # handle cases where label itself could be a list
            normalized_label = normalized_label[0] # take the first one for validation lookup

        valid_properties = {}
        schema_props = self.edge_schema_properties.get(normalized_label, set())

        for prop, value in properties.items():
            if prop in schema_props:
                valid_properties[prop] = value

        return valid_properties
    # --- MODIFICATION END: KGX INTEGRATION ---

    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}
        for k, v in schema.items():
            if v["represented_as"] == "edge":
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type is not None and target_type is not None:
                    # --- MODIFICATION START: KGX INTEGRATION ---
                    # Use _normalize_label for consistency with schema lookups
                    if isinstance(v["input_label"], list):
                        label = self._normalize_label(v["input_label"][0])
                        source_type_normalized = self._normalize_label(source_type[0])
                        target_type_normalized = self._normalize_label(target_type[0])
                    else:
                        label = self._normalize_label(v["input_label"])
                        source_type_normalized = self._normalize_label(source_type)
                        target_type_normalized = self._normalize_label(target_type)
                    # --- MODIFICATION END: KGX INTEGRATION ---

                    output_label = v.get("output_label", label)

                    if '.' not in k: # Original check
                        self.edge_node_types[label] = {
                            "source": source_type_normalized,
                            "target": target_type_normalized,
                            "output_label": output_label
                        }
                        # --- MODIFICATION START: KGX INTEGRATION ---
                        self.edge_configs[label] = v # Store full config
                        # --- MODIFICATION END: KGX INTEGRATION ---

    # --- MODIFICATION START: KGX INTEGRATION ---
    def create_node_types(self):
        """Populate node_configs for schema validation (from KGXWriter)."""
        schema = self.bcy._get_ontology_mapping()._extend_schema()

        for k, v in schema.items():
            if v.get("represented_as") == "node":
                if isinstance(v["input_label"], list):
                    label = self._normalize_label(v["input_label"][0])
                else:
                    label = self._normalize_label(v["input_label"])
                output_label = v.get("output_label", label)

                self.node_configs[label] = {
                    "output_label": output_label,
                    "config": v
                }
    # --- MODIFICATION END: KGX INTEGRATION ---

    def preprocess_value(self, value):
        value_type = type(value)
        if value_type is list:
            return json.dumps([self.preprocess_value(item) for item in value]).replace('"', '"')
        if value_type is rdflib.term.Literal:
            return str(value).translate(self.translation_table)
        if value_type is str:
            return value.translate(self.translation_table)
        return value

    # --- MODIFICATION START: KGX INTEGRATION (replaces existing version) ---
    def preprocess_id(self, prev_id):
        """Ensure ID remains in CURIE format while cleaning special characters (from KGXWriter)."""
        if isinstance( prev_id, tuple ):
            prev_id = prev_id[1]
        prev_id = str(prev_id) # Ensure it's a string

        if ':' in prev_id:
            prefix, local_id = prev_id.split(':', 1)
            # Standardize prefix to uppercase
            prefix = prefix.upper()
            # Clean local ID (remove duplicate prefix if present)
            clean_local = local_id.lower().replace(f"{prefix.lower()}_", "")
            clean_local = clean_local.strip().translate(str.maketrans({' ': '_'}))
            return f"{prefix}:{clean_local}"
        # Fallback for IDs without prefix, convert to lowercase and replace spaces/colons
        return prev_id.lower().strip().translate(str.maketrans({' ': '_', ':': '_'}))
    # --- MODIFICATION END: KGX INTEGRATION ---

    def _write_buffer_to_temp(self, label_or_key, buffer):
        if buffer and label_or_key in self._temp_files:
            with open(self._temp_files[label_or_key], 'a') as f:
                for entry in buffer:
                    json.dump(entry, f)
                    f.write('\n')
            buffer.clear()

    def _init_node_writer(self, label, properties, path_prefix=None, adapter_name=None):
        output_dir = self.get_output_path(path_prefix, adapter_name)
        # --- MODIFICATION START: KGX INTEGRATION ---
        # Update headers with *all* keys from the prepared node_data, not just input properties
        self._node_headers[label].update(properties.keys())
        # --- MODIFICATION END: KGX INTEGRATION ---
        
        # Original logic might add 'id' explicitly, but our node_data will already have it
        # self._node_headers[label].add('id')

        if label not in self._temp_files:
            temp_file_path = output_dir / f"temp_nodes_{label}.jsonl"
            if temp_file_path.exists():
                temp_file_path.unlink()
            self._temp_files[label] = temp_file_path
        return label

    def _init_edge_writer(self, label, source_type, target_type, properties, path_prefix=None, adapter_name=None):
        output_dir = self.get_output_path(path_prefix, adapter_name)
        key = (label, source_type, target_type)
        # --- MODIFICATION START: KGX INTEGRATION ---
        # Update headers with *all* keys from the prepared edge_data, not just input properties
        self._edge_headers[key].update(properties.keys())
        # --- MODIFICATION END: KGX INTEGRATION ---

        # Original logic might add these explicitly, but our edge_data will already have them
        # self._edge_headers[key].update({'source_id', 'target_id', 'label', 'source_type', 'target_type'})

        if key not in self._temp_files:
            temp_file_path = output_dir / f"temp_edges_{label}_{source_type}_{target_type}.jsonl"
            if temp_file_path.exists():
                temp_file_path.unlink()
            self._temp_files[key] = temp_file_path
        return key

    def write_nodes(self, nodes, path_prefix=None, adapter_name=None):
        self.temp_buffer.clear()
        self._temp_files.clear()
        self._node_headers.clear()
        node_freq = defaultdict(int)
        output_dir = self.get_output_path(path_prefix, adapter_name)

        try:
            for node in nodes:
                self.extract_node_info(node) # Original call from BaseWriter

                node_id, label, properties = node # Renamed from 'id' to 'node_id' to avoid confusion with internal 'id' property
                # --- MODIFICATION START: KGX INTEGRATION ---
                normalized_label = self._normalize_label(label)
                node_freq[normalized_label] += 1

                # Get node info from schema
                node_info = self.node_configs.get(normalized_label, {})
                node_config = node_info.get("config", {})
                output_label = node_info.get("output_label", normalized_label)

                # Validate properties against schema
                validated_props = self._validate_node_properties(normalized_label, properties)

                # Create base node data with required properties and validated ones
                node_data = {
                    'id': self.preprocess_id(node_id), # Ensure 'id' is always present and preprocessed
                    **validated_props
                }

                # Get KGX properties from schema and merge
                kgx_props = node_config.get('kgx_properties', {})
                for kgx_key, kgx_value in kgx_props.items():
                    if kgx_key not in node_data:  # Don't overwrite existing properties
                        node_data[kgx_key] = kgx_value

                # Ensure category is set (from kgx_properties or fallback to output_label)
                if 'category' not in node_data:
                    node_data['category'] = output_label

                # Use output_label for writer key and CSV file naming
                writer_key = self._init_node_writer(output_label, node_data, path_prefix, adapter_name)
                self.temp_buffer[output_label].append(node_data)

                if len(self.temp_buffer[output_label]) >= self.batch_size:
                    self._write_buffer_to_temp(output_label, self.temp_buffer[output_label])
            # --- MODIFICATION END: KGX INTEGRATION ---

            for label in list(self.temp_buffer.keys()):
                self._write_buffer_to_temp(label, self.temp_buffer[label])

            for label in self._node_headers.keys():
                csv_file_path = output_dir / f"nodes_{label}.csv"
                cypher_file_path = output_dir / f"nodes_{label}.cypher"

                if csv_file_path.exists():
                    csv_file_path.unlink()
                if cypher_file_path.exists():
                    cypher_file_path.unlink()

                with open(csv_file_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=sorted(self._node_headers[label]),
                                         delimiter=self.csv_delimiter, extrasaction='ignore')
                    writer.writeheader()

                    if label in self._temp_files and self._temp_files[label].exists():
                        with open(self._temp_files[label], 'r') as temp_f:
                            chunk = []
                            for line in temp_f:
                                chunk.append(json.loads(line))
                                if len(chunk) >= self.batch_size:
                                    for data in chunk:
                                        writer.writerow({k: self.preprocess_value(v) for k, v in data.items()})
                                    chunk.clear()

                            for data in chunk:
                                writer.writerow({k: self.preprocess_value(v) for k, v in data.items()})

                self.write_node_cypher(label, csv_file_path, cypher_file_path)
                if label in self._temp_files and self._temp_files[label].exists():
                    self._temp_files[label].unlink()

        finally:
            self.temp_buffer.clear()
            for temp_file in self._temp_files.values():
                if isinstance(temp_file, Path) and temp_file.exists():
                    temp_file.unlink()
            self._temp_files.clear()

        return node_freq, self._node_headers

    def write_edges(self, edges, path_prefix=None, adapter_name=None):
        self.temp_buffer.clear()
        self._temp_files.clear()
        self._edge_headers.clear()
        edge_freq = defaultdict(int)
        output_dir = self.get_output_path(path_prefix, adapter_name)

        try:
            for edge in edges:
                self.extract_edge_info(edge) # Original call from BaseWriter

                source_id, target_id, label, properties = edge # Original unpacking
                # --- MODIFICATION START: KGX INTEGRATION ---
                normalized_label = self._normalize_label(label)
                edge_freq[normalized_label] += 1

                # Get edge info from schema
                edge_info = self.edge_node_types.get(normalized_label, {})
                edge_config = self.edge_configs.get(normalized_label, {})
                source_type_from_schema = edge_info.get("source", "")
                target_type_from_schema = edge_info.get("target", "")
                output_edge_label = edge_info.get("output_label", normalized_label)

                # Validate properties against schema
                validated_props = self._validate_edge_properties(normalized_label, properties)

                # The original source_type and target_type might be tuples, handle that.
                # The actual source_type and target_type used for file naming and the Cypher
                # query need to be determined robustly.
                current_source_type = source_type_from_schema
                if isinstance(source_id, tuple):
                    current_source_type = source_id[0]
                    source_id = source_id[1] # Use actual ID for processing
                current_target_type = target_type_from_schema
                if isinstance(target_id, tuple):
                    current_target_type = target_id[0]
                    target_id = target_id[1] # Use actual ID for processing

                # Preprocess IDs for output
                preprocessed_source_id = self.preprocess_id(source_id)
                preprocessed_target_id = self.preprocess_id(target_id)

                # Handle ontology terms in source/target types for specific cases
                if current_source_type == "ontology_term":
                    current_source_type = preprocessed_source_id.split('_')[0]
                if current_target_type == "ontology_term":
                    current_target_type = preprocessed_target_id.split('_')[0]

                # Construct edge_data for output
                edge_data = {
                    'source_id': preprocessed_source_id,
                    'target_id': preprocessed_target_id,
                    'label': output_edge_label, # Use output_edge_label from schema
                    'source_type': current_source_type, # Use determined source type
                    'target_type': current_target_type, # Use determined target type
                    **validated_props # Include validated properties
                }

                # Add predicate if defined in schema
                if 'predicate' in self.edge_schema_properties.get(normalized_label, set()):
                    edge_data['predicate'] = edge_config.get('biolink_predicate')

                # Merge in all KGX properties from schema
                kgx_props = edge_config.get('kgx_properties', {})
                for kgx_key, kgx_value in kgx_props.items():
                    if kgx_key not in edge_data:  # Don't overwrite existing properties
                        edge_data[kgx_key] = kgx_value

                # The KGXWriter includes an 'id' for edges. For Neo4jCSVWriter,
                # we don't typically need 'id' on the relationship itself for the merge query.
                # If an 'id' property is desired for edges in Neo4j,
                # the write_edge_cypher function would also need modification.
                # For now, we omit an auto-generated 'id' for edges in this writer,
                # assuming only node IDs need to be unique by constraint.
                # If a user provides an 'id' in properties, it will be included by validated_props.

                # Use output_edge_label and determined source/target types for writer key and file naming
                writer_key = self._init_edge_writer(output_edge_label, current_source_type, current_target_type, edge_data, path_prefix, adapter_name)
                self.temp_buffer[writer_key].append(edge_data)

                if len(self.temp_buffer[writer_key]) >= self.batch_size:
                    self._write_buffer_to_temp(writer_key, self.temp_buffer[writer_key])
            # --- MODIFICATION END: KGX INTEGRATION ---

            for key in list(self.temp_buffer.keys()):
                self._write_buffer_to_temp(key, self.temp_buffer[key])

            for key in self._edge_headers.keys():
                input_label, source_type, target_type = key
                edge_label_for_file = self.edge_node_types.get(input_label, {}).get("output_label") or input_label

                file_suffix = f"{edge_label_for_file}_{source_type}_{target_type}".lower()
                csv_file_path = output_dir / f"edges_{file_suffix}.csv"
                cypher_file_path = output_dir / f"edges_{file_suffix}.cypher"

                if csv_file_path.exists():
                    csv_file_path.unlink()
                if cypher_file_path.exists():
                    cypher_file_path.unlink()

                with open(csv_file_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=sorted(self._edge_headers[key]),
                                     delimiter=self.csv_delimiter, extrasaction='ignore')
                    writer.writeheader()

                    if key in self._temp_files and self._temp_files[key].exists():
                        with open(self._temp_files[key], 'r') as temp_f:
                            chunk = []
                            for line in temp_f:
                                chunk.append(json.loads(line))
                                if len(chunk) >= self.batch_size:
                                    for data in chunk:
                                        writer.writerow({k: self.preprocess_value(v) for k, v in data.items()})
                                    chunk.clear()

                            for data in chunk:
                                writer.writerow({k: self.preprocess_value(v) for k, v in data.items()})

                self.write_edge_cypher(edge_label_for_file, source_type, target_type, csv_file_path, cypher_file_path)
                if key in self._temp_files and self._temp_files[key].exists():
                    self._temp_files[key].unlink()

        finally:
            self.temp_buffer.clear()
            for temp_file in self._temp_files.values():
                if isinstance(temp_file, Path) and temp_file.exists():
                    temp_file.unlink()
            self._temp_files.clear()

        return edge_freq

    def write_node_cypher(self, label, csv_path, cypher_path):
        absolute_path = csv_path.resolve().as_posix()

        cypher_query = f"""
CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MERGE (n:{label} {{id: row.id}})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {{batchSize:1000, parallel:true, concurrency:4}}
)
YIELD batches, total
RETURN batches, total;
"""
        with open(cypher_path, 'w') as f:
            f.write(cypher_query)

    def write_edge_cypher(self, edge_label, source_type, target_type, csv_path, cypher_path):
        absolute_path = csv_path.resolve().as_posix()

        cypher_query = f"""
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MATCH (source:{source_type} {{id: row.source_id}})
    MATCH (target:{target_type} {{id: row.target_id}})
    MERGE (source)-[r:{edge_label}]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {{batchSize:1000}}
)
YIELD batches, total
RETURN batches, total;
"""
        with open(cypher_path, 'w') as f:
            f.write(cypher_query)

    def get_output_path(self, prefix=None, adapter_name=None):
        if prefix:
            output_dir = self.output_path / prefix
        elif adapter_name:
            output_dir = self.output_path / adapter_name
        else:
            output_dir = self.output_path

        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir