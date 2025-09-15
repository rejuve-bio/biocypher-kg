from collections import defaultdict
import json
import csv
from pathlib import Path
from biocypher._logger import logger
from biocypher_metta import BaseWriter

class KGXWriter(BaseWriter):
    def __init__(self, schema_config, biocypher_config, output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.csv_delimiter = ','
        self.array_delimiter = ';'
        self.translation_table = str.maketrans({
            self.csv_delimiter: '', 
            self.array_delimiter: ' ', 
            "'": "",
            '"': ""
        })
        self._node_headers = defaultdict(set)
        self._edge_headers = defaultdict(set)
        self._temp_files = {}
        self.batch_size = 10000
        self.temp_buffer = defaultdict(list)
        
        # Schema validation structures
        self.node_schema_properties = defaultdict(set)
        self.edge_schema_properties = defaultdict(set)
        self.edge_node_types = {}
        self.edge_configs = {}  # Store full edge configs
        self.node_configs = {}   # Store full node configs
        self._initialize_schema_validation()
        self.create_edge_types()
        self.create_node_types()

    def create_edge_types(self):
        """
        Map edge types to their source and target node types based on the schema,
        supporting multiple source and target types.
        """
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}
        self.edge_configs = {}

        for k, v in schema.items():
            if v.get("represented_as") == "edge":
                edge_type = self._normalize_label(k)
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type and target_type:
                    # Convert to list if not already
                    if not isinstance(source_type, list):
                        source_type = [source_type]
                    if not isinstance(target_type, list):
                        target_type = [target_type]

                    # Normalize all source/target types
                    source_type = self._normalize_label(source_type)
                    target_type = self._normalize_label(target_type)

                    # Determine label
                    if isinstance(v["input_label"], list):
                        label = self._normalize_label(v["input_label"][0])
                    else:
                        label = self._normalize_label(v["input_label"])

                    output_label = v.get("output_label", label)

                    # Store the full list of source/target types
                    self.edge_node_types[label.lower()] = {
                        "source": source_type,
                        "target": target_type,
                        "output_label": output_label.lower()
                    }

                    # Keep full edge config for KGX properties
                    self.edge_configs[label.lower()] = v

    def create_node_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        
        for k, v in schema.items():
            if v.get("represented_as") == "node":
                if isinstance(v["input_label"], list):
                    label = self._normalize_label(v["input_label"][0])
                else:
                    label = self._normalize_label(v["input_label"])
                output_label = v.get("output_label", label)
                
                self.node_configs[label.lower()] = {
                    "output_label": output_label.lower(),
                    "config": v
                }

    def _initialize_schema_validation(self):
        """Initialize schema validation structures from the schema configuration"""
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        
        for label, config in schema.items():
            normalized_label = self._normalize_label(config.get("input_label"))
            
            if config.get("represented_as") == "node":
                if isinstance(normalized_label, list):
                    for nl in normalized_label:
                        self._process_node_schema(nl, config)
                else:
                    self._process_node_schema(normalized_label, config)
            
            elif config.get("represented_as") == "edge":
                if isinstance(normalized_label, list):
                    for nl in normalized_label:
                        self._process_edge_schema(nl, config)
                else:
                    self._process_edge_schema(normalized_label, config)

    def _process_node_schema(self, label, config):
        """Extract valid properties from node schema including inherited properties"""
        # Required node properties
        self.node_schema_properties[label].update(['id'])
        
        # Add properties defined in schema
        if 'properties' in config:
            props = config['properties']
            if isinstance(props, list):
                self.node_schema_properties[label].update(props)
            elif isinstance(props, dict):
                self.node_schema_properties[label].update(props.keys())
        
        # Add KGX properties if defined
        kgx_props = config.get('kgx_properties', {})
        if kgx_props:
            self.node_schema_properties[label].update(kgx_props.keys())
        
        # Handle inherited properties if inherit_properties is True
        if config.get('inherit_properties', False):
            self._add_inherited_properties(label, config)
    def _process_edge_schema(self, label, config):
        """Extract valid properties from edge schema including KGX properties"""
        # Required edge properties
        self.edge_schema_properties[label].update([
            'id', 'subject', 'object', 'label', 'source_type', 'target_type'
        ])
        
        # Add predicate if defined in schema
        if 'biolink_predicate' in config:
            self.edge_schema_properties[label].add('predicate')

        # Add properties defined in schema
        if 'properties' in config:
            props = config['properties']
            if isinstance(props, list):
                self.edge_schema_properties[label].update(props)
            elif isinstance(props, dict):
                self.edge_schema_properties[label].update(props.keys())
        
        # Add KGX properties if defined
        kgx_props = config.get('kgx_properties', {})
        if kgx_props:
            self.edge_schema_properties[label].update(kgx_props.keys())

    def _add_inherited_properties(self, label, config):
        """Recursively add properties from parent classes"""
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        
        # Get parent classes from is_a
        parent_classes = config.get('is_a', [])
        if not isinstance(parent_classes, list):
            parent_classes = [parent_classes]
        
        for parent in parent_classes:
            # Find parent config in schema
            parent_config = None
            for k, v in schema.items():
                if v.get('input_label') == parent or k == parent:
                    parent_config = v
                    break
            
            if parent_config:
                # Add parent's properties
                if 'properties' in parent_config:
                    props = parent_config['properties']
                    if isinstance(props, list):
                        self.node_schema_properties[label].update(props)
                    elif isinstance(props, dict):
                        self.node_schema_properties[label].update(props.keys())
                
                # Recursively inherit from parent's parents
                if parent_config.get('inherit_properties', False):
                    self._add_inherited_properties(label, parent_config)

    def _normalize_label(self, label):
        if not label:
            return label
        if isinstance(label, list):
            return [l.lower().replace(" ", "_") for l in label]
        return label.lower().replace(" ", "_")

    def _validate_node_properties(self, label, properties):
        """Validate node properties against schema and return only valid ones"""
        normalized_label = self._normalize_label(label)
        if isinstance(normalized_label, list):
            normalized_label = normalized_label[0]
        
        valid_properties = {}
        schema_props = self.node_schema_properties.get(normalized_label, set())
        
        for prop, value in properties.items():
            if prop in schema_props:
                valid_properties[prop] = value
        
        return valid_properties

    def _validate_edge_properties(self, label, properties):
        """Validate edge properties against schema and return only valid ones"""
        normalized_label = self._normalize_label(label)
        if isinstance(normalized_label, list):
            normalized_label = normalized_label[0]
        
        valid_properties = {}
        schema_props = self.edge_schema_properties.get(normalized_label, set())
        
        for prop, value in properties.items():
            if prop in schema_props:
                valid_properties[prop] = value
        
        return valid_properties

    def preprocess_value(self, value):
        value_type = type(value)
        if value_type is list:
            return json.dumps([self.preprocess_value(item) for item in value])
        if value_type is str:
            return value.translate(self.translation_table)
        return value

    def preprocess_id(self, prev_id):
      """Ensure ID remains in CURIE format while cleaning special characters"""
      if ':' in prev_id:
          prefix, local_id = prev_id.split(':', 1)
          # Standardize prefix to uppercase
          prefix = prefix.upper()
          # Clean local ID (remove duplicate prefix if present)
          clean_local = local_id.lower().replace(f"{prefix.lower()}_", "")
          clean_local = clean_local.strip().translate(str.maketrans({' ': '_'}))
          return f"{prefix}:{clean_local}"
      return prev_id.lower().strip().translate(str.maketrans({' ': '_', ':': '_'}))

    def _write_buffer_to_temp(self, label_or_key, buffer):
        if buffer and label_or_key in self._temp_files:
            with open(self._temp_files[label_or_key], 'a') as f:
                for entry in buffer:
                    json.dump(entry, f)
                    f.write('\n')
            buffer.clear()

    def _init_node_writer(self, label, properties, path_prefix=None, adapter_name=None):
        output_dir = self.get_output_path(path_prefix, adapter_name)
        self._node_headers[label].update(properties.keys())
        
        if label not in self._temp_files:
            temp_file_path = output_dir / f"temp_nodes_{label}.jsonl"
            if temp_file_path.exists():
                temp_file_path.unlink()
            self._temp_files[label] = temp_file_path
        return label

    def _init_edge_writer(self, label, source_type, target_type, properties, path_prefix=None, adapter_name=None):
        output_dir = self.get_output_path(path_prefix, adapter_name)
        key = (label, source_type, target_type)
        self._edge_headers[key].update(properties.keys())
        
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
                node_id, label, properties = node
                normalized_label = self._normalize_label(label)
                node_freq[normalized_label] += 1
                
                # Get node info from schema
                node_info = self.node_configs.get(normalized_label, {})
                node_config = node_info.get("config", {})
                output_label = node_info.get("output_label", normalized_label)
                
                # Validate properties against schema
                validated_props = self._validate_node_properties(normalized_label, properties)
                
                # Create base node data with required properties
                node_data = {
                    'id': self.preprocess_id(node_id),
                    'label': output_label,
                    **validated_props
                }
                
                # Get KGX properties from schema
                kgx_props = node_config.get('kgx_properties', {})
                
                # Merge in all KGX properties from schema
                for kgx_key, kgx_value in kgx_props.items():
                    if kgx_key not in node_data:  # Don't overwrite existing properties
                        node_data[kgx_key] = kgx_value
                
                # Ensure category is set (from kgx_properties or fallback to label)
                if 'category' not in node_data:
                    node_data['category'] = output_label
                
                writer_key = self._init_node_writer(output_label, node_data, path_prefix, adapter_name)
                self.temp_buffer[output_label].append(node_data)
                
                if len(self.temp_buffer[output_label]) >= self.batch_size:
                    self._write_buffer_to_temp(output_label, self.temp_buffer[output_label])
            
            for label in list(self.temp_buffer.keys()):
                self._write_buffer_to_temp(label, self.temp_buffer[label])
            
            for label in self._node_headers.keys():
                csv_file_path = output_dir / f"{label}_nodes.csv"
                cypher_file_path = output_dir / f"{label}_nodes.cypher"
                
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
            self._cleanup_temp_files()
                
        return node_freq, self._node_headers

    def write_edges(self, edges, path_prefix=None, adapter_name=None):
        """
        Write edges to CSV/Neo4j Cypher files, supporting multiple source and target types.
        """
        self.temp_buffer.clear()
        self._temp_files.clear()
        self._edge_headers.clear()
        edge_freq = defaultdict(int)
        output_dir = self.get_output_path(path_prefix, adapter_name)

        try:
            for edge in edges:
                source_id, target_id, label, properties = edge
                normalized_label = self._normalize_label(label)
                edge_freq[normalized_label] += 1

                # Get edge info from schema
                edge_info = self.edge_node_types.get(normalized_label, {})
                edge_config = self.edge_configs.get(normalized_label, {})
                
                source_types = edge_info.get("source", [])
                target_types = edge_info.get("target", [])
                if not isinstance(source_types, list):
                    source_types = [source_types]
                if not isinstance(target_types, list):
                    target_types = [target_types]

                edge_label = edge_info.get("output_label", normalized_label)
                kgx_props = edge_config.get('kgx_properties', {})
                validated_props = self._validate_edge_properties(normalized_label, properties)
                edge_id = properties.get('id', f"{source_id}_{edge_label}_{target_id}")

                # Generate edges for all source/target type combinations
                for src_type in source_types:
                    for tgt_type in target_types:
                        src_type_final = src_type
                        tgt_type_final = tgt_type

                        if src_type == "ontology_term":
                            src_type_final = self.preprocess_id(source_id).split('_')[0]
                        if tgt_type == "ontology_term":
                            tgt_type_final = self.preprocess_id(target_id).split('_')[0]

                        edge_data = {
                            'id': self.preprocess_id(edge_id),
                            'subject': self.preprocess_id(source_id),
                            'object': self.preprocess_id(target_id),
                            'label': edge_label,
                            'source_type': src_type_final,
                            'target_type': tgt_type_final,
                            **validated_props
                        }

                        # Add predicate if defined in schema
                        if 'predicate' in self.edge_schema_properties.get(normalized_label, set()):
                            edge_data['predicate'] = edge_config.get('biolink_predicate')

                        # Merge KGX properties from schema
                        for kgx_key, kgx_value in kgx_props.items():
                            if kgx_key not in edge_data:
                                edge_data[kgx_key] = kgx_value

                        writer_key = self._init_edge_writer(edge_label, src_type_final, tgt_type_final, edge_data, path_prefix, adapter_name)
                        self.temp_buffer[writer_key].append(edge_data)

                        if len(self.temp_buffer[writer_key]) >= self.batch_size:
                            self._write_buffer_to_temp(writer_key, self.temp_buffer[writer_key])

            # Flush remaining buffers
            for key in list(self.temp_buffer.keys()):
                self._write_buffer_to_temp(key, self.temp_buffer[key])

            # Write CSV and Cypher files
            for key in self._edge_headers.keys():
                input_label, source_type, target_type = key
                edge_label = self.edge_node_types.get(input_label, {}).get("output_label", input_label)
                file_suffix = f"{input_label}_{source_type}_{target_type}".lower()
                csv_file_path = output_dir / f"{file_suffix}_edges.csv"
                cypher_file_path = output_dir / f"{file_suffix}_edges.cypher"

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

                self.write_edge_cypher(edge_label, source_type, target_type, csv_file_path, cypher_file_path)

                if key in self._temp_files and self._temp_files[key].exists():
                    self._temp_files[key].unlink()

        finally:
            self._cleanup_temp_files()

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
CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:{edge_label}]-() REQUIRE r.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MATCH (source:{source_type} {{id: row.subject}})
    MATCH (target:{target_type} {{id: row.object}})
    MERGE (source)-[r:{edge_label} {{id: row.id}}]->(target)
    SET r += apoc.map.removeKeys(row, ['id', 'subject', 'object', 'label', 'source_type', 'target_type'])",
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

    def _cleanup_temp_files(self):
        self.temp_buffer.clear()
        for temp_file in self._temp_files.values():
            if isinstance(temp_file, Path) and temp_file.exists():
                temp_file.unlink()
        self._temp_files.clear()