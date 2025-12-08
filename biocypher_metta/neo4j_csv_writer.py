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

        self.label_is_ontology = self._build_label_types_map()
        self.type_hierarchy = self._type_hierarchy()

        self.create_edge_types()
        self._node_writers = {}
        self._edge_writers = {}
        self._node_headers = defaultdict(set)
        self._edge_headers = defaultdict(set)
        self._temp_files = {}
        self.batch_size = 10000
        self.temp_buffer = defaultdict(list)


    def _build_label_types_map(self):
        """Build mapping of node labels to whether they are ontology terms."""
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        label_is_ontology = {}

        ontology_types = set()
        for schema_type, config in schema.items():
            if config.get("represented_as") == "node":
                is_a = config.get("is_a")
                normalized_schema_type = self.normalize_text(schema_type)
                if normalized_schema_type == "ontology_term":
                    ontology_types.add(normalized_schema_type)
                elif is_a:
                    parent_types = [is_a] if isinstance(is_a, str) else is_a
                    for parent in parent_types:
                        if self.normalize_text(parent) == "ontology_term":
                            ontology_types.add(normalized_schema_type)
                            break

        for schema_type, config in schema.items():
            if config.get("represented_as") == "node":
                input_label = config.get("input_label")

                if isinstance(input_label, list):
                    labels_to_process = input_label
                else:
                    labels_to_process = [input_label]

                normalized_schema_type = self.normalize_text(schema_type)
                is_ontology = normalized_schema_type in ontology_types

                for label in labels_to_process:
                    normalized_label = label.split(".")[-1] if "." in label else label
                    normalized_label = self.normalize_text(normalized_label)
                    label_is_ontology[normalized_label] = is_ontology
        return label_is_ontology

    def _is_ontology_label(self, label):
        normalized_label = self.normalize_text(label) if label else None
        return self.label_is_ontology.get(normalized_label, False)

    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type is not None and target_type is not None:
                    label = self.normalize_text(v["input_label"])
                    source_type_normalized = self.normalize_text(source_type)
                    target_type_normalized = self.normalize_text(target_type)
                
                    output_label = v.get("output_label", label)

                    if '.' not in k:
                        self.edge_node_types[label] = {
                            "source": source_type_normalized, 
                            "target": target_type_normalized,
                            "output_label": output_label
                        }


    def preprocess_value(self, value, key=None):
        value_type = type(value)
        if value_type is list:
            return json.dumps([self.preprocess_value(item, key) for item in value]).replace('\\"', '"')
        if value_type is rdflib.term.Literal:
            value = str(value).translate(self.translation_table)
        elif value_type is str:
            value = value.translate(self.translation_table)

        
        return value
    def normalize_text(self, label, replace_char="_", lowercase=True):
        if isinstance(label, list):
            labels = []
            for aLabel in label:
                processed = aLabel.replace(" ", replace_char)
                labels.append(processed.lower() if lowercase else processed)
            return labels
        processed = label.replace(" ", replace_char)
        return processed.lower() if lowercase else processed

    def preprocess_id(self, prev_id, label=None):
        """
        Clean ID, preserving ontology prefixes when the label represents an ontology term.
        """
        prev_id = str(prev_id)

        if ':' in prev_id:
            prefix, local_id = prev_id.split(':', 1)

            if label and self._is_ontology_label(label):
                clean_id = f"{prefix.strip().upper()}_{local_id.strip().replace(' ', '_').upper()}"
                return clean_id
            else:
                clean_local = local_id.strip().replace(' ', '_').upper()
                return clean_local

        return prev_id.strip().replace(' ', '_').upper()

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
        self._node_headers[label].add('id')
        
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
        self._edge_headers[key].update({'source_id', 'target_id', 'label', 'source_type', 'target_type'})
        
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
                self.extract_node_info(node)
                
                id, label, properties = node
                if "." in label:
                    label = label.split(".")[1]
                label = label.lower()
                node_freq[label] += 1
                
                writer_key = self._init_node_writer(label, properties, path_prefix, adapter_name)
                node_data = {'id': self.preprocess_id(id, label=label), **properties}
                self.temp_buffer[label].append(node_data)
                
                if len(self.temp_buffer[label]) >= self.batch_size:
                    self._write_buffer_to_temp(label, self.temp_buffer[label])
            
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
                                        writer.writerow({k: self.preprocess_value(v, k) for k, v in data.items()})
                                    chunk.clear()
                            
                            for data in chunk:
                                writer.writerow({k: self.preprocess_value(v, k) for k, v in data.items()})
                
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


    def _type_hierarchy(self):
        # to use Biolink-compatible schema
        # to not use  ontologies names but the ontologies types if their IDs occur  in edge's source/target
        return {
            'biolink:geneorgeneproduct': frozenset({'gene', 'transcript', 'protein'}),
            'gene': frozenset({'gene'}),
            'transcript': frozenset({'transcript'}),
            'protein': frozenset({'protein'}),
            'ontology_term': frozenset({'ontology_term', 'anatomy', 'developmental_stage', 'cell_type', 'cell_line', 'experimental_factor', 'phenotype', 'disease', 'sequence_type', 'chemical_substance'}),
            'anatomy': frozenset({'anatomy'}),
            'developmental_stage': frozenset({'developmental_stage'}),
            'cell_type': frozenset({'cell_type'}),
            'cell_line': frozenset({'cell_line'}),
            'experimental_factor': frozenset({'experimental_factor'}),
            'phenotype': frozenset({'phenotype'}),
            'disease': frozenset({'disease'}),
            'sequence_type': frozenset({'sequence_type'}),
            'chemical_substance': frozenset({'chemical_substance'}),
            'biological_process': frozenset({'biological_process'}),
            'molecular_function': frozenset({'molecular_function'}),
            'cellular_component': frozenset({'cellular_component'}),
        }

    def write_edges(self, edges, path_prefix=None, adapter_name=None):
        self.temp_buffer.clear()
        self._temp_files.clear()
        self._edge_headers.clear()
        edge_freq = defaultdict(int)
        output_dir = self.get_output_path(path_prefix, adapter_name)
        
        try:
            for edge in edges:
                # Extract edge info for counting (from BaseWriter)
                self.extract_edge_info(edge)
                
                source_id, target_id, label, properties = edge
                label = label.lower()
                
                edge_info = self.edge_node_types[label]
                
                if isinstance(source_id, tuple):
                    source_type = source_id[0]
                    if isinstance(edge_info["source"], list):
                        if source_type not in edge_info["source"]:
                            raise TypeError(f"Type '{source_type}' must be one of {edge_info['source']}")
                    else:
                        # if source_type != edge_info["source"]:
                        if source_type not in self.type_hierarchy:
                            raise TypeError(f"Type '{source_type}' must be '{edge_info['source']}'")
                    source_id = source_id[1]
                else:
                    if isinstance(edge_info["source"], list):
                        source_type = edge_info["source"][0]
                    else:
                        source_type = edge_info["source"]

                if isinstance(target_id, tuple):
                    target_type = target_id[0]
                    if isinstance(edge_info["target"], list):
                        if target_type not in edge_info["target"]:
                            raise TypeError(f"Type '{target_type}' must be one of {edge_info['target']}")
                    else:
                        # if target_type != edge_info["target"]:
                        if target_type not in self.type_hierarchy:
                            raise TypeError(f"Type '{target_type}' must be '{edge_info['target']}'")
                    target_id = target_id[1]
                else:
                    if isinstance(edge_info["target"], list):
                        target_type = edge_info["target"][0]
                    else:
                        target_type = edge_info["target"]

                if source_type == "ontology_term" and not isinstance(source_id, tuple):
                    source_type = self.preprocess_id(source_id, label=source_type).split('_')[0]
                if target_type == "ontology_term" and not isinstance(target_id, tuple):
                    target_type = self.preprocess_id(target_id, label=target_type).split('_')[0]
                    print(f'target id: {target_id}, target type: {target_type}')


                edge_freq[f"{label}|{source_type}|{target_type}"] += 1

                edge_label = edge_info.get("output_label") or label

                edge_data = {
                    'source_id': self.preprocess_id(source_id, label=source_type),
                    'target_id': self.preprocess_id(target_id, label=target_type),
                    'source_type': source_type,
                    'target_type': target_type,
                    'label': edge_label,
                    **properties
                }
                
                writer_key = self._init_edge_writer(label, source_type, target_type, properties, path_prefix, adapter_name)
                self.temp_buffer[writer_key].append(edge_data)
                
                if len(self.temp_buffer[writer_key]) >= self.batch_size:
                    self._write_buffer_to_temp(writer_key, self.temp_buffer[writer_key])
        
            for key in list(self.temp_buffer.keys()):
                self._write_buffer_to_temp(key, self.temp_buffer[key])
        
            for key in self._edge_headers.keys():
                input_label, source_type, target_type = key
                edge_label = self.edge_node_types[input_label].get("output_label") or input_label 
            
                file_suffix = f"{input_label}_{source_type}_{target_type}".lower()
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
                                        writer.writerow({k: self.preprocess_value(v, k) for k, v in data.items()})
                                    chunk.clear()
                        
                            for data in chunk:
                                writer.writerow({k: self.preprocess_value(v, k) for k, v in data.items()})
            
                self.write_edge_cypher(edge_label, source_type, target_type, csv_file_path, cypher_file_path)
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