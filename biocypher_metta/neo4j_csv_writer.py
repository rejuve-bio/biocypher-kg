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
        
        self.create_edge_types()
        self._node_writers = {}
        self._edge_writers = {}
        self._node_headers = defaultdict(set)
        self._edge_headers = defaultdict(set)
        self._temp_files = {}
        self.batch_size = 10000
        self.temp_buffer = defaultdict(list)

    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                edge_type = self.convert_input_labels(k)
                source_type = v.get("source", None)
                target_type = v.get("target", None)

                if source_type is not None and target_type is not None:
                    if isinstance(v["input_label"], list):
                        label = self.convert_input_labels(v["input_label"][0])
                        if isinstance(source_type, list):
                            source_type = [self.convert_input_labels(st) for st in source_type]
                        else:
                            source_type = self.convert_input_labels(source_type)
                        if isinstance(target_type, list):
                            target_type = [self.convert_input_labels(tt) for tt in target_type]
                        else:
                            target_type = self.convert_input_labels(target_type)
                    else:
                        label = self.convert_input_labels(v["input_label"])
                        if isinstance(source_type, list):
                            source_type = [self.convert_input_labels(st) for st in source_type]
                        else:
                            source_type = self.convert_input_labels(source_type)
                        if isinstance(target_type, list):
                            target_type = [self.convert_input_labels(tt) for tt in target_type]
                        else:
                            target_type = self.convert_input_labels(target_type)
                    
                    output_label = v.get("output_label", label)

                    # Handle different combinations of source/target types (single/list)
                    if isinstance(source_type, str) and isinstance(target_type, str):
                        if '.' not in k:                            
                            self.edge_node_types[label.lower()] = {
                                "source": source_type.lower(), 
                                "target": target_type.lower(),
                                "output_label": output_label.lower() if output_label is not None else None
                            }
                    
                    elif isinstance(source_type, list) and isinstance(target_type, str):
                        source_type_lower = [st.lower() for st in source_type]
                        self.edge_node_types[label.lower()] = {
                            "target": target_type.lower(),
                            "output_label": output_label.lower() if output_label is not None else None
                        }
                        self.edge_node_types[label.lower()]["source"] = source_type_lower
                        
                    elif isinstance(source_type, str) and isinstance(target_type, list):
                        target_type_lower = [tt.lower() for tt in target_type]
                        self.edge_node_types[label.lower()] = {
                            "source": source_type.lower(), 
                            "output_label": output_label.lower() if output_label is not None else None
                        } 
                        self.edge_node_types[label.lower()]["target"] = target_type_lower
                    
                    elif isinstance(source_type, list) and isinstance(target_type, list):
                        source_type_lower = [st.lower() for st in source_type]
                        target_type_lower = [tt.lower() for tt in target_type]
                        self.edge_node_types[label.lower()] = {
                            "output_label": output_label.lower() if output_label is not None else None
                        }
                        self.edge_node_types[label.lower()]["source"] = source_type_lower
                        self.edge_node_types[label.lower()]["target"] = target_type_lower
                    else:
                        print(f"UNKNOWN key type: => {k}")

    def preprocess_value(self, value):
        value_type = type(value)
        if value_type is list:
            return json.dumps([self.preprocess_value(item) for item in value]).replace('\\"', '"')
        if value_type is rdflib.term.Literal:
            return str(value).translate(self.translation_table)
        if value_type is str:
            return value.translate(self.translation_table)
        return value

    def convert_input_labels(self, label):
        if isinstance(label, list):
            labels = []
            for aLabel in label:
                labels.append(aLabel.replace(" ", "_"))
            return labels
        return label.lower().replace(" ", "_")

    def preprocess_id(self, prev_id):
        prev_id = str(prev_id)
        
        if ':' in prev_id:
            prefix, local_id = prev_id.split(':', 1)
            prefix = prefix.upper()
        
            if prefix.lower() in self.ontologies:
                clean_local = local_id.strip().translate(str.maketrans({' ': '_'}))
                result = f"{prefix}_{clean_local}"
                return result
            else:
                clean_local = local_id.lower().replace(f"{prefix.lower()}_", "")
                clean_local = clean_local.strip().translate(str.maketrans({' ': '_'}))
                result = clean_local
                return result
        
        result = prev_id.lower().strip().translate(str.maketrans({' ': '_', ':': '_'}))
        return result

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
                node_data = {'id': self.preprocess_id(id), **properties}
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
                        if source_type != edge_info["source"]:
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
                        if target_type != edge_info["target"]:
                            raise TypeError(f"Type '{target_type}' must be '{edge_info['target']}'")
                    target_id = target_id[1]
                else:
                    if isinstance(edge_info["target"], list):
                        target_type = edge_info["target"][0]
                    else:
                        target_type = edge_info["target"]

                if source_type == "ontology_term":
                    source_type = self.preprocess_id(source_id).split('_')[0]
                if target_type == "ontology_term":
                    target_type = self.preprocess_id(target_id).split('_')[0]
                
                edge_freq[f"{label}|{source_type}|{target_type}"] += 1
                          
                edge_label = edge_info.get("output_label") or label
                
                edge_data = {
                    'source_id': self.preprocess_id(source_id),
                    'target_id': self.preprocess_id(target_id),
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
                                        writer.writerow({k: self.preprocess_value(v) for k, v in data.items()})
                                    chunk.clear()
                        
                            for data in chunk:
                                writer.writerow({k: self.preprocess_value(v) for k, v in data.items()})
            
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