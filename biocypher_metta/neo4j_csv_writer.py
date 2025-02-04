import json
import csv
import gc
from collections import Counter, defaultdict
from pathlib import Path
from biocypher._logger import logger
import rdflib

from biocypher_metta import BaseWriter

class Neo4jCSVWriter(BaseWriter):
    def __init__(self, schema_config, biocypher_config, output_dir):
        super().__init__(schema_config, biocypher_config, output_dir)
        self.csv_delimiter = '|'
        self.array_delimiter = ';'
        self.translation_table = str.maketrans({'|': '', ';': ' ', "'": "", '"': ""})
        self.ontologies = {'go', 'bto', 'efo', 'cl', 'clo', 'uberon'}
        self.edge_node_types = self.create_edge_types()

    def create_edge_types(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        edge_types = {}
        for k, v in schema.items():
            if v["represented_as"] == "edge":
                label = self.convert_input_labels(k).lower()
                source_type = self.convert_input_labels(v.get("source", '')).lower()
                target_type = self.convert_input_labels(v.get("target", '')).lower()
                output_label = v.get("output_label", '').lower() if v.get("output_label") else None
                edge_types[label] = {
                    "source": source_type,
                    "target": target_type,
                    "output_label": output_label,
                }
        return edge_types

    def preprocess_value(self, value):
        if isinstance(value, list):
            return json.dumps([self.preprocess_value(item) for item in value])
        if isinstance(value, (rdflib.term.Literal, str)):
            return str(value).translate(self.translation_table)
        return value

    def preprocess_id(self, prev_id):
        return prev_id.lower().strip().translate(str.maketrans({' ': '_', ':': '_'}))

    def write_chunk(self, chunk, headers, file_path):
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=self.csv_delimiter)
            writer.writerows([self.preprocess_value(row.get(header, '')) for header in headers] for row in chunk)

    def write_to_csv(self, data_generator, file_path, headers, chunk_size=1000):
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=self.csv_delimiter)
            writer.writerow(headers)

        for chunk in self.chunk_generator(data_generator, chunk_size):
            self.write_chunk(chunk, headers, file_path)
            gc.collect()

    @staticmethod
    def chunk_generator(data, chunk_size):
        chunk = []
        for item in data:
            chunk.append(item)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def write_nodes(self, nodes, path_prefix=None, adapter_name=None):
        output_dir = self.get_output_dir(path_prefix, adapter_name)
        node_groups = defaultdict(list)
        node_freq = Counter()
        node_props = defaultdict(set)

        for node in nodes:
            node_id, label, properties = node
            label = label.split(".")[-1].lower()
            node_id = self.preprocess_id(node_id)
            node_freq[label] += 1
            node_props[label].update(properties.keys())
            node_groups[label].append({'id': node_id, 'label': label, **properties})

        for label, node_data in node_groups.items():
            csv_file_path = output_dir / f"nodes_{label}.csv"
            cypher_file_path = output_dir / f"nodes_{label}.cypher"
            headers = ['id'] + sorted(set().union(*(d.keys() for d in node_data)) - {'id'})

            self.write_to_csv(iter(node_data), csv_file_path, headers)
            self.generate_cypher_query(label, csv_file_path, cypher_file_path, edge=False)

        logger.info(f"Finished writing nodes to: {output_dir}")
        return node_freq, node_props

    def write_edges(self, edges, path_prefix=None, adapter_name=None):
        output_dir = self.get_output_dir(path_prefix, adapter_name)
        edge_groups = defaultdict(list)
        edges_freq = Counter()

        for edge in edges:
            source_id, target_id, label, properties = edge
            label = label.lower()
            edges_freq[label] += 1
            source_type = self.edge_node_types[label]["source"]
            target_type = self.edge_node_types[label]["target"]
            output_label = self.edge_node_types[label]["output_label"] or label

            edge_groups[(label, source_type, target_type)].append({
                'source_type': source_type,
                'source_id': self.preprocess_id(source_id),
                'target_type': target_type,
                'target_id': self.preprocess_id(target_id),
                'label': output_label,
                **properties
            })

        for (label, source_type, target_type), edge_data in edge_groups.items():
            file_suffix = f"{label}_{source_type}_{target_type}".lower()
            csv_file_path = output_dir / f"edges_{file_suffix}.csv"
            cypher_file_path = output_dir / f"edges_{file_suffix}.cypher"
            headers = sorted(set().union(*(d.keys() for d in edge_data)))
            self.write_to_csv(iter(edge_data), csv_file_path, headers)
            self.generate_cypher_query(label, csv_file_path, cypher_file_path, edge=True)

        logger.info(f"Finished writing edges to: {output_dir}")
        return edges_freq

    def generate_cypher_query(self, label, csv_file_path, cypher_file_path, edge=False):
        absolute_path = csv_file_path.resolve().as_posix()
        with open(cypher_file_path, 'w') as f:
            if edge:
                cypher_query = f"""
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MATCH (source:{self.edge_node_types[label]['source']} {{id: row.source_id}})
     MATCH (target:{self.edge_node_types[label]['target']} {{id: row.target_id}})
     MERGE (source)-[r:{label}]->(target)
     SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {{batchSize:1000}}
)
YIELD batches, total
RETURN batches, total;
                """
            else:
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
            f.write(cypher_query)

    def get_output_dir(self, path_prefix, adapter_name):
        output_dir = self.output_path / (path_prefix or adapter_name or '')
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir