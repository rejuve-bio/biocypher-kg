import json
import pathlib
import os
import csv
from biocypher import BioCypher
from biocypher._logger import logger
import networkx as nx
import rdflib
from io import StringIO
import multiprocessing as mp
from functools import lru_cache

class Neo4jCSVWriter:
    def __init__(self, schema_config, biocypher_config, output_dir):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = pathlib.Path(output_dir)
        self.csv_delimiter = '|'
        self.array_delimiter = ';'

        if not os.path.exists(output_dir):
            self.output_path.mkdir(parents=True, exist_ok=True)

        self.bcy = BioCypher(
            schema_config_path=schema_config, biocypher_config_path=biocypher_config
        )

        self.ontology = self.bcy._get_ontology()
        self.create_edge_types()

        self.excluded_properties = []
        self.translation_table = str.maketrans({self.csv_delimiter: '', 
                                                self.array_delimiter: ' ', 
                                                "'": "",
                                                '"': ""})
        self.ontologies = set(['go', 'bto', 'efo', 'cl', 'clo', 'uberon'])

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
                        source_type = self.convert_input_labels(source_type[0])
                        target_type = self.convert_input_labels(target_type[0])
                    else:
                        label = self.convert_input_labels(v["input_label"])
                        source_type = self.convert_input_labels(source_type)
                        target_type = self.convert_input_labels(target_type)
                    output_label = v.get("output_label", None)

                    self.edge_node_types[label.lower()] = {
                        "source": source_type.lower(),
                        "target": target_type.lower(),
                        "output_label": (
                            output_label.lower() if output_label is not None else None
                        ),
                    }
    
    def preprocess_value(self, value):
        value_type = type(value)
        
        if value_type is list:
            return json.dumps([self.preprocess_value(item) for item in value])
        
        if value_type is rdflib.term.Literal:
            return str(value).translate(self.translation_table)
        
        if value_type is str:
            return value.translate(self.translation_table)
        
        return value
    
    def preprocess_id(self, prev_id):
        replace_map = str.maketrans({' ': '_', ':':'_'})
        id = prev_id.lower().strip().translate(replace_map)
        return id
    
    def write_chunk(self, chunk, headers, file_path, csv_delimiter, preprocess_value):
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=csv_delimiter)
            for row in chunk:
                processed_row = [preprocess_value(row.get(header, '')) for header in headers]
                writer.writerow(processed_row)

    def write_to_csv(self, data, file_path, chunk_size=1000):
        headers = list(data[0].keys())
        
        # Write headers
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=self.csv_delimiter)
            writer.writerow(headers)
        
        # Process and write data in chunks
        num_processes = mp.cpu_count()
        pool = mp.Pool(processes=num_processes)
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            pool.apply_async(self.write_chunk, (chunk, headers, file_path, self.csv_delimiter, self.preprocess_value))
        
        pool.close()
        pool.join()

    def write_nodes(self, nodes, path_prefix=None, adapter_name=None):
        # Determine the output directory based on the given parameters
        if path_prefix:
            output_dir = self.output_path / path_prefix
        elif adapter_name:
            output_dir = self.output_path / adapter_name
        else:
            output_dir = self.output_path

        # Ensure the output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Prepare node data for CSV
        node_groups = {}
        for node in nodes:
            id, label, properties = node
            if "." in label:
                label = label.split(".")[1]
            label = label.lower()
            if label not in node_groups:
                node_groups[label] = []
            id = self.preprocess_id(id)
            node_groups[label].append({'id': id, 'label': label, **properties})

        # Write node data to CSV and generate Cypher queries
        for label, node_data in node_groups.items():
            csv_file_path = output_dir / f"nodes_{label}.csv"
            cypher_file_path = output_dir / f"nodes_{label}.cypher"
            self.write_to_csv(node_data, csv_file_path)

            # Generate Cypher query for loading nodes
            absolute_path = csv_file_path.resolve().as_posix()
            additional_label = ":ontology_term" if label in self.ontologies else ""
            with open(cypher_file_path, 'w') as f:
                cypher_query = f"""
CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MERGE (n:{label}{additional_label} {{id: row.id}})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {{batchSize:1000, parallel:true, concurrency:4}}
)
YIELD batches, total
RETURN batches, total;
                """
                f.write(cypher_query)
            # logger.info(f"Finished writing out node import queries for: {output_dir}, node type: {label}")

        logger.info(f"Finished writing out all node import queries for: {output_dir}")

    def write_edges(self, edges, path_prefix=None, adapter_name=None):
        # Determine the output directory based on the given parameters
        if path_prefix:
            output_dir = self.output_path / path_prefix
        elif adapter_name:
            output_dir = self.output_path / adapter_name
        else:
            output_dir = self.output_path

        # Ensure the output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Group edges by their label
        edge_groups = {}
        for edge in edges:
            source_id, target_id, label, properties = edge
            label = label.lower()
            source_type = self.edge_node_types[label]["source"]
            target_type = self.edge_node_types[label]["target"]
            if source_type == 'ontology_term':
                source_type = self.preprocess_id(source_id).split('_')[0]
            if target_type == 'ontology_term':
                target_type = self.preprocess_id(target_id).split('_')[0]
            output_label = self.edge_node_types[label]["output_label"]
            if output_label is None:
                output_label = label
            if label not in edge_groups:
                edge_groups[label] = []
            edge_groups[label].append({
                'source_type': source_type,
                'source_id': self.preprocess_id(source_id),
                'target_type': target_type,
                'target_id': self.preprocess_id(target_id),
                'label': output_label,
                **properties
            })

        # Process each edge type separately
        for label, edge_data in edge_groups.items():
            # File paths for CSV and Cypher files
            csv_file_path = output_dir / f"edges_{label}.csv"
            cypher_file_path = output_dir / f"edges_{label}.cypher"

            output_label = self.edge_node_types[label]["output_label"]
            if output_label is not None:
                label = output_label
            # Write edge data to CSV
            self.write_to_csv(edge_data, csv_file_path)

            # Generate Cypher query to load edges from the CSV file using the absolute path
            absolute_path = csv_file_path.resolve().as_posix()
            with open(cypher_file_path, 'w') as f:
                cypher_query = f"""
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MATCH (source:row.source_type {{id: row.source_id}})
    MATCH (target:row.target_type {{id: row.target_id}})
    MERGE (source)-[r:{label}]->(target)
    SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label', 'source_type', 'target_type'])",
    {{batchSize:1000, parallel:true, concurrency:4}}
)
YIELD batches, total
RETURN batches, total;
                """
                f.write(cypher_query)

            logger.info(f"Finished writing out edge import queries for: edge type: {label}")

        logger.info(f"Finished writing out all edge import queries for: {output_dir}")

    def convert_input_labels(self, label, replace_char="_"):
        return label.replace(" ", replace_char)

    def get_parent(self, G, node):
        return nx.dfs_preorder_nodes(G, node, depth_limit=2)

    def show_ontology_structure(self):
        self.bcy.show_ontology_structure()

    def summary(self):
        self.bcy.summary()

