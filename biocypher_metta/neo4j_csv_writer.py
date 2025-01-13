import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from itertools import islice
from biocypher._logger import logger
import rdflib


class Neo4jCSVWriter:
    def __init__(self, schema_config, biocypher_config, output_dir):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = Path(output_dir)
        self.csv_delimiter = '|'
        self.array_delimiter = ';'
        self.chunk_size = 1000

        self.translation_table = str.maketrans({
            self.csv_delimiter: '',
            self.array_delimiter: ' ',
            "'": "",
            '"': ""
        })
        self.ontologies = {'go', 'bto', 'efo', 'cl', 'clo', 'uberon'}
        self.edge_node_types = self.create_edge_types()

    def create_edge_types(self):
        """Creates edge types from schema."""
        schema = self.schema_config.get("edges", {})
        edge_node_types = {}
        for label, config in schema.items():
            if config["represented_as"] == "edge":
                label = label.lower().replace(" ", "_")
                edge_node_types[label] = {
                    "source": config["source"].lower().replace(" ", "_"),
                    "target": config["target"].lower().replace(" ", "_"),
                    "output_label": config.get("output_label", "").lower().replace(" ", "_"),
                }
        return edge_node_types

    def preprocess_value(self, value):
        """Preprocesses a value for CSV writing."""
        if isinstance(value, list):
            return json.dumps([self.preprocess_value(v) for v in value])
        if isinstance(value, rdflib.term.Literal):
            return str(value).translate(self.translation_table)
        if isinstance(value, str):
            return value.translate(self.translation_table)
        return value

    def preprocess_id(self, identifier):
        """Preprocesses an identifier."""
        return identifier.lower().strip().replace(" ", "_").replace(":", "_")

    def write_chunk(self, chunk, headers, file_path):
        """Writes a chunk of data to a CSV file."""
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=self.csv_delimiter)
            for row in chunk:
                writer.writerow({header: self.preprocess_value(row.get(header, "")) for header in headers})

    def write_to_csv(self, data, file_path):
        """Writes data to a CSV file in chunks."""
        headers = sorted({key for row in data for key in row.keys()})
        if 'id' in headers:
            headers.remove('id')
            headers.insert(0, 'id')  # Ensure 'id' is the first column

        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=self.csv_delimiter)
            writer.writeheader()

        iterator = iter(data)
        while True:
            chunk = list(islice(iterator, self.chunk_size))
            if not chunk:
                break
            self.write_chunk(chunk, headers, file_path)

    def write_nodes(self, nodes, path_prefix=None):
        """Processes and writes nodes to CSV."""
        output_dir = self.output_path / (path_prefix or "nodes")
        output_dir.mkdir(parents=True, exist_ok=True)

        node_groups = defaultdict(list)
        for node in nodes:
            node_id, label, properties = node
            label = label.split(".")[-1].lower()  # Standardize label
            node_id = self.preprocess_id(node_id)
            node_groups[label].append({'id': node_id, 'label': label, **properties})

        for label, group in node_groups.items():
            csv_path = output_dir / f"nodes_{label}.csv"
            self.write_to_csv(group, csv_path)

            cypher_path = output_dir / f"nodes_{label}.cypher"
            with open(cypher_path, 'w') as f:
                absolute_path = csv_path.resolve().as_posix()
                additional_label = ":ontology_term" if label in self.ontologies else ""
                f.write(f"""
CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE;

CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MERGE (n:{label}{additional_label} {{id: row.id}})
    SET n += apoc.map.removeKeys(row, ['id'])",
    {{batchSize:1000, parallel:true, concurrency:4}}
)
YIELD batches, total
RETURN batches, total;
                """)

    def write_edges(self, edges, path_prefix=None):
        """Processes and writes edges to CSV."""
        output_dir = self.output_path / (path_prefix or "edges")
        output_dir.mkdir(parents=True, exist_ok=True)

        edge_groups = defaultdict(list)
        for edge in edges:
            source_id, target_id, label, properties = edge
            label = label.lower()
            edge_data = {
                'source_id': self.preprocess_id(source_id),
                'target_id': self.preprocess_id(target_id),
                'label': label,
                **properties
            }
            edge_groups[label].append(edge_data)

        for label, group in edge_groups.items():
            csv_path = output_dir / f"edges_{label}.csv"
            self.write_to_csv(group, csv_path)

            cypher_path = output_dir / f"edges_{label}.cypher"
            with open(cypher_path, 'w') as f:
                absolute_path = csv_path.resolve().as_posix()
                f.write(f"""
CALL apoc.periodic.iterate(
    "LOAD CSV WITH HEADERS FROM 'file:///{absolute_path}' AS row FIELDTERMINATOR '{self.csv_delimiter}' RETURN row",
    "MATCH (source {{id: row.source_id}})
     MATCH (target {{id: row.target_id}})
     MERGE (source)-[r:{label}]->(target)
     SET r += apoc.map.removeKeys(row, ['source_id', 'target_id', 'label'])",
    {{batchSize:1000}}
)
YIELD batches, total
RETURN batches, total;
                """)


# Usage example
# writer = Neo4jCSVWriter(schema_config, biocypher_config, 'output')
# writer.write_nodes(nodes)
# writer.write_edges(edges)
