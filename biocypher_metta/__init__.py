from biocypher import BioCypher
from collections import Counter, defaultdict
from abc import ABC, abstractmethod
import pathlib
import os


class BaseWriter(ABC):
    def __init__(self, schema_config, biocypher_config, output_dir, include_curie: bool = False):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = pathlib.Path(output_dir)
        self.include_curie = include_curie
        self.bcy = BioCypher(schema_config_path=schema_config,
                             biocypher_config_path=biocypher_config)
        if not os.path.exists(output_dir):
            self.output_path.mkdir(parents=True)
        self.ontology = self.bcy._get_ontology()

        self.node_freq = Counter()
        self.node_props = defaultdict(set)
        self.edge_freq = Counter()

        self.valid_node_labels = set()
        self.valid_edge_labels = set()
        self._get_valid_labels()

    def _get_valid_labels(self):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        for k, v in schema.items():
            input_label = v.get("input_label")
            if input_label:
                labels = [input_label] if isinstance(input_label, str) else input_label
                for label in labels:
                    normalized_label = self.normalize_label(label)
                    if v.get("represented_as") == "node":
                        self.valid_node_labels.add(normalized_label)
                    elif v.get("represented_as") == "edge":
                        self.valid_edge_labels.add(normalized_label)

    def normalize_label(self, label):
        if not label:
            return ""
        if "." in label:
            label = label.split(".")[-1]
        return label.lower().replace(" ", "_")

    def check_node_label(self, label):
        normalized_label = self.normalize_label(label)
        return normalized_label in self.valid_node_labels

    def check_edge_label(self, label):
        normalized_label = self.normalize_label(label)
        return normalized_label in self.valid_edge_labels

    @abstractmethod
    def write_nodes(self, nodes, path_prefix=None, create_dir=True):
        pass

    @abstractmethod
    def write_edges(self, edges, path_prefix=None, create_dir=True):
        pass

    def extract_node_info(self, node):
        id, label, properties = node
        self.node_freq[label] += 1
        self.node_props[label] = self.node_props[label].union(properties.keys())
    
    def extract_edge_info(self, edge):
        source_id, target_id, label, properties = edge
        self.edge_freq[label] += 1
    
    def clear_counts(self):
        self.node_freq.clear()
        self.node_props.clear()
        self.edge_freq.clear()