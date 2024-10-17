from biocypher import BioCypher
from collections import Counter, defaultdict
from abc import ABC, abstractmethod
import pathlib
import os


class BaseWriter(ABC):
    def __init__(self, schema_config, biocypher_config, output_dir):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = pathlib.Path(output_dir)
        self.bcy = BioCypher(schema_config_path=schema_config,
                             biocypher_config_path=biocypher_config)
        if not os.path.exists(output_dir):
            self.output_path.mkdir(parents=True)
        self.ontology = self.bcy._get_ontology()

        self.node_freq = Counter()
        self.node_props = defaultdict(set)
        self.edge_freq = Counter()

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