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
        self.type_hierarchy = self._type_hierarchy()

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

    def _type_hierarchy(self):
        # to use Biolink-compatible schema
        # to not use  ontologies names but the ontologies types if their IDs occur  in edge's source/target
        return {
            'biolink:geneorgeneproduct': frozenset({'gene', 'transcript', 'protein', 'biolink:geneorgeneproduct', 'biolink_geneorgeneproduct'}),
            'biolink_geneorgeneproduct': frozenset({'gene', 'transcript', 'protein', 'biolink:geneorgeneproduct', 'biolink_geneorgeneproduct'}),
            'gene': frozenset({'gene'}),
            'transcript': frozenset({'transcript'}),
            'protein': frozenset({'protein'}),
            
            'ontology_term': frozenset({'ontology_term', 'anatomy', 'developmental_stage', 'cell_type', 'cell_line', 'small_molecule', 'experimental_factor', 'phenotype', 'disease', 'sequence_type', 'tissue', }),
            'anatomy': frozenset({'anatomy'}),
            'developmental_stage': frozenset({'developmental_stage'}),
            'cell_type': frozenset({'cell_type'}),
            'cell_line': frozenset({'cell_line'}),
            'experimental_factor': frozenset({'experimental_factor'}),
            'phenotype': frozenset({'phenotype'}),
            'disease': frozenset({'disease'}),
            'sequence_type': frozenset({'sequence_type'}),
            'small_molecule': frozenset({'small_molecule'}),
            'biological_process': frozenset({'biological_process'}),
            'molecular_function': frozenset({'molecular_function'}),
            'cellular_component': frozenset({'cellular_component'}),
            'tissue': frozenset({'tissue'}),
        }