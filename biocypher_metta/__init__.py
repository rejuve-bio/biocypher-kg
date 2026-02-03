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
        # to not use ontologies names but the ontologies types if their IDs occur in edge's source/target
        ontology_terms = frozenset({'ontology_term', 'anatomy', 'developmental_stage', 'cell_type', 'cell_line', 'small_molecule', 'experimental_factor', 'phenotype', 'disease', 'sequence_type', 'tissue', 'biological_process', 'molecular_function', 'cellular_component', 'chemical_substance', 'chemical_entity', 'compound'})
        
        gene_products = frozenset({'gene', 'transcript', 'protein', 'biolink:geneorgeneproduct', 'biolink_geneorgeneproduct'})
        
        return {
            'biolink:geneorgeneproduct': gene_products,
            'biolink_geneorgeneproduct': gene_products,
            'gene': frozenset({'gene'}),
            'transcript': frozenset({'transcript'}),
            'protein': frozenset({'protein'}),
            
            'ontology_term': ontology_terms,
            'anatomy': frozenset({'anatomy', 'cell_type', 'cell_line', 'tissue'}),
            'developmental_stage': frozenset({'developmental_stage'}),
            'cell_type': frozenset({'cell_type'}),
            'cell_line': frozenset({'cell_line'}),
            'experimental_factor': frozenset({'experimental_factor'}),
            'phenotype': frozenset({'phenotype'}),
            'disease': frozenset({'disease'}),
            'sequence_type': frozenset({'sequence_type'}),
            'small_molecule': frozenset({'small_molecule', 'chemical_substance', 'chemical_entity', 'compound'}),
            'chemical_substance': frozenset({'small_molecule', 'chemical_substance', 'chemical_entity', 'compound'}),
            'chemical_entity': frozenset({'small_molecule', 'chemical_substance', 'chemical_entity', 'compound'}),
            'biological_process': frozenset({'biological_process', 'ontology_term'}),
            'molecular_function': frozenset({'molecular_function', 'ontology_term'}),
            'cellular_component': frozenset({'cellular_component', 'ontology_term'}),
            'tissue': frozenset({'tissue', 'cell_type', 'cell_line'}),

            # Top level parents for robustness
            'biological_entity': ontology_terms.union(gene_products).union({'biological_entity', 'genomic_variant', 'snp', 'structural_variant', 'sequence_variant'}),
            'named_thing': ontology_terms.union(gene_products).union({'named_thing', 'biological_entity', 'genomic_variant', 'snp', 'structural_variant', 'sequence_variant', 'pathway', 'reaction'}),
            'genomic_variant': frozenset({'genomic_variant', 'snp', 'structural_variant', 'sequence_variant', 'polyphen2_variant'}),
        }