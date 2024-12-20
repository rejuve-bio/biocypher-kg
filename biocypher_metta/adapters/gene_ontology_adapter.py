import rdflib
import pickle
import os
import json
from datetime import datetime
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class GeneOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'go': 'http://purl.obolibrary.org/obo/go.owl'
    }
    
    # GO subontology types
    BIOLOGICAL_PROCESS = 'biological_process'
    MOLECULAR_FUNCTION = 'molecular_function'
    CELLULAR_COMPONENT = 'cellular_component'
    
    # Edge type mappings
    EDGE_LABELS = {
        'biological_process': 'biological_process_subclass_of',
        'molecular_function': 'molecular_function_subclass_of',
        'cellular_component': 'cellular_component_subclass_of'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label=None, 
                 dry_run=False, add_description=False, cache_dir=None):
        # Temporarily set dry_run to False for parent init since we'll handle it here
        super(GeneOntologyAdapter, self).__init__(write_properties, add_provenance, 
                                                 ontology, type, label, False,   
                                                 add_description, cache_dir)
        self.dry_run = dry_run
        self.mapping_file = os.path.join('aux_files/go_subontology_mapping.pkl')
        self.meta_file = os.path.join(self.cache_dir, 'go_meta.json')
        
        if type == 'node':
            self.current_subontology = label if label in [
                self.BIOLOGICAL_PROCESS,
                self.MOLECULAR_FUNCTION,
                self.CELLULAR_COMPONENT
            ] else None
        else:  # type == 'edge'
            for subonto, edge_label in self.EDGE_LABELS.items():
                if label == edge_label:
                    self.current_subontology = subonto
                    break
            else:
                self.current_subontology = None
                
        self.subontology_counter = 0
        self.subontology_mapping = self.load_or_update_mapping()

    def get_ontology_source(self):
        """Returns the source and source URL for the Gene Ontology."""
        return 'Gene Ontology', 'http://purl.obolibrary.org/obo/go.owl'

    def should_update_mapping(self):
        """Check if mapping file needs to be updated based on GO.owl timestamp"""
        if not os.path.exists(self.mapping_file):
            return True
            
        if not os.path.exists(self.meta_file):
            return True
            
        with open(self.meta_file, 'r') as f:
            meta = json.load(f)
            owl_date = datetime.strptime(meta['date'], "%Y-%m-%dT%H:%M:%S.%f")
            
        mapping_timestamp = datetime.fromtimestamp(os.path.getmtime(self.mapping_file))
        return mapping_timestamp < owl_date

    def find_go_nodes(self, graph):
        # subontologies are defined as namespaces
        nodes_in_namespaces = list(graph.subject_objects(predicate=OntologyAdapter.NAMESPACE))
        node_namespace_lookup = {}
        for n in nodes_in_namespaces:
            node = n[0]
            namespace = n[1]
            node_key = OntologyAdapter.to_key(node)
            node_namespace_lookup[node_key] = str(namespace)
        return node_namespace_lookup

    def create_subontology_mapping(self):
        """Create mapping of GO IDs to their subontologies"""
        self.update_graph()
        mapping = {}
        nodes_in_go_namespaces = self.find_go_nodes(self.graph)
        
        for node in self.graph.all_nodes():
            if not isinstance(node, rdflib.term.URIRef):
                continue
                
            if self.is_a_restriction_block(node) or self.is_deprecated(node):
                continue
                
            node_key = self._process_node_key(node)
            if node_key is None:
                continue
                
            namespace = nodes_in_go_namespaces.get(node_key)
            if namespace in [self.BIOLOGICAL_PROCESS, self.MOLECULAR_FUNCTION, self.CELLULAR_COMPONENT]:
                mapping[node_key] = namespace
                
        return mapping

    def load_or_update_mapping(self):
        """Load existing mapping or create new one if needed"""
        if self.should_update_mapping():
            mapping = self.create_subontology_mapping()
            os.makedirs(os.path.dirname(self.mapping_file), exist_ok=True)
            with open(self.mapping_file, 'wb') as f:
                pickle.dump(mapping, f)
            return mapping
            
        with open(self.mapping_file, 'rb') as f:
            return pickle.load(f)

    def get_subontology(self, go_id):
        """Get subontology for a given GO ID"""
        return self.subontology_mapping.get(go_id)

    def get_nodes(self):
        self.update_graph()
        self.cache_node_properties()
        
        if self.graph is not None and self.current_subontology:
            self.subontology_counter = 0
            processed_nodes = set()
            
            for node in self.graph.all_nodes():
                if not isinstance(node, rdflib.term.URIRef):
                    continue
                    
                if self.is_a_restriction_block(node):
                    continue

                node_key = self._process_node_key(node)
                if node_key is None or node_key in processed_nodes:
                    continue
                    
                if self.is_deprecated(node):
                    continue

                if self.subontology_mapping.get(node_key) != self.current_subontology:
                    continue

                if self.dry_run and self.subontology_counter >= 100:
                    break

                term_name = ', '.join(self.get_all_property_values_from_node(node, 'term_names'))
                synonyms = (self.get_all_property_values_from_node(node, 'related_synonyms') + 
                    self.get_all_property_values_from_node(node, 'exact_synonyms'))
                alternative_ids = self.get_alternative_ids(node)

                props = {}
                if self.write_properties:
                    props['term_name'] = term_name
                    if synonyms:
                        props['synonyms'] = synonyms
                    if alternative_ids:
                        props['alternative_ids'] = alternative_ids

                    if self.add_description:
                        description = ' '.join(self.get_all_property_values_from_node(node, 'descriptions'))
                        if description:
                            props['description'] = description

                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                processed_nodes.add(node_key)
                self.subontology_counter += 1
                yield node_key, self.label, props

    def get_edges(self):
        self.update_graph()
        self.cache_edge_properties()

        if self.graph is not None and self.current_subontology:
            edge_label = self.EDGE_LABELS[self.current_subontology]
            self.subontology_counter = 0

            for predicate in OntologyAdapter.PREDICATES:
                edges = self.graph.subject_objects(predicate=predicate, unique=True)
                
                for edge in edges:
                    from_node, to_node = edge

                    if self.is_blank(from_node):
                        continue

                    # Handle restriction blocks
                    if self.is_blank(to_node) and self.is_a_restriction_block(to_node):
                        restriction_predicate, restriction_node = self.read_restriction_block(to_node)
                        if restriction_predicate is None or restriction_node is None or self.is_blank(restriction_node):
                            continue
                        predicate = restriction_predicate
                        to_node = restriction_node

                    if self.is_blank(from_node) or self.is_blank(to_node):
                        continue

                    if self.is_deprecated(from_node) or self.is_deprecated(to_node):
                        continue

                    from_node_key = OntologyAdapter.to_key(from_node)
                    source_namespace = self.get_subontology(from_node_key)
                    
                    if source_namespace != self.current_subontology:
                        continue

                    if self.dry_run and self.subontology_counter >= 100:
                        return  # Stop generating edges for this subontology

                    to_node_key = OntologyAdapter.to_key(to_node)
                    predicate_key = OntologyAdapter.to_key(predicate)

                    if predicate == OntologyAdapter.DB_XREF:
                        if to_node.__class__ == rdflib.term.Literal:
                            if str(to_node) == str(from_node):
                                continue
                            if len(str(to_node).split(':')) != 2:
                                continue
                            to_node_key = str(to_node).replace(':', '_')
                            if from_node_key == to_node_key:
                                continue
                        else:
                            continue

                    predicate_name = self.predicate_name(predicate)
                    if predicate_name == 'dbxref':
                        continue

                    props = {}
                    if self.write_properties:
                        props['rel_type'] = predicate_name
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    self.subontology_counter += 1
                    yield from_node_key, to_node_key, edge_label, props