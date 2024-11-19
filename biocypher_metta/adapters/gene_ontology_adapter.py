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
        super(GeneOntologyAdapter, self).__init__(write_properties, add_provenance, 
                                                 ontology, type, label, dry_run, 
                                                 add_description, cache_dir)
        
        if type == 'node':
            self.current_subontology = label if label in [
                self.BIOLOGICAL_PROCESS,
                self.MOLECULAR_FUNCTION,
                self.CELLULAR_COMPONENT
            ] else None
        else:  # type == 'edge'
            # Extract the base subontology from the edge label
            for subonto, edge_label in self.EDGE_LABELS.items():
                if label == edge_label:
                    self.current_subontology = subonto
                    break
            else:
                self.current_subontology = None

    def get_ontology_source(self):
        """
        Returns the source and source URL for the Gene Ontology.
        """
        return 'Gene Ontology', 'http://purl.obolibrary.org/obo/go.owl'

    def find_go_nodes(self, graph):
        # subontologies are defined as `namespaces`
        nodes_in_namespaces = list(graph.subject_objects(predicate=OntologyAdapter.NAMESPACE))
        node_namespace_lookup = {}
        for n in nodes_in_namespaces:
            node = n[0]
            namespace = n[1]
            node_key = OntologyAdapter.to_key(node)
            node_namespace_lookup[node_key] = str(namespace)
        return node_namespace_lookup

    def get_nodes(self):
        nodes = list(super().get_nodes())
        if self.graph is not None and self.current_subontology:
            nodes_in_go_namespaces = self.find_go_nodes(self.graph)
            for node_id, label, props in nodes:
                namespace = nodes_in_go_namespaces.get(node_id, None)
                if namespace == self.current_subontology:
                    yield node_id, label, props

    def get_edges(self):
        edges = list(super().get_edges())
        if self.graph is not None and self.current_subontology:
            nodes_in_go_namespaces = self.find_go_nodes(self.graph)
            edge_label = self.EDGE_LABELS[self.current_subontology]
            
            for source, target, rel_type, props in edges:
                source_namespace = nodes_in_go_namespaces.get(source, None)
                # Only need to check source namespace since GO enforces subontology boundaries
                if source_namespace == self.current_subontology:
                    yield source, target, edge_label, props