from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter
class ExperimentalFactorOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'efo': 'http://www.ebi.ac.uk/efo/efo.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='efo', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        return 'Experimental Factor Ontology', 'http://www.ebi.ac.uk/efo/efo.owl'
    
    def get_uri_prefixes(self):
        return {
            'primary': 'http://www.ebi.ac.uk/efo/EFO_'
        }
        
    def get_nodes(self):
        self.update_graph()
        self.cache_node_properties()
        
        node_count = 0
        
        for term_id, label, props in super().get_nodes():
            if self.write_properties and self.add_description and 'description' in props:
                props['description'] = props['description'].replace('"', '')
                
            yield term_id, label, props
            
            node_count += 1
            if self.dry_run and node_count > 100:
                break