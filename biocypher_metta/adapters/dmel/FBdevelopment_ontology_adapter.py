from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class FBDevelopmentOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'FBdv': 'https://purl.obolibrary.org/obo/fbdv.owl'
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='FBdv', dry_run=False, add_description=False, cache_dir=None):
        super(FBDevelopmentOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Flybase development ontology (FBdv#).
        """
        return 'Flybase Development Ontology', 'https://purl.obolibrary.org/obo/fbdv.owl'
