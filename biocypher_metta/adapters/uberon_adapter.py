from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class UberonAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'uberon': 'http://purl.obolibrary.org/obo/uberon.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='uberon', dry_run=False, add_description=False, cache_dir=None):
        super(UberonAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for UBERON ontology.
        """
        
        return 'UBERON', 'http://purl.obolibrary.org/obo/uberon.owl'

        
