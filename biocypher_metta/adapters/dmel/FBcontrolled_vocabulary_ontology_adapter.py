from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class FBControlledVocabularyOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'FBcv': 'https://purl.obolibrary.org/obo/fbcv.owl'
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='FBcv', dry_run=False, add_description=False, cache_dir=None):
        super(FBControlledVocabularyOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Flybase controlled vocabulary ontology (FBcv#).
        """
        return 'Flybase Controlled Vocabulary Ontology', 'https://purl.obolibrary.org/obo/fbcv.owl'
