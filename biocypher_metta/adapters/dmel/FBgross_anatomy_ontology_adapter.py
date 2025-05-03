from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class FBGrossAnatomyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'FBbt': 'https://purl.obolibrary.org/obo/fbbt.owl'     
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='FBbt', dry_run=False, add_description=False, cache_dir=None):
        super(FBGrossAnatomyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Flybase gross anatomy ontology (FBbt#).
        """
        return 'Flybase Gross Anatomy Ontology', 'https://purl.obolibrary.org/obo/fbbt.owl'
