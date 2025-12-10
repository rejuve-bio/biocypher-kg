from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class FBGrossAnatomyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'FBbt': 'https://purl.obolibrary.org/obo/fbbt.owl'     
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='anatomy', dry_run=False, add_description=False, cache_dir=None):
        super(FBGrossAnatomyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_uri_prefixes(self):
        """Define URI prefixes for FBbt Ontology."""
        return {
            'primary': 'http://purl.obolibrary.org/obo/FBbt_',
            'go': 'http://purl.obolibrary.org/obo/GO_',
            'uberon': 'http://purl.obolibrary.org/obo/UBERON_',
            'bfo': 'http://purl.obolibrary.org/obo/BFO_',
        }
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Flybase gross anatomy ontology (FBbt#).
        """
        return 'Flybase Gross Anatomy Ontology', 'https://purl.obolibrary.org/obo/fbbt.owl'
