# https://purl.obolibrary.org/obo/so.owl


from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class SequenceOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'so': 'https://purl.obolibrary.org/obo/so.owl' 
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='so', dry_run=False, add_description=False, cache_dir=None):
        super(SequenceOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Sequence Ontology (SO).
        """
        return 'Sequence Ontology', 'https://purl.obolibrary.org/obo/so.owl' 
