
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class DiseaseOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        # 'do': 'https://purl.obolibrary.org/obo/do.owl' 
        # Sometimes the above link doesn't respond. This is an alternative:
        'do': 'https://data.bioontology.org/ontologies/DOID/submissions/654/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb'        
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='do', dry_run=False, add_description=False, cache_dir=None):
        super(DiseaseOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Disease Ontology (DO).
        """
        return 'Disease Ontology', 'https://purl.obolibrary.org/obo/do.owl' 
