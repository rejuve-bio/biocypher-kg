
import os
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class DiseaseOntologyAdapter(OntologyAdapter):
    @classmethod
    def _get_ontologies(cls):
        api_key = os.getenv('BIOPORTAL_API_KEY')
        if not api_key:
            raise EnvironmentError(
                "BIOPORTAL_API_KEY environment variable is not set. "
                "Please add it to your .env file to download the Disease Ontology."
            )
        return {
            # 'do': 'https://purl.obolibrary.org/obo/do.owl'
            # Sometimes the above link doesn't respond. This is an alternative:
            'do': f'https://data.bioontology.org/ontologies/DOID/submissions/654/download?apikey={api_key}'
        }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='disease', dry_run=False, add_description=False, cache_dir=None):
        self.ONTOLOGIES = self._get_ontologies()
        super(DiseaseOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for Disease Ontology (DO).
        """
        return 'Disease Ontology', 'https://purl.obolibrary.org/obo/do.owl' 

    def get_uri_prefixes(self):
        """Define URI prefixes for Sequence Ontology."""
        return {
            'primary': 'http://purl.obolibrary.org/obo/DOID_',
            'clo': 'http://purl.obolibrary.org/obo/CL_',
        }                