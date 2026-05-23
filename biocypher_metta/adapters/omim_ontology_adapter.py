import os
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class OMIMOntologyAdapter(OntologyAdapter):
    @classmethod
    def _get_ontologies(cls):
        api_key = os.getenv('BIOPORTAL_API_KEY')
        if not api_key:
            raise EnvironmentError(
                "BIOPORTAL_API_KEY environment variable is not set. "
                "Please add it to your .env file to download the OMIM ontology."
            )
        return {
            'omim': f'https://data.bioontology.org/ontologies/OMIM/submissions/29/download?apikey={api_key}'
        }

    def __init__(self, write_properties, add_provenance, ontology, type, label='disease', dry_run=False, add_description=False, cache_dir=None):
        self.ONTOLOGIES = self._get_ontologies()
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        """
        Returns the source and source URL for OMIM.
        """
        return 'OMIM', 'https://bioportal.bioontology.org/ontologies/OMIM'

    def get_uri_prefixes(self):
        """Define URI prefixes for OMIM."""
        return {
            'primary': 'http://purl.bioontology.org/ontology/OMIM/'
        }

    @classmethod
    def to_key(cls, node_uri):
        key = super().to_key(node_uri)
        if key is not None and key.isdigit():
            return f"OMIM:{key}"
        # handle case where to_key already prefixed it with number_
        if key is not None and key.startswith('number_'):
            return f"OMIM:{key[7:]}"
        return key
