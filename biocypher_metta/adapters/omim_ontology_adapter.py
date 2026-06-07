
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class OMIMOntologyAdapter(OntologyAdapter):

    ONTOLOGIES = {
        'omim': 'http://purl.obolibrary.org/obo/mondo/sources/omim.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='disease', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        """
        Return the ontology name and the URL of the OWL source.
        """
        return 'OMIM', 'http://purl.obolibrary.org/obo/mondo/sources/omim.owl'

    def get_uri_prefixes(self):
        """Define URI prefixes for OMIM."""
        return {
            'primary': 'https://omim.org/'
        }

    @classmethod
    def to_key(cls, node_uri):
        key = super().to_key(node_uri)
        if key is not None and key.isdigit():
            return f"OMIM:{key}"
        # handle case where to_key already prefixed it with number_
        if key is not None and key.startswith('number_'):
            return f"OMIM:{key[7:]}"
        if key is not None and key.startswith('PS') and key[2:].isdigit():
            return f"OMIM:{key}"
        return key
