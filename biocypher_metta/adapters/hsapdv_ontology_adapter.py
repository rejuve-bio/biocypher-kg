from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class HsapDvOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'hsapdv': 'http://purl.obolibrary.org/obo/hsapdv.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='developmental_stage', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        """
        Returns the source and source URL for HsapDv.
        """
        return 'HsapDv', 'http://purl.obolibrary.org/obo/hsapdv.owl'

    def get_uri_prefixes(self):
        """Define URI prefixes for HsapDv."""
        return {
            'primary': 'http://purl.obolibrary.org/obo/HsapDv_'
        }
