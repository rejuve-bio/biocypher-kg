# Evidence & Conclusion Ontology (ECO)
# https://evidenceontology.org/
# Download: http://purl.obolibrary.org/obo/eco.owl

from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter


class EvidenceOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'eco': 'http://purl.obolibrary.org/obo/eco.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type,
                 label='evidence', dry_run=False, add_description=False,
                 cache_dir=None):
        super(EvidenceOntologyAdapter, self).__init__(
            write_properties, add_provenance, ontology, type, label,
            dry_run, add_description, cache_dir
        )

    def get_ontology_source(self):
        """Returns the source and source URL for the Evidence & Conclusion Ontology (ECO)."""
        return 'Evidence & Conclusion Ontology', 'http://purl.obolibrary.org/obo/eco.owl'

    def get_uri_prefixes(self):
        """Define URI prefixes for the Evidence & Conclusion Ontology."""
        return {
            'primary': 'http://purl.obolibrary.org/obo/ECO_',
        }
