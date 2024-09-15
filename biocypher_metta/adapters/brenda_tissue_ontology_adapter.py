from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter


class BrendaTissueOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'bto': 'http://purl.obolibrary.org/obo/bto.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='bto', dry_run=False, add_description=False, cache_dir=None):        
        super(BrendaTissueOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        """
        Returns the source and source URL for the BRENDA Tissue Ontology.
        """
        return 'BRENDA Tissue Ontology', 'http://purl.obolibrary.org/obo/bto.owl'

    def get_nodes(self):
        for term_id, label, props in super().get_nodes():
            if self.write_properties and self.add_description and 'description' in props:
                # Remove quotation marks from the description
                props['description'] = props['description'].replace('"', '')
            yield term_id, label, props