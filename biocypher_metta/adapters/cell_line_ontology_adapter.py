from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class CellLineOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'clo': 'http://purl.obolibrary.org/obo/clo.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='clo', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        return 'Cell Line Ontology', 'http://purl.obolibrary.org/obo/clo.owl'

    def get_uri_prefixes(self):
        return {
            'primary': 'http://purl.obolibrary.org/obo/CLO_'
        }

    def get_nodes(self):
        for term_id, label, props in super().get_nodes():
            if self.write_properties and self.add_description and 'description' in props:
                props['description'] = props['description'].replace('"', '')
            yield term_id, label, props