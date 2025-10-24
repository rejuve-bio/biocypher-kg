from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class HumanPhenotypeOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'hpo': 'http://purl.obolibrary.org/obo/hp.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology='hpo', type='node', label='hpo', 
                 dry_run=False, add_description=False, cache_dir=None): 
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, 
                        add_description, cache_dir)

    def get_ontology_source(self):
        return 'Human Phenotype Ontology', 'http://purl.obolibrary.org/obo/hp.owl'

    def get_uri_prefixes(self):
        return {
            'primary': 'http://purl.obolibrary.org/obo/HP_'
        }

    def get_nodes(self):
        for term_id, label, props in super().get_nodes():
            if self.write_properties and self.add_description and 'description' in props:
                props['description'] = props['description'].replace('"', '')
            yield term_id, label, props