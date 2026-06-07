# https://purl.obolibrary.org/obo/mi.owl
# Alternative (requires BIOPORTAL_API_KEY): https://data.bioontology.org/ontologies/MI/download?apikey=<BIOPORTAL_API_KEY>&download_format=rdf

from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class MolecularInteractionsOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'mi': 'https://purl.obolibrary.org/obo/mi.owl' 
    }
    
    def __init__(self, write_properties, add_provenance, ontology, type, label='molecular_interaction', dry_run=False, add_description=False, cache_dir=None):
        super(MolecularInteractionsOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
    
    def get_ontology_source(self):
        """
        Returns the source and source URL for molecular interactions ontology (MI).
        """
        return 'Molecular Interactions Ontology', 'https://purl.obolibrary.org/obo/mi.owl' 

    def get_uri_prefixes(self):
        """Define URI prefixes for MI Ontology."""
        return {
            'primary': 'http://purl.obolibrary.org/obo/MI_',
        }
        