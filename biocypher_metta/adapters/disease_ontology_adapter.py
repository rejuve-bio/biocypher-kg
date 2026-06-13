
import os
from biocypher._logger import logger
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class DiseaseOntologyAdapter(OntologyAdapter):
    # Disease Ontology is fetched from OBOLibrary, which needs no API key. OBOLibrary
    # is occasionally unreachable, so if its load fails we fall back to BioPortal —
    # but only when a BIOPORTAL_API_KEY is available (e.g. set in your .env).
    OBO_URL = 'https://purl.obolibrary.org/obo/doid.owl'

    @classmethod
    def _get_ontologies(cls):
        return {'do': cls.OBO_URL}

    @staticmethod
    def _bioportal_url():
        """BioPortal DO download URL, or None when no API key is configured."""
        api_key = os.getenv('BIOPORTAL_API_KEY')
        if not api_key:
            return None
        return f'https://data.bioontology.org/ontologies/DOID/submissions/654/download?apikey={api_key}'

    def __init__(self, write_properties, add_provenance, ontology, type, label='disease', dry_run=False, add_description=False, cache_dir=None):
        self.ONTOLOGIES = self._get_ontologies()
        self._enabled = bool(self.ONTOLOGIES)
        super(DiseaseOntologyAdapter, self).__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def update_graph(self):
        """Load DO from OBOLibrary; if that fails, retry once via BioPortal (if a key is set)."""
        try:
            return super().update_graph()
        except Exception as obo_error:
            fallback_url = self._bioportal_url()
            # No key for a fallback, or we already switched to BioPortal — surface the error.
            if not fallback_url or self.ONTOLOGIES.get(self.ontology) == fallback_url:
                raise
            logger.warning(
                f"Loading Disease Ontology from OBOLibrary failed ({obo_error}); "
                "retrying via BioPortal (BIOPORTAL_API_KEY detected)."
            )
            self.ONTOLOGIES[self.ontology] = fallback_url
            return super().update_graph()

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
