from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter
import rdflib
from rdflib.namespace import RDF, RDFS, OWL

class ExperimentalFactorOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'efo': 'http://www.ebi.ac.uk/efo/efo.owl'
    }
    
    EFO_URI_PREFIX = 'http://www.ebi.ac.uk/efo/EFO_'

    def __init__(self, write_properties, add_provenance, ontology, type, label='efo', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        """
        Returns the source and source URL for the Experimental Factor Ontology.
        """
        return 'Experimental Factor Ontology', 'http://www.ebi.ac.uk/efo/efo.owl'
    
    def is_efo_term(self, uri):
        """
        Check if a URI represents an EFO term.
        """
        return str(uri).startswith(self.EFO_URI_PREFIX)

    def _process_node_key(self, node):
        """
        Override the node key processing to filter only EFO terms.
        """
        node_key = super()._process_node_key(node)
        
        if node_key is None or not self.is_efo_term(node):
            return None
            
        return node_key
        
    def get_nodes(self):
        """
        Filter only EFO terms from the ontology.
        """
        self.update_graph()
        self.cache_node_properties()
        
        node_count = 0
        
        for term_id, label, props in super().get_nodes():
            
            if self.write_properties and self.add_description and 'description' in props:
                props['description'] = props['description'].replace('"', '')
                
            yield term_id, label, props
            
            node_count += 1
            if self.dry_run and node_count > 100:
                break
    
    def get_edges(self):
        if self.type != 'edge':
            return

        self.update_graph()
        self.cache_edge_properties()

        edge_count = 0
        
        for subject in self.graph.subjects(RDF.type, OWL.Class):
            if not self.is_efo_term(subject):
                continue

            if self.is_deprecated(subject):
                continue

            for obj in self.graph.objects(subject, RDFS.subClassOf):
                if isinstance(obj, rdflib.term.BNode) or not self.is_efo_term(obj):
                    continue
                
                if self.is_deprecated(obj):
                    continue

                from_node_key = self.to_key(subject)
                to_node_key = self.to_key(obj)

                props = {}
                if self.write_properties:
                    props['rel_type'] = 'subclass'
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield from_node_key, to_node_key, self.label, props

                edge_count += 1
                if self.dry_run and edge_count > 100:
                    return