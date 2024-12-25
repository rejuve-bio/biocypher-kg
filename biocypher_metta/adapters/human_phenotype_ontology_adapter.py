from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter
from rdflib import URIRef

class HumanPhenotypeOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'hpo': 'http://purl.obolibrary.org/obo/hp.owl'
    }
    
    HP_URI_PREFIX = 'http://purl.obolibrary.org/obo/HP_'

    def __init__(self, write_properties, add_provenance, ontology='hpo', type='node', label='hpo', 
                 dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, 
                        add_description, cache_dir)

    def get_ontology_source(self):
        """
        Returns the source and source URL for Human Phenotype Ontology.
        """
        return 'Human Phenotype Ontology', 'http://purl.obolibrary.org/obo/hp.owl'

    def _process_node_key(self, node):
        if not isinstance(node, URIRef) or not str(node).startswith(self.HP_URI_PREFIX):
            return None
            
        return super()._process_node_key(node)

    def _is_valid_edge(self, from_node, to_node):
        return (isinstance(from_node, URIRef) and isinstance(to_node, URIRef) and
                str(from_node).startswith(self.HP_URI_PREFIX) and 
                str(to_node).startswith(self.HP_URI_PREFIX))

    def get_edges(self):
        self.update_graph()
        self.cache_edge_properties()

        i = 0  
        for predicate in OntologyAdapter.PREDICATES:
            for edge in self.graph.subject_objects(predicate=predicate, unique=True):
                if i > 100 and self.dry_run:
                    break

                from_node, to_node = edge

                if self.is_blank(from_node):
                    continue

                if self.is_blank(to_node) and self.is_a_restriction_block(to_node):
                    restriction_predicate, restriction_node = self.read_restriction_block(to_node)
                    if restriction_predicate is None or restriction_node is None or self.is_blank(restriction_node):
                        continue
                    
                    predicate = restriction_predicate
                    to_node = restriction_node

                if not self._is_valid_edge(from_node, to_node):
                    continue

                if self.is_deprecated(from_node) or self.is_deprecated(to_node):
                    continue

                from_node_key = self.to_key(from_node)
                to_node_key = self.to_key(to_node)
                
                props = {}
                if self.write_properties:
                    props['rel_type'] = self.predicate_name(predicate)
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield from_node_key, to_node_key, self.label, props
                i += 1