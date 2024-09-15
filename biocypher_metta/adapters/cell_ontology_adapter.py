import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class CellOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'cl': 'http://purl.obolibrary.org/obo/cl.owl'
    }

    CAPABLE_OF = rdflib.term.URIRef('http://purl.obolibrary.org/obo/RO_0002215')
    PART_OF = rdflib.term.URIRef('http://purl.obolibrary.org/obo/BFO_0000050')
    CL_URI_PREFIX = 'http://purl.obolibrary.org/obo/CL_'
    GO_URI_PREFIX = 'http://purl.obolibrary.org/obo/GO_'
    UBERON_URI_PREFIX = 'http://purl.obolibrary.org/obo/UBERON_'

    def __init__(self, write_properties, add_provenance, ontology, type, label='cl', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
       
        
    def get_ontology_source(self):
        return 'Cell Ontology', 'http://purl.obolibrary.org/obo/cl.owl'

    def is_cl_term(self, uri):
        return str(uri).startswith(self.CL_URI_PREFIX)

    def is_go_term(self, uri):
        return str(uri).startswith(self.GO_URI_PREFIX)

    def is_uberon_term(self, uri):
        return str(uri).startswith(self.UBERON_URI_PREFIX)

    def get_nodes(self):
        self.update_graph()
        self.cache_node_properties()

        node_count = 0
        for node in self.graph.subjects(RDF.type, OWL.Class):
            if not self.is_cl_term(node):
                continue
            
            term_id = self.to_key(node)
            term_name = ', '.join(self.get_all_property_values_from_node(node, 'term_names'))
            synonyms = self.get_all_property_values_from_node(node, 'related_synonyms') + self.get_all_property_values_from_node(node, 'exact_synonyms')

            props = {}
            if self.write_properties:
                props['term_name'] = term_name
                props['synonyms'] = synonyms

                if self.add_description:
                    description = ' '.join(self.get_all_property_values_from_node(node, 'descriptions'))
                    # Remove quotation marks from the description
                    props['description'] = description.replace('"', '')

                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
            
            yield term_id, self.label, props

            node_count += 1
            if self.dry_run and node_count > 100:
                break

    def get_edges(self):
        if self.type != 'edge':
            return

        self.update_graph()
        self.cache_edge_properties()

        predicates = {
            'cl_subclass_of': RDFS.subClassOf,
            'cl_capable_of': self.CAPABLE_OF,
            'cl_part_of': self.PART_OF
        }

        if self.label not in predicates:
            return

        predicate = predicates[self.label]

        edge_count = 0
        for subject in self.graph.subjects(RDF.type, OWL.Class):
            if not self.is_cl_term(subject):
                continue

            for _, object_or_restriction in self.graph.predicate_objects(subject, predicate):
                object = self.resolve_object(object_or_restriction, predicate)
                if object is None:
                    continue

                if not self.is_valid_edge(subject, object, self.label):
                    continue

                from_node_key = self.to_key(subject)
                to_node_key = self.to_key(object)

                props = {}
                if self.write_properties:
                    props['rel_type'] = self.predicate_name(predicate)
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url

                yield from_node_key, to_node_key, self.label, props

                edge_count += 1
                if self.dry_run and edge_count > 100:
                    return

    def is_valid_edge(self, from_node, to_node, edge_type):
        if edge_type == 'cl_subclass_of':
            return self.is_cl_term(from_node) and self.is_cl_term(to_node)
        elif edge_type == 'cl_capable_of':
            return self.is_cl_term(from_node) and self.is_go_term(to_node)
        elif edge_type == 'cl_part_of':
            return self.is_cl_term(from_node) and self.is_uberon_term(to_node)
        return False

    def resolve_object(self, object_or_restriction, predicate):
        if isinstance(object_or_restriction, rdflib.term.BNode):
            restriction_type = self.graph.value(subject=object_or_restriction, predicate=RDF.type)
            if restriction_type == OWL.Restriction:
                on_property = self.graph.value(subject=object_or_restriction, predicate=OWL.onProperty)
                some_values_from = self.graph.value(subject=object_or_restriction, predicate=OWL.someValuesFrom)

                if on_property == predicate and some_values_from:
                    return some_values_from
        else:
            return object_or_restriction
        return None

    def predicate_name(self, predicate):
        predicate_str = str(predicate)
        if predicate_str == str(self.CAPABLE_OF):
            return 'capable_of'
        return super().predicate_name(predicate)