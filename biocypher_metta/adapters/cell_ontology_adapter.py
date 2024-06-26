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

    def __init__(self, write_properties, add_provenance, ontology, type, label='cl', dry_run=False):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run)
        self.edge_node_types = {
            'cl_subtype_of': {'source': 'CL', 'target': 'CL'},
            'capable_of': {'source': 'CL', 'target': 'GO'},
            'part_of': {'source': 'CL', 'target': 'UBERON'}
        }

    def get_ontology_source(self):
        return 'Cell Ontology', 'http://purl.obolibrary.org/obo/cl.owl'

    def is_cl_term(self, uri):
        return str(uri).startswith(self.CL_URI_PREFIX)

    def is_go_term(self, uri):
        return str(uri).startswith(self.GO_URI_PREFIX)

    def is_uberon_term(self, uri):
        return str(uri).startswith(self.UBERON_URI_PREFIX)

    def get_nodes(self):
        if self.type != 'node':
            return

        self.update_graph()
        self.cache_node_properties()

        node_count = 0

        for node in self.graph.subjects(RDF.type, OWL.Class):
            if not self.is_cl_term(node):
                continue
            
            term_id = self.to_key(node)
            term_name = ', '.join(self.get_all_property_values_from_node(node, 'term_names'))
            description = ' '.join(self.get_all_property_values_from_node(node, 'descriptions'))
            synonyms = self.get_all_property_values_from_node(node, 'related_synonyms') + self.get_all_property_values_from_node(node, 'exact_synonyms')

            # Check if the description contains double quotes
            if '"' in description:
                continue  # Skip this node

            props = {}
            if self.write_properties:
                props['term_name'] = term_name
                props['description'] = description
                props['synonyms'] = synonyms

                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
            
            yield term_id, self.label, props

            node_count += 1
            if self.dry_run and node_count >= 100:
                return 

        if self.dry_run:
            print(f"Dry run: Retrieved {node_count} nodes.")

    def get_edges(self):
        if self.type != 'edge':
            return

        self.update_graph()
        self.cache_edge_properties()

        predicates = {
            'cl_subtype_of': RDFS.subClassOf,
            'capable_of': self.CAPABLE_OF,
            'part_of': self.PART_OF
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

                if self.label == 'cl_subtype_of' and not self.is_cl_term(object):
                    continue
                elif self.label == 'capable_of' and not self.is_go_term(object):
                    continue
                elif self.label == 'part_of' and not self.is_uberon_term(object):
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
        if predicate_str == str(self.PART_OF):
            return 'part_of'
        return super().predicate_name(predicate)
