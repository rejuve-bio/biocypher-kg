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
        """
        Returns the source and source URL for Cell Ontology.
        """
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
            
            if self.is_deprecated(node):
                print(f"Skipping deprecated node: {self.to_key(node)}")
                continue

            term_id = self.to_key(node)
            term_name = ', '.join(self.get_all_property_values_from_node(node, 'term_names'))
            synonyms = self.get_all_property_values_from_node(node, 'related_synonyms') + self.get_all_property_values_from_node(node, 'exact_synonyms')
            alternative_ids = self.get_alternative_ids(node)

            props = {}
            if self.write_properties:
                props['term_name'] = term_name
                if synonyms:
                    props['synonyms'] = synonyms
                if alternative_ids:
                    props['alternative_ids'] = alternative_ids

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

            if self.is_deprecated(subject):
                continue

            objects_to_process = []

            # For part_of and capable_of edges, we ONLY want to process restrictions
            if self.label in ['cl_part_of', 'cl_capable_of']:
                for _, subclass_restriction in self.graph.predicate_objects(subject, RDFS.subClassOf):
                    if isinstance(subclass_restriction, rdflib.term.BNode):
                        resolved = self.resolve_object(subclass_restriction, predicate)
                        if resolved:
                            objects_to_process.append(resolved)
                        
                equiv_class = self.graph.value(subject=subject, predicate=OWL.equivalentClass)
                if equiv_class:
                    resolved = self.resolve_object(equiv_class, predicate)
                    if resolved:
                        objects_to_process.append(resolved)
            else:
                objects_to_process.extend(self.graph.objects(subject, predicate))

            for object_or_restriction in set(objects_to_process):
                if object_or_restriction is None or self.is_deprecated(object_or_restriction):
                    continue

                if not self.is_valid_edge_with_predicate(subject, object_or_restriction, predicate, self.label):
                    continue

                from_node_key = self.to_key(subject)
                to_node_key = self.to_key(object_or_restriction)

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

    def is_valid_edge_with_predicate(self, from_node, to_node, predicate, edge_type):
        if predicate == RDFS.subClassOf:
            return self.is_cl_term(from_node) and self.is_cl_term(to_node)
    
        elif predicate == self.CAPABLE_OF:
            return (self.is_cl_term(from_node) and 
                    self.is_go_term(to_node))
    
        elif predicate == self.PART_OF:
            return (self.is_cl_term(from_node) and 
                    self.is_uberon_term(to_node))

    
        return False

    def resolve_object(self, object_or_restriction, predicate):
        if not isinstance(object_or_restriction, rdflib.term.BNode):
            return object_or_restriction

        restriction_type = self.graph.value(subject=object_or_restriction, predicate=RDF.type)
    
        if restriction_type == OWL.Restriction:
            on_property = self.graph.value(subject=object_or_restriction, predicate=OWL.onProperty)
            some_values_from = self.graph.value(subject=object_or_restriction, predicate=OWL.someValuesFrom)

            if on_property == predicate and some_values_from:
                return some_values_from

        intersection_list = self.graph.value(subject=object_or_restriction, predicate=OWL.intersectionOf)
        if intersection_list:
            for item in self.graph.items(intersection_list):
                resolved = self.resolve_object(item, predicate)
                if resolved and resolved != item:
                    return resolved

        return None

    def predicate_name(self, predicate):
        predicate_str = str(predicate)
        if predicate_str == str(self.CAPABLE_OF):
            return 'capable_of'
        return super().predicate_name(predicate)