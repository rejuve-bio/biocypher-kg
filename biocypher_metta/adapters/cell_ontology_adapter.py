import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from pathlib import Path
from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class CellOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'cl': 'http://purl.obolibrary.org/obo/cl.owl'
    }

    CAPABLE_OF = rdflib.term.URIRef('http://purl.obolibrary.org/obo/RO_0002215')
    PART_OF = rdflib.term.URIRef('http://purl.obolibrary.org/obo/BFO_0000050')

    def __init__(self, write_properties, add_provenance, ontology, type, label='cell_type', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)
       
    def get_ontology_source(self):
        return 'Cell Ontology', 'http://purl.obolibrary.org/obo/cl.owl'
    
    def get_uri_prefixes(self):
        """Define URI prefixes for Cell Ontology."""
        return {
            'primary': 'http://purl.obolibrary.org/obo/CL_',
            'go': 'http://purl.obolibrary.org/obo/GO_',
            'uberon': 'http://purl.obolibrary.org/obo/UBERON_',
            'chebi': 'http://purl.obolibrary.org/obo/CHEBI_',
        }
    
    def should_include_edge(self, from_node, to_node, predicate=None, edge_type=None):
        if predicate == RDFS.subClassOf:
            return (self.is_term_of_type(from_node, 'primary') and 
                    self.is_term_of_type(to_node, 'primary'))
    
        elif predicate == self.CAPABLE_OF:
            return (self.is_term_of_type(from_node, 'primary') and 
                    self.is_term_of_type(to_node, 'go'))
    
        elif predicate == self.PART_OF:
            return (self.is_term_of_type(from_node, 'primary') and 
                    self.is_term_of_type(to_node, 'uberon'))
        
        return super().should_include_edge(from_node, to_node, predicate, edge_type)

    def get_nodes(self):
        self.update_graph()
        self.cache_node_properties()

        node_count = 0
        for node in self.graph.subjects(RDF.type, OWL.Class):
            if not self.should_include_node(node):
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
                    props['synonym'] = synonyms
                if alternative_ids:
                    props['alternative_ids'] = alternative_ids

                if self.add_description:
                    description = ' '.join(self.get_all_property_values_from_node(node, 'descriptions'))
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
            'cl_part_of': self.PART_OF,
            'cell_type_part_of_tissue': self.PART_OF,
        }

        if self.label not in predicates:
            return

        predicate = predicates[self.label]

        uberon_to_bto = None
        if self.label == 'cell_type_part_of_tissue':
            uberon_to_bto = self._build_uberon_to_bto_map()

        edge_count = 0
        for subject in self.graph.subjects(RDF.type, OWL.Class):
            if not self.should_include_node(subject):
                continue

            if self.is_deprecated(subject):
                continue

            objects_to_process = []

            if self.label in ['cl_part_of', 'cl_capable_of', 'cell_type_part_of_tissue']:
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

                if not self.should_include_edge(subject, object_or_restriction, predicate, self.label):
                    continue

                from_node_key = self.to_key(subject)

                if self.label == 'cell_type_part_of_tissue':
                    uberon_key = self.to_key(object_or_restriction)
                    if not uberon_key or not uberon_to_bto:
                        continue
                    bto_targets = uberon_to_bto.get(uberon_key)
                    if not bto_targets:
                        continue

                    for bto_key in bto_targets:
                        props = {}
                        if self.write_properties:
                            props['rel_type'] = self.predicate_name(predicate)
                            if self.add_provenance:
                                props['source'] = self.source
                                props['source_url'] = self.source_url

                        yield from_node_key, bto_key, self.label, props

                    edge_count += 1
                    if self.dry_run and edge_count > 100:
                        return
                    continue

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

    def _build_uberon_to_bto_map(self):
        """Build a mapping UBERON term key -> set of BTO term keys.

        UBERON provides dbxrefs to BTO; we invert those xrefs.
        """
        uberon_graph = rdflib.Graph()

        cache_dir = Path(self.cache_dir) if self.cache_dir else None
        local_uberon = cache_dir / 'uberon.owl' if cache_dir else None

        parsed = False
        if local_uberon and local_uberon.exists():
            for fmt in ('xml', 'turtle', 'n3', 'nt'):
                try:
                    uberon_graph.parse(str(local_uberon), format=fmt)
                    parsed = True
                    break
                except Exception:
                    continue

        if not parsed:
            try:
                from biocypher_metta.adapters.uberon_adapter import UberonAdapter

                uberon_graph.parse(UberonAdapter.ONTOLOGIES['uberon'])
                parsed = True
            except Exception:
                return {}

        uberon_to_bto = {}
        for uberon_node, xref in uberon_graph.subject_objects(predicate=OntologyAdapter.DB_XREF, unique=True):
            if not isinstance(uberon_node, rdflib.term.URIRef):
                continue
            if not str(uberon_node).startswith('http://purl.obolibrary.org/obo/UBERON_'):
                continue
            if not isinstance(xref, rdflib.term.Literal):
                continue

            xref_str = str(xref).strip()
            if not xref_str:
                continue
            xref_str = xref_str.split()[0]
            if not xref_str.startswith('BTO:'):
                continue

            uberon_key = OntologyAdapter.to_key(uberon_node)
            if not uberon_key:
                continue
            bto_key = xref_str.replace('BTO:', 'BTO:', 1)

            uberon_to_bto.setdefault(uberon_key, set()).add(bto_key)

        return uberon_to_bto

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