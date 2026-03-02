import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from pathlib import Path

from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class BrendaTissueOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'bto': 'http://purl.obolibrary.org/obo/bto.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='tissue', dry_run=False, add_description=False, cache_dir=None):        
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        return 'BRENDA Tissue Ontology', 'http://purl.obolibrary.org/obo/bto.owl'

    def get_uri_prefixes(self):
        return {
            'primary': 'http://purl.obolibrary.org/obo/BTO_'
        }

    def get_nodes(self):
        for term_id, label, props in super().get_nodes():
            if self.write_properties and self.add_description and 'description' in props:
                props['description'] = props['description'].replace('"', '')
            yield term_id, label, props

    def get_edges(self):
        if self.type != 'edge':
            return

        # Preserve default behaviour for existing labels like `bto_subclass_of`.
        if self.label != 'tissue_part_of_anatomy':
            yield from super().get_edges()
            return

        # Note: This edge type is derived from UBERON structure + UBERON dbxrefs
        # to BTO. We intentionally avoid scanning the full BTO class set here.
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
                return

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

            bto_key = xref_str
            uberon_to_bto.setdefault(uberon_node, set()).add(bto_key)

        def resolve_part_of_targets(subject_uri: rdflib.term.URIRef):
            targets = []
            for _, restriction in uberon_graph.predicate_objects(subject_uri, RDFS.subClassOf):
                if not isinstance(restriction, rdflib.term.BNode):
                    continue
                restriction_type = uberon_graph.value(subject=restriction, predicate=RDF.type)
                if restriction_type != OWL.Restriction:
                    continue
                on_property = uberon_graph.value(subject=restriction, predicate=OWL.onProperty)
                if on_property != OntologyAdapter.PART_OF:
                    continue
                some_values_from = uberon_graph.value(subject=restriction, predicate=OWL.someValuesFrom)
                if isinstance(some_values_from, rdflib.term.URIRef):
                    targets.append(some_values_from)
            return targets

        seen = set()
        for uberon_subject, bto_keys in uberon_to_bto.items():
            for uberon_target in resolve_part_of_targets(uberon_subject):
                to_key = OntologyAdapter.to_key(uberon_target)
                if not to_key:
                    continue

                for bto_key in bto_keys:
                    pair = (bto_key, to_key)
                    if pair in seen:
                        continue
                    seen.add(pair)

                    props = {}
                    if self.write_properties:
                        props['rel_type'] = 'part_of'
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield bto_key, to_key, self.label, props