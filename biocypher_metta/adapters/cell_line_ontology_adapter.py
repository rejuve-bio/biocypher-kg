"""
PR Summary: Create Relations Between Ontology Types (#178)

This PR introduces three new cross-ontology relations to the BioCypher knowledge graph (human / hsa build):

- cell_line_is_a_cell_type
  Connects CLO cell lines to CL cell types using:
  * rdfs:subClassOf axioms in CLO
  * CLO database cross-references (hasDbXref) to CL terms

- cell_type_part_of_tissue
  Connects CL cell types to BTO tissues by:
  * Resolving CL → UBERON anatomical references
  * Following UBERON part_of OWL restrictions
  * Mapping UBERON terms to BTO tissues via UBERON dbxrefs

- tissue_part_of_anatomy
  Connects BTO tissues to UBERON anatomical structures by:
  * Using UBERON structural part_of relationships
  * Translating UBERON terms to BTO tissues via dbxrefs

Changes:
- Updated schema and adapter configurations to define the new edge types.
- Extended the following adapters to emit cross-ontology edges:
  - Cell Line Ontology adapter (CLO)
  - Cell Ontology adapter (CL)
  - Brenda Tissue Ontology adapter (BTO)
- Validated that generated KG output files include the new relations with correct labels and properties.

Technical Notes:
- Cross-ontology relations are derived using:
  * rdfs:subClassOf
  * OWL part_of restrictions
  * hasDbXref mappings
- UBERON serves as the central bridge ontology for anatomical structure resolution.
- Edge generation logic includes duplicate prevention and optimized traversal of OWL restrictions for performance and correctness.
"""

import rdflib
from rdflib.namespace import RDFS

from biocypher_metta.adapters.ontologies_adapter import OntologyAdapter

class CellLineOntologyAdapter(OntologyAdapter):
    ONTOLOGIES = {
        'clo': 'http://purl.obolibrary.org/obo/clo.owl'
    }

    def __init__(self, write_properties, add_provenance, ontology, type, label='cell_line', dry_run=False, add_description=False, cache_dir=None):
        super().__init__(write_properties, add_provenance, ontology, type, label, dry_run, add_description, cache_dir)

    def get_ontology_source(self):
        return 'Cell Line Ontology', 'http://purl.obolibrary.org/obo/clo.owl'

    def get_nodes(self):
        for term_id, label, props in super().get_nodes():
            if self.write_properties and self.add_description and 'description' in props:
                props['description'] = props['description'].replace('"', '')
            yield term_id, label, props

    def get_uri_prefixes(self):
        """Define URI prefixes for Sequence Ontology."""
        return {
            'primary': 'http://purl.obolibrary.org/obo/CLO_',
            'cl': 'http://purl.obolibrary.org/obo/CL_',
            'uberon': 'http://purl.obolibrary.org/obo/UBERON_',
        }

    def get_edges(self):
        if self.type != 'edge':
            return

        # Preserve default behaviour for existing labels like `clo_subclass_of`.
        if self.label != 'cell_line_is_a_cell_type':
            yield from super().get_edges()
            return

        self.update_graph()
        self.cache_edge_properties()

        seen = set()

        # 1) Direct subclass axioms: CLO term rdfs:subClassOf CL term
        for from_node, to_node in self.graph.subject_objects(predicate=RDFS.subClassOf, unique=True):
            if not isinstance(from_node, rdflib.term.URIRef) or not isinstance(to_node, rdflib.term.URIRef):
                continue
            if not self.is_term_of_type(from_node, 'primary'):
                continue
            if not self.is_term_of_type(to_node, 'cl'):
                continue

            if self.is_deprecated(from_node) or self.is_deprecated(to_node):
                continue

            from_key = OntologyAdapter.to_key(from_node)
            to_key = OntologyAdapter.to_key(to_node)
            if not from_key or not to_key:
                continue

            pair = (from_key, to_key)
            if pair in seen:
                continue
            seen.add(pair)

            props = {}
            if self.write_properties:
                props['rel_type'] = 'subclass'
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url

            yield from_key, to_key, self.label, props

        # 2) Xrefs: CLO term hasDbXref "CL:xxxx" => CLO term is_a CL term
        # Note: OntologyAdapter.get_edges() ignores dbxrefs by default, so we handle them here.
        for from_node, xref in self.graph.subject_objects(predicate=OntologyAdapter.DB_XREF, unique=True):
            if not isinstance(from_node, rdflib.term.URIRef) or not self.is_term_of_type(from_node, 'primary'):
                continue
            if self.is_deprecated(from_node):
                continue
            if not isinstance(xref, rdflib.term.Literal):
                continue

            xref_str = str(xref).strip()
            if not xref_str:
                continue
            xref_str = xref_str.split()[0]
            if xref_str.startswith('CL:'):
                to_key = xref_str
            elif xref_str.startswith('CL_'):
                to_key = xref_str.replace('CL_', 'CL:', 1)
            else:
                continue

            from_key = OntologyAdapter.to_key(from_node)
            if not from_key:
                continue

            pair = (from_key, to_key)
            if pair in seen:
                continue
            seen.add(pair)

            props = {}
            if self.write_properties:
                props['rel_type'] = 'subclass'
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url

            yield from_key, to_key, self.label, props