from oaklib import get_adapter
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import get_predicate


class CellOntologyAdapter(Adapter):
    def __init__(self, write_properties, add_provenance, label, predicate=None, dry_run=False):
        self.label = label
        self.dry_run = dry_run
        self.adapter = get_adapter("sqlite:obo:cl")
        self.predicate = get_predicate(predicate)

        self.source = 'CL'
        self.source_url = 'http://purl.obolibrary.org/obo/cl.owl'
        
        super(CellOntologyAdapter, self).__init__(write_properties, add_provenance)
    
    def get_nodes(self):
        i = 0
        for node in self.adapter.nodes():
            if i > 100 and self.dry_run:
                break
            try:
                if node.type != 'CLASS':
                    continue
                id = node.id
                name = node.lbl
                try:
                    definition = node.meta.definition.val
                except:
                    definition = None
                try:
                    synonyms = [s.val for s in node.meta.synonyms]
                except:
                    synonyms = []
                props = {}
                if self.write_properties:
                    props['cl_id'] = id
                    props['cl_name'] = name
                    props['cl_definition'] = definition
                    props['cl_synonyms'] = synonyms
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                i += 1
                yield id, self.label, props
            except:
                continue

    def get_edges(self):
        i = 0
        assert self.predicate != None, f"{self.predicate} predicate couldn't be found. please check get_predicate function in the helpers.py"
        
        for _source, _predicate, _target in self.adapter.relationships():
            if self.predicate != _predicate:
                continue
            if i > 100 and self.dry_run:
                break
            i += 1
            yield _source, _target, self.label, {}
            