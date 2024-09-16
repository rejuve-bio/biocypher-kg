from biocypher_metta.adapters.helpers import build_chr_chain_id, find_all_start_locs

class ChromosomeChainMixin:
    def get_located_on_chain_edges(self, node_id, chr, start, end):
        for resolution in self.resolutions:
            start_locs = find_all_start_locs(start, end, resolution)
            for start_loc in start_locs:
                try:
                    end_loc = start_loc + resolution -1
                    chain_id = build_chr_chain_id(chr, start_loc, end_loc, resolution)

                    props = {}
                    if self.write_properties:
                        props['resolution'] = resolution
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                    
                    yield node_id, chain_id, self.label, props
                except Exception as e:
                    raise e