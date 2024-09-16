import math
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_chr_chain_id

# Example chromosome size input file
# chr1	248956422
# chr2	242193529
# chr3	198295559

class ChromosomeChainAdapter(Adapter):
    ALLOWED_LABELS = ['chromosome_chain',
                      'next_chain', 'lower_resolution']
    def __init__(self, file_path, resolutions, write_properties, add_provenance, label="chromosome_chain", dry_run=False):
        if label not in ChromosomeChainAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid label. Allowed labels: ' +
                             ','.join(ChromosomeChainAdapter.ALLOWED_LABELS))
        self.file_path = file_path
        self.resolutions = self.handle_resolutions(resolutions)
        self.label = label
        self.dry_run = dry_run

        self.source = "NCBI"
        self.source_url = "https://hgdownload.cse.ucsc.edu/goldenpath/hg38/bigZips/hg38.chrom.sizes"
        super(ChromosomeChainAdapter, self).__init__(write_properties, add_provenance)

    def handle_resolutions(self, res):
        if not isinstance(res, list):
            res = [int(res)]
        return sorted(res, reverse=True)

    def get_nodes(self):
        for resolution in self.resolutions:
            with open(self.file_path, 'r') as f:
                count = 0
                for line in f:
                    chr, size = line.lower().split()
                    size = int(size)
                    total_chains = math.ceil(float(size) / float(resolution))
                    count = 0
                    for i in range(total_chains):
                        if self.dry_run and count > 1000:
                            break
                        start_loc = i * resolution
                        end_loc = (i + 1) * resolution - 1

                        chain_id = build_chr_chain_id(chr, start_loc, end_loc, resolution)
                        props = {}
                        if self.write_properties:
                            props['chr'] = chr
                            props['start'] = start_loc
                            props['end'] = end_loc
                            props['resolution'] = resolution
                            if self.add_provenance:
                                props['source'] = self.source
                                props['source_url'] = self.source_url
                        count += 1
                        yield chain_id, self.label, props

    def get_edges(self):
        prev_resolution = None
        for resolution in self.resolutions:
            with open(self.file_path, 'r') as f:
                count = 0
                for line in f:
                    chr, size = line.lower().split()
                    size = int(size)
                    prev_chain_id = None
                    total_chains = math.ceil(float(size) / float(resolution))
                    count = 0
                    for i in range(total_chains):
                        if self.dry_run and count > 1000:
                            break
                        start_loc = i * resolution
                        end_loc = (i + 1) * resolution - 1
                        curr_chain_id = build_chr_chain_id(chr, start_loc, end_loc, resolution)
                        
                        if self.label == "next_chain":
                            if prev_chain_id is None: # Skip first chain
                                prev_chain_id = curr_chain_id
                                continue
                            props = {}
                            if self.write_properties and self.add_provenance:
                                props['source'] = self.source
                                props['source_url'] = self.source_url

                            temp_prev_chain_id = prev_chain_id
                            prev_chain_id = curr_chain_id

                            count += 1
                            yield temp_prev_chain_id, curr_chain_id, self.label, props

                        elif self.label == "lower_resolution":
                            if prev_resolution is None: # Skip first resolution
                                continue
                            # Calculate the start location of the parent chain at the previous (lower) resolution.
                            # This ensures that the current chain falls within its parent chain.
                            parent_start_loc = math.floor(float(start_loc) / float(prev_resolution)) * prev_resolution
                            partent_end_loc = parent_start_loc + prev_resolution -1
                            parent_chain_id = build_chr_chain_id(chr, parent_start_loc, partent_end_loc, prev_resolution)

                            props = {}
                            if self.write_properties and self.add_provenance:
                                props['source'] = self.source
                                props['source_url'] = self.source_url

                            count += 1
                            yield curr_chain_id, parent_chain_id, self.label, props
            
            prev_resolution = resolution
