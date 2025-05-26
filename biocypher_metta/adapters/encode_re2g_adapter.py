import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location

class ENCODERe2GAdapter(Adapter):
    def __init__(self, filepath, write_properties, add_provenance, label='enhancer',
                 chr=None, start=None, end=None):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        
        self.source = "ENCODE-rE2G"
        self.version = "1.0"
        self.source_url = "https://www.encodeproject.org/"
        
        super().__init__(write_properties, add_provenance)

    def get_nodes(self):
        with gzip.open(self.filepath, "rt") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                
                fields = line.strip().split("\t")
                chr = fields[0]
                start = int(fields[1])
                end = int(fields[2])
                region_id = build_regulatory_region_id(chr, start, end)
                
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue
                
                props = {
                    "chr": chr,
                    "start": start,
                    "end": end,
                }
                
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                
                yield region_id, self.label, props

    def get_edges(self):
        with gzip.open(self.filepath, "rt") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                
                fields = line.strip().split("\t")
                chr = fields[0]
                start = int(fields[1])
                end = int(fields[2])
                gene_id = fields[6]  
                score = float(fields[-1])
                region_id = build_regulatory_region_id(chr, start, end)
                
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue
                
                props = {
                    "score": score,  
                }
                
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                
                yield region_id, gene_id, self.label, props