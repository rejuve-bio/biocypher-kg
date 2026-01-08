import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location

# Human data:
# https://www.encodeproject.org/

# Mouse data:
# https://www.encodeproject.org/


# Is this adapter used?   Where are the data?


class ENCODERe2GAdapter(Adapter):
    def __init__(self, filepath, taxon_id, write_properties, add_provenance, label,
                 chr=None, start=None, end=None):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.taxon_id = taxon_id
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
                # CURIE format, SO:0000165 a sequence ontology term for enhancer
                # KGX-compliant (SO: prefix + genomic coords)
                region_id = f"SO:0000165_{build_regulatory_region_id(chr, start, end)}"
                
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
                #CURIE ID for gene, ENSEMBL prefix
                gene_id = f"ENSEMBL:{fields[6] }"
                score = float(fields[-1])
                # KGX-compliant CURIE ID format (SO: prefix + genomic coords)
                region_id = f"SO:0000165_{build_regulatory_region_id(chr, start, end)}"
                
                if not check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    continue
                
                props = {
                    "score": score,  
                }
                
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                
                yield region_id, gene_id, self.label, props