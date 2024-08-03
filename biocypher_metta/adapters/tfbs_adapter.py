import gzip
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location, to_float

# Example data
# Description for each field can be found here: http://genome.ucsc.edu/cgi-bin/hgTables
#bin    chrom   chromStart  chromEnd    name    score   sourceCount sourceIds   sourceScores
# 585	chr1	10045	10234	DPF2	695	2	62,669	695,506
# 585	chr1	10048	10215	RELB	262	1	263	262
# 585	chr1	10049	10148	TCF12	156	1	167	156


class TfbsAdapter(Adapter):
    INDEX = {'chr': 1, 'start': 2, 'end': 3, 'tf': 4, 'score': 5}
    def __init__(self, write_properties, add_provenance, filepath,
                 hgnc_to_ensembl, label, chr=None, start=None, end=None):
        self.filepath = filepath
        self.hgnc_to_ensembl_map = pickle.load(open(hgnc_to_ensembl, 'rb'))
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label

        self.source = 'ENCODE'
        self.source_url = 'https://hgdownload.soe.ucsc.edu/goldenpath/hg38/database/encRegTfbsClustered.txt.gz'
        super(TfbsAdapter, self).__init__(write_properties, add_provenance)
    
    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as f:
            for line in f:
                data = line.split('\t')
                chr = data[TfbsAdapter.INDEX['chr']]
                start = int(data[TfbsAdapter.INDEX['start']])
                end = int(data[TfbsAdapter.INDEX['end']])
                tfbs_id = build_regulatory_region_id(chr, start, end)
                props = {}

                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    if self.write_properties:
                        props['chr'] = chr
                        props['start'] = start
                        props['end'] = end
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                
                yield tfbs_id, self.label, props
    
    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as f:
            for line in f:
                data = line.split('\t')
                chr = data[TfbsAdapter.INDEX['chr']]
                start = int(data[TfbsAdapter.INDEX['start']])
                end = int(data[TfbsAdapter.INDEX['end']])
                tf = data[TfbsAdapter.INDEX['tf']]
                tf_ensembl = self.hgnc_to_ensembl_map.get(tf)
                tfbs_id = build_regulatory_region_id(chr, start, end)
                score = to_float(data[TfbsAdapter.INDEX['score']]) / 1000 # divide by 1000 to normalize score
                props = {}
                if tf_ensembl is None:
                    continue

                if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                    if self.write_properties:
                        props['score'] = score
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                
                    yield tf_ensembl, tfbs_id, self.label, props