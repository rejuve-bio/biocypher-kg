import gzip
import csv
from io import StringIO
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location, to_float
from biocypher_metta.processors import HGNCProcessor

# Example data
# Description for each field can be found here: http://genome.ucsc.edu/cgi-bin/hgTables
#bin,chrom,chromStart,chromEnd,name,score,sourceCount,sourceIds,sourceScores
# "1","chr1","8388344","8388800","ZBTB33","341","1","471","341"
# "1","chr1","16776930","16777306","ATF2","113","1","274","113"
# "1","chr1","58719516","58720395","ZFX","235","1","428","235"
# "3","chr1","176160080","176160980","CTCF","101","1","1155","101"

class TfbsAdapter(Adapter):
    INDEX = {'bin': 0, 'chr': 1, 'start': 2, 'end': 3, 'tf': 4, 'score': 5}
    
    def __init__(self, write_properties, add_provenance, filepath,
                 hgnc_to_ensembl=None, label=None, chr=None, start=None, end=None,
                 hgnc_processor=None):
        self.filepath = filepath

        # Use provided processor or create new one
        if hgnc_processor is None:
            self.hgnc_processor = HGNCProcessor()
            self.hgnc_processor.load_or_update()
        else:
            self.hgnc_processor = hgnc_processor

        self.chr = chr
        self.start = start
        self.end = end
        self.label = label

        self.source = 'ENCODE'
        self.source_url = 'https://genome.ucsc.edu/cgi-bin/hgTables'
        super(TfbsAdapter, self).__init__(write_properties, add_provenance)
    
    def _read_csv_gz(self):
        with gzip.open(self.filepath, 'rt') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            
            for row in reader:
                if row: 
                    yield row
    
    def get_nodes(self):
        for data in self._read_csv_gz():
            chr = data[TfbsAdapter.INDEX['chr']].strip('"')
            start = int(data[TfbsAdapter.INDEX['start']].strip('"'))
            end = int(data[TfbsAdapter.INDEX['end']].strip('"'))
            #CURIE ID Format for tfbs (SO:0000235 for TFBS)
            tfbs_id = f"SO:0000235_{build_regulatory_region_id(chr, start, end)}"
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
        for data in self._read_csv_gz():
            chr = data[TfbsAdapter.INDEX['chr']].strip('"')
            start = int(data[TfbsAdapter.INDEX['start']].strip('"'))
            end = int(data[TfbsAdapter.INDEX['end']].strip('"'))
            tf = data[TfbsAdapter.INDEX['tf']].strip('"')
            #CURIE ID Format - Get Ensembl ID from HGNC symbol
            ensembl_id = self.hgnc_processor.get_ensembl_id(tf)
            if ensembl_id is None:
                continue
            tf_ensembl = f"ENSEMBL:{ensembl_id}"
            tfbs_id = f"SO:0000235_{build_regulatory_region_id(chr, start, end)}"

            score_str = data[TfbsAdapter.INDEX['score']].strip('"')
            score = to_float(score_str) / 1000  # divide by 1000 to normalize score

            props = {}

            if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                if self.write_properties:
                    props['score'] = score
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
            
                yield tf_ensembl, tfbs_id, self.label, props