import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location
from biocypher._logger import logger


# Dmel:
# 3R	FlyBase	gene	17750129	17763188	.	-	.	gene_id "FBgn0038542"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding";
# 3R	FlyBase	transcript	17750129	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding";
# 3R	FlyBase	exon	17758709	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "1"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E1";
# 3R	FlyBase	exon	17757024	17757709	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "2"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E2";

# dmelSummaries: table
#FBgn_ID	Gene_Symbol	Summary_Source	Summary

class GencodeExonAdapter(Adapter):
    ALLOWED_KEYS = ['gene_id', 'transcript_id', 'transcript_type', 'transcript_name', 'transcript_biotype', # 'transcript_biotype'  key for dmel data
                    'exon_number', 'exon_id']
    INDEX = {'chr': 0, 'type': 2, 'coord_start': 3, 'coord_end': 4, 'info': 8}

    def __init__(self, write_properties, add_provenance, filepath=None, 
                 type = 'exon', label = 'exon', chr=None, start=None, end=None):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.type = type
        self.dataset = 'gencode_exon'
        self.source = 'GENCODE'
        self.version = 'v44'
        self.source_url = 'https://www.gencodegenes.org'

        super(GencodeExonAdapter, self).__init__(write_properties, add_provenance)

    def parse_info_metadata(self, info):
        parsed_info = {}
        for key, value in zip(info, info[1:]):
            if key in GencodeExonAdapter.ALLOWED_KEYS:
                parsed_info[key] = value.replace('"', '').replace(';', '')
        return parsed_info

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as input:
            self.source_url = 'https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/' # https://www.ensembl.org/Drosophila_melanogaster/Info/Index
            for line in input:
                if line.startswith('#'):
                    continue
                split_line = line.strip().split()
                if split_line[GencodeExonAdapter.INDEX['type']] == 'exon':
                    info = self.parse_info_metadata(
                        split_line[GencodeExonAdapter.INDEX['info']:])
                    gene_id = info['gene_id'].split('.')[0]
                    transcript_id = info['transcript_id'].split('.')[0]
                    exon_id = info['exon_id'].split('.')[0]
                    chr = split_line[GencodeExonAdapter.INDEX['chr']]
                    start = int(split_line[GencodeExonAdapter.INDEX['coord_start']])
                    end = int(split_line[GencodeExonAdapter.INDEX['coord_end']])
                    props = {}
                    try:
                        if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                            if self.write_properties: 
                                props = {
                                    'gene': gene_id,
                                    'transcript': transcript_id,
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'exon_number': int(info.get('exon_number', -1)), 
                                    'taxon_id': 7227
                                }
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url
                                    
                            yield exon_id, self.label, props
                    except:
                        logger.info(f'fail to process for label to load: {self.label}, type to load: {self.type}, data: {line}')
    def get_edges(self):
        self.label = 'includes'
        with gzip.open(self.filepath, 'rt') as input:
            self.source_url = 'https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/'    # https://www.ensembl.org/Drosophila_melanogaster/Info/Index
            for line in input:
                if line.startswith('#'):
                    continue

                data_line = line.strip().split()
                if data_line[GencodeExonAdapter.INDEX['type']] != 'exon':
                    continue

                info = self.parse_info_metadata(data_line[GencodeExonAdapter.INDEX['info']:])
                transcript_key = info['transcript_id'].split('.')[0]
                if info['transcript_id'].endswith('_PAR_Y'):
                    transcript_key = transcript_key + '_PAR_Y'
                exon_key = info['exon_id'].split('.')[0]
                if info['exon_id'].endswith('_PAR_Y'):
                    exon_key = exon_key + '_PAR_Y'

                _props = {}
                if self.write_properties and self.add_provenance:
                    _props['source'] = self.source
                    _props['source_url'] = self.source_url
                try:
                    _source = transcript_key
                    _target = exon_key
                    yield _source, _target, self.label, _props

                except:
                    print(f'Fail to process for label to load: {self.label}, type to load: {self.type}\nsource: {_source}, target: {_target}\ndata: {line}')