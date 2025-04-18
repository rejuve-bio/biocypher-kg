from biocypher_metta.adapters import Adapter
import gzip
from biocypher._logger import logger
from biocypher_metta.adapters.helpers import check_genomic_location


# Example genocde vcf input file:

# Dmel:
# 3R	FlyBase	gene	17750129	17763188	.	-	.	gene_id "FBgn0038542"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding";
# 3R	FlyBase	transcript	17750129	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding";
# 3R	FlyBase	exon	17758709	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "1"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E1";
# 3R	FlyBase	exon	17757024	17757709	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "2"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E2";

# dmelSummaries: table
#FBgn_ID	Gene_Symbol	Summary_Source	Summary


class GencodeAdapter(Adapter):
    ALLOWED_TYPES = ['transcript',
                     'transcribed to', 'transcribed from']
    ALLOWED_LABELS = ['transcript',
                      'transcribed_to', 'transcribed_from']

    ALLOWED_KEYS = ['gene_id', 'gene_type', 'gene_biotype', 'gene_name',  # 'gene_biotype'  key for dmel data
                    'transcript_id', 'transcript_type', 'transcript_biotype', 'transcript_name'] # 'transcript_biotype'  key for dmel data

    INDEX = {'chr': 0, 'type': 2, 'coord_start': 3, 'coord_end': 4, 'info': 8}

    def __init__(self, write_properties, add_provenance, filepath=None, 
                 type='transcript', label='transcript',
                 chr=None, start=None, end=None):
        if label not in GencodeAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid labelS. Allowed values: ' +
                             ','.join(GencodeAdapter.ALLOWED_LABELS))

        self.filepath = filepath
        self.type = type
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.dataset = label

        self.source = 'GENCODE'
        self.version = 'v44'
        self.source_url = 'https://www.gencodegenes.org/human/'

        super(GencodeAdapter, self).__init__(write_properties, add_provenance)

    def parse_info_metadata(self, info):
        parsed_info = {}
        for key, value in zip(info, info[1:]):
            if key in GencodeAdapter.ALLOWED_KEYS:
                parsed_info[key] = value.replace('"', '').replace(';', '')
        return parsed_info

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as input:
            self.source_url = 'https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/'
            for line in input:
                if line.startswith('#'):
                    continue

                data_line = line.strip().split()
                if data_line[GencodeAdapter.INDEX['type']] != 'transcript':
                    continue

                data = data_line[:GencodeAdapter.INDEX['info']]
                info = self.parse_info_metadata(data_line[GencodeAdapter.INDEX['info']:])
                transcript_key = info['transcript_id'].split('.')[0]
                if info['transcript_id'].endswith('_PAR_Y'):
                    transcript_key = transcript_key + '_PAR_Y'
                gene_key = info['gene_id'].split('.')[0]
                if info['gene_id'].endswith('_PAR_Y'):
                    gene_key = gene_key + '_PAR_Y'
                chr = data[GencodeAdapter.INDEX['chr']]
                start = int(data[GencodeAdapter.INDEX['coord_start']])
                end = int(data[GencodeAdapter.INDEX['coord_end']])
                props = {}
                try:
                    if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                        if self.type == 'transcript':
                            if self.write_properties:
                                props = {
                                    'gene': gene_key,
                                    'transcript_name': info['transcript_name'],
                                    'transcript_type': info['transcript_biotype'],
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'taxon_id': 7227
                                }
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url
                            yield transcript_key, self.label, props

                except Exception as e:
                    print(e)
                    logger.info(
                        f'gencode_transcripts_adapter.py::GencodeAdapter::get_nodes-DMEL: failed to process for label to load: {self.label}, type to load: {self.type}:\n'
                        f'Missing data: {e}\ndata: {line}')

                    
    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as input:
            self.source_url = 'https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/'
            for line in input:
                if line.startswith('#'):
                    continue

                data_line = line.strip().split()
                if data_line[GencodeAdapter.INDEX['type']] != 'transcript':
                    continue

                info = self.parse_info_metadata(data_line[GencodeAdapter.INDEX['info']:])
                transcript_key = info['transcript_id'].split('.')[0]
                if info['transcript_id'].endswith('_PAR_Y'):
                    transcript_key = transcript_key + '_PAR_Y'
                gene_key = info['gene_id'].split('.')[0]
                if info['gene_id'].endswith('_PAR_Y'):
                    gene_key = gene_key + '_PAR_Y'
               
                _props = {
                    'taxon_id': 7227
                }
                if self.write_properties and self.add_provenance:
                    _props['source'] = self.source
                    _props['source_url'] = self.source_url
               
                try:
                    if self.type == 'transcribed to':
                        _source = gene_key
                        _target = transcript_key
                        yield _source, _target, self.label, _props
                    elif self.type == 'transcribed from':
                        _source = transcript_key
                        _target = gene_key
                        yield _source, _target, self.label, _props
                except:
                    logger.info(
                        f'GencodeAdapter::get_edges-DMEL: failed to process for label to load: {self.label}, type to load: {self.type}, data: {line}')

