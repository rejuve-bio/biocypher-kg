from biocypher_metta.adapters import Adapter
import gzip
from biocypher._logger import logger
from biocypher_metta.adapters.hsa.helpers import check_genomic_location


# Example genocde vcf input file:
# ##description: evidence-based annotation of the human genome (GRCh38), version 42 (Ensembl 108)
# ##provider: GENCODE
# ##contact: gencode-help@ebi.ac.uk
# ##format: gtf
# ##date: 2022-07-20
# chr1    HAVANA  gene    11869   14409   .       +       .       gene_id "ENSG00000290825.1"; gene_type "lncRNA"; gene_name "DDX11L2"; level 2; tag "overlaps_pseudogene";
# chr1    HAVANA  transcript      11869   14409   .       +       .       gene_id "ENSG00000290825.1"; transcript_id "ENST00000456328.2"; gene_type "lncRNA"; gene_name "DDX11L2"; transcript_type "lncRNA"; transcript_name "DDX11L2-202"; level 2; transcript_support_level "1"; tag "basic"; tag "Ensembl_canonical"; havana_transcript "OTTHUMT00000362751.1";
# chr1    HAVANA  exon    11869   12227   .       +       .       gene_id "ENSG00000290825.1"; transcript_id "ENST00000456328.2"; gene_type "lncRNA"; gene_name "DDX11L2"; transcript_type "lncRNA"; transcript_name "DDX11L2-202"; exon_number 1; exon_id "ENSE00002234944.1"; level 2; transcript_support_level "1"; tag "basic"; tag "Ensembl_canonical"; havana_transcript "OTTHUMT00000362751.1";
# chr1    HAVANA  exon    12613   12721   .       +       .       gene_id "ENSG00000290825.1"; transcript_id "ENST00000456328.2"; gene_type "lncRNA"; gene_name "DDX11L2"; transcript_type "lncRNA"; transcript_name "DDX11L2-202"; exon_number 2; exon_id "ENSE00003582793.1"; level 2; transcript_support_level "1"; tag "basic"; tag "Ensembl_canonical"; havana_transcript "OTTHUMT00000362751.1";



class GencodeAdapter(Adapter):
    ALLOWED_TYPES = ['transcript',
                     'transcribed to', 'transcribed from']
    ALLOWED_LABELS = ['transcript',
                      'transcribed_to', 'transcribed_from']
    #ALLOWED_KEYS = ['gene_id', 'gene_type', 'gene_name',
    ALLOWED_KEYS = ['gene_id', 'gene_type', 'gene_biotype', 'gene_name',  # 'gene_biotype'  key for dmel data
                    'transcript_id', 'transcript_type', 'transcript_biotype', 'transcript_name'] # 'transcript_biotype'  key for dmel data

    INDEX = {'chr': 0, 'type': 2, 'coord_start': 3, 'coord_end': 4, 'info': 8}

    def __init__(self, write_properties, add_provenance, dmel_filepath=None,  hsa_filepath=None,
                 type='gene', label='gencode_gene',
                 chr=None, start=None, end=None):
        if label not in GencodeAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid labelS. Allowed values: ' +
                             ','.join(GencodeAdapter.ALLOWED_LABELS))

        self.dmel_filepath = dmel_filepath
        self.hsa_filepath = hsa_filepath
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
        with gzip.open(self.dmel_filepath, 'rt') as input:
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
                                    'transcript_id': info['transcript_id'],
                                    'transcript_name': info['transcript_name'],
                                    #'transcript_type': info['transcript_type'],  # 'transcript_biotype'  key for dmel data
                                    'transcript_type': info['transcript_biotype'],
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'gene_name': info['gene_name'],
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

        with gzip.open(self.hsa_filepath, 'rt') as input:
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
                                    'transcript_id': info['transcript_id'],
                                    'transcript_name': info['transcript_name'],
                                    'transcript_type': info['transcript_type'],
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'gene_name': info['gene_name'],
                                    'taxon_id': 9606
                                }
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url
                            yield transcript_key, self.label, props
                except:
                    logger.info(
                        f'gencode_transcripts_adapter.py::GencodeAdapter::get_nodes-HSA: failed to process for label to load: {self.label}, type to load: {self.type}, data: {line}')
                    
    def get_edges(self):
        with gzip.open(self.dmel_filepath, 'rt') as input:
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
                        _id = gene_key + '_' + transcript_key
                        _source = gene_key
                        _target = transcript_key
                        yield _source, _target, self.label, _props
                    elif self.type == 'transcribed from':
                        _id = transcript_key + '_' + gene_key
                        _source = transcript_key
                        _target = gene_key
                        yield _source, _target, self.label, _props
                except:
                    logger.info(
                        f'GencodeAdapter::get_edges-DMEL: failed to process for label to load: {self.label}, type to load: {self.type}, data: {line}')

        with gzip.open(self.hsa_filepath, 'rt') as input:
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
                    'taxon_id': 9606
                }
                if self.write_properties and self.add_provenance:
                    _props['source'] = self.source
                    _props['source_url'] = self.source_url

                try:
                    if self.type == 'transcribed to':
                        _id = gene_key + '_' + transcript_key
                        _source = gene_key
                        _target = transcript_key
                        yield _source, _target, self.label, _props
                    elif self.type == 'transcribed from':
                        _id = transcript_key + '_' + gene_key
                        _source = transcript_key
                        _target = gene_key
                        yield _source, _target, self.label, _props
                except:
                    logger.info(
                        f'GencodeAdapter::get_edges-HSA: failed to process for label to load: {self.label}, type to load: {self.type}, data: {line}')
