from biocypher_metta.adapters import Adapter
import gzip
from biocypher_metta.adapters.helpers import check_genomic_location
from biocypher_metta.adapters.hgnc_processor import HGNCSymbolProcessor

class GencodeAdapter(Adapter):
    ALLOWED_TYPES = ['transcript', 'transcribed to', 'transcribed from']
    ALLOWED_LABELS = ['transcript', 'transcribed_to', 'transcribed_from']
    ALLOWED_KEYS = ['gene_id', 'gene_type', 'gene_name',
                    'transcript_id', 'transcript_type', 'transcript_name', 'tag']

    INDEX = {'chr': 0, 'type': 2, 'coord_start': 3, 'coord_end': 4, 'info': 8}

    # Only transcripts that code for proteins
    CODING_TYPES = {
        'protein_coding', 
        'nonsense_mediated_decay',  
        'non_stop_decay',                   
        'IG_C_gene', 
        'IG_D_gene',  
        'IG_J_gene',  
        'IG_V_gene',  
        'TR_C_gene',  
        'TR_D_gene',  
        'TR_J_gene', 
        'TR_V_gene'   
    }

    # Tags indicating high-quality reviewed transcripts
    REVIEWED_TAGS = {
        # MANE (Matched Annotation from NCBI and EBI) - gold standard
        'MANE_Select',
        # CCDS - consensus protein-coding regions
        'CCDS',               
        # Ensembl canonical - one transcript per gene chosen as canonical
        'Ensembl_canonical'
    }

    def __init__(self, write_properties, add_provenance, filepath=None, 
                 type='gene', label='gencode_gene', 
                 chr=None, start=None, end=None):
        if label not in GencodeAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid label. Allowed values: ' +
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

        self.hgnc_processor = HGNCSymbolProcessor()
        self.hgnc_processor.update_hgnc_data()

        super(GencodeAdapter, self).__init__(write_properties, add_provenance)

    def parse_info_metadata(self, info):
        parsed_info = {}
        tags = []
        
        info_str = ' '.join(info)
        items = info_str.split(';')
        
        for item in items:
            item = item.strip()
            if not item:
                continue
                
            if 'tag' in item:
                tag_match = item.split('"')
                if len(tag_match) > 1:
                    tags.append(tag_match[1])
            else:
                for key in GencodeAdapter.ALLOWED_KEYS:
                    if key in item:
                        value = item.split('"')[1] if '"' in item else item
                        parsed_info[key] = value.strip()
        
        parsed_info['tags'] = tags
        return parsed_info

    def should_keep_transcript(self, transcript_type, tags):
        """Determine if a transcript should be kept based on type and tags."""
        # Keep all non-coding transcripts
        if transcript_type not in self.CODING_TYPES:
            return True
        
        # For coding transcripts, keep those with tags in REVIEWED_TAGS or any appris_principal tags
        return any(tag in self.REVIEWED_TAGS or tag.startswith('appris_principal') for tag in tags)

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as input:
            for line in input:
                if line.startswith('#'):
                    continue

                data_line = line.strip().split()
                if data_line[GencodeAdapter.INDEX['type']] != 'transcript':
                    continue

                data = data_line[:GencodeAdapter.INDEX['info']]
                info = self.parse_info_metadata(data_line[GencodeAdapter.INDEX['info']:])
                
                # Skip if we don't want to keep this transcript
                if not self.should_keep_transcript(info.get('transcript_type', ''), info.get('tags', [])):
                    continue

                result = self.hgnc_processor.process_identifier(info['gene_name'])
            
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
                                    'gene_name': 'unknown' if result['status'] == 'unknown' or result['status'] == 'ensembl_only' else result['current'],
                                }
                                if result['status'] == 'updated':
                                    props['old_gene_name'] = result['original']

                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url
                        
                            if result['status'] == 'unknown':
                                print(f"Unknown gene symbol: {result['original']}")
                            elif result['status'] == 'updated':
                                print(f"Replaced gene symbol: {result['original']} -> {result['current']}")
                            elif result['status'] == 'ensembl_with_symbol' and result['original'] != result['current']:
                                print(f"Ensembl symbol replaced: {result['original']} -> {result['current']}")
                        
                            yield transcript_key, self.label, props
                except Exception as e:
                    print(
                        f'Failed to process for label to load: {self.label}, type to load: {self.type}, data: {line}')
                    print(f'Error: {str(e)}')

    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as input:
            for line in input:
                if line.startswith('#'):
                    continue

                data_line = line.strip().split()
                if data_line[GencodeAdapter.INDEX['type']] != 'transcript':
                    continue

                info = self.parse_info_metadata(data_line[GencodeAdapter.INDEX['info']:])
                
                # Skip if we don't want to keep this transcript
                if not self.should_keep_transcript(info.get('transcript_type', ''), info.get('tags', [])):
                    continue

                transcript_key = info['transcript_id'].split('.')[0]
                if info['transcript_id'].endswith('_PAR_Y'):
                    transcript_key = transcript_key + '_PAR_Y'
                gene_key = info['gene_id'].split('.')[0]
                if info['gene_id'].endswith('_PAR_Y'):
                    gene_key = gene_key + '_PAR_Y'
               
                _props = {}
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
                except Exception as e:
                    print(
                        f'Failed to process for label to load: {self.label}, type to load: {self.type}, data: {line}')
                    print(f'Error: {str(e)}')
