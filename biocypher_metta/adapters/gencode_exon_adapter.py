import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location


# Human data:
# https://www.gencodegenes.org/human/

# Example gencode vcf input file:
# ##description: evidence-based annotation of the human genome (GRCh38), version 42 (Ensembl 108)
# ##provider: GENCODE
# ##contact: gencode-help@ebi.ac.uk
# ##format: gtf
# ##date: 2022-07-20
# chr1    HAVANA  gene    11869   14409   .       +       .       gene_id "ENSG00000290825.1"; gene_type "lncRNA"; gene_name "DDX11L2"; level 2; tag "overlaps_pseudogene";
# chr1    HAVANA  transcript      11869   14409   .       +       .       gene_id "ENSG00000290825.1"; transcript_id "ENST00000456328.2"; gene_type "lncRNA"; gene_name "DDX11L2"; transcript_type "lncRNA"; transcript_name "DDX11L2-202"; level 2; transcript_support_level "1"; tag "basic"; tag "Ensembl_canonical"; havana_transcript "OTTHUMT00000362751.1";
# chr1    HAVANA  exon    11869   12227   .       +       .       gene_id "ENSG00000290825.1"; transcript_id "ENST00000456328.2"; gene_type "lncRNA"; gene_name "DDX11L2"; transcript_type "lncRNA"; transcript_name "DDX11L2-202"; exon_number 1; exon_id "ENSE00002234944.1"; level 2; transcript_support_level "1"; tag "basic"; tag "Ensembl_canonical"; havana_transcript "OTTHUMT00000362751.1";
# chr1    HAVANA  exon    12613   12721   .       +       .       gene_id "ENSG00000290825.1"; transcript_id "ENST00000456328.2"; gene_type "lncRNA"; gene_name "DDX11L2"; transcript_type "lncRNA"; transcript_name "DDX11L2-202"; exon_number 2; exon_id "ENSE00003582793.1"; level 2; transcript_support_level "1"; tag "basic"; tag "Ensembl_canonical"; havana_transcript "OTTHUMT00000362751.1";

# Mouse data:
# https://www.gencodegenes.org/mouse/


# Fly data:
# https://ftp.ebi.ac.uk/ensemblgenomes/pub/metazoa/current/gtf/drosophila_melanogaster/

# Example gencode vcf input file:
# Dmel:
# 3R	FlyBase	gene	17750129	17763188	.	-	.	gene_id "FBgn0038542"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding";
# 3R	FlyBase	transcript	17750129	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding";
# 3R	FlyBase	exon	17758709	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "1"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E1";
# 3R	FlyBase	exon	17757024	17757709	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "2"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E2";

# dmelSummaries: table
#FBgn_ID	Gene_Symbol	Summary_Source	Summary

class GencodeExonAdapter(Adapter):
    ALLOWED_KEYS = ['gene_id', 'transcript_id', 'transcript_type', 'transcript_name', 'exon_number', 'exon_id']
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

    def __init__(self, write_properties, add_provenance, taxon_id, label = 'exon', filepath=None,
                 chr=None, start=None, end=None):
        self.filepath = filepath
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.dataset = 'gencode_exon'
        self.source = 'GENCODE'
        self.version = 'v44'
        self.source_url = 'https://www.gencodegenes.org/human/'

        super(GencodeExonAdapter, self).__init__(write_properties, add_provenance)

    def parse_info_metadata(self, info):
        parsed_info = {}
        for key, value in zip(info, info[1:]):
            if key in GencodeExonAdapter.ALLOWED_KEYS:
                parsed_info[key] = value.replace('"', '').replace(';', '')
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
                    
                split_line = line.strip().split()
                if split_line[self.INDEX['type']] == 'exon':
                    info = self.parse_info_metadata(
                        split_line[self.INDEX['info']:])
                    
                    # Skip if we don't want to keep this transcript
                    # if not self.should_keep_transcript(info.get('transcript_type', ''), info.get('tags', [])):
                    #     continue

                    gene_id = f"ENSEMBL:{info['gene_id'].split('.')[0]}"
                    if info['gene_id'].endswith('PAR_Y'):
                        gene_id = gene_id + '_PAR_Y'
                        
                    transcript_id = f"ENSEMBL:{info['transcript_id'].split('.')[0]}"
                    if info['transcript_id'].endswith('_PAR_Y'):
                        transcript_id = transcript_id + '_PAR_Y'
                        
                    exon_id = f"ENSEMBL:{info['exon_id'].split('.')[0]}"
                    # If the exon_id ends with _PAR_Y, we append it to the exon_id
                    if info['exon_id'].endswith('_PAR_Y'):
                        exon_id = exon_id + '_PAR_Y'
                        
                    chr = split_line[self.INDEX['chr']]
                    start = int(split_line[self.INDEX['coord_start']])
                    end = int(split_line[self.INDEX['coord_end']])
                    
                    props = {}
                    try:
                        if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                            if self.write_properties:
                                props = {
                                    'gene_id': gene_id,
                                    'transcript_id': transcript_id,
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'exon_number': int(info.get('exon_number', -1)),
                                    'exon_id': exon_id,
                                }
                                
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url
                                    
                            yield exon_id, self.label, props
                    except Exception as e:
                        print(
                            f'Failed to process for label to load: {self.label}, type to load: exon, data: {line}')
                        print(f'Error: {str(e)}')

    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as input:
            for line in input:
                if line.startswith('#'):
                    continue

                data_line = line.strip().split()
                if data_line[self.INDEX['type']] != 'exon':
                    continue

                info = self.parse_info_metadata(data_line[self.INDEX['info']:])
                
                # Skip if we don't want to keep this transcript
                if not self.should_keep_transcript(info.get('transcript_type', ''), info.get('tags', [])):
                    continue

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
                    _source = f"ENSEMBL:{transcript_key}"
                    _target = F"ENSEMBL:{exon_key}"
                    yield _source, _target, self.label, _props
                except Exception as e:
                    print(
                        f'Failed to process for label to load: {self.label}, type to load: exon, data: {line}')
                    print(f'Error: {str(e)}')