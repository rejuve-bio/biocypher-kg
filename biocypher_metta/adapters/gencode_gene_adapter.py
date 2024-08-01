import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.hsa.helpers import check_genomic_location
from biocypher._logger import logger


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

# Dmel:
# 3R	FlyBase	gene	17750129	17763188	.	-	.	gene_id "FBgn0038542"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding";
# 3R	FlyBase	transcript	17750129	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding";
# 3R	FlyBase	exon	17758709	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "1"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E1";
# 3R	FlyBase	exon	17757024	17757709	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "2"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E2";

class GencodeGeneAdapter(Adapter):
    ALLOWED_KEYS = ['gene_id', 'gene_type', 'gene_biotype', 'gene_name',  # 'gene_biotype'  key for dmel data
                    'transcript_id', 'transcript_type', 'transcript_name', 'transcript_biotype', 'hgnc_id']  # 'transcript_biotype'  key for dmel data
    INDEX = {'chr': 0, 'type': 2, 'coord_start': 3, 'coord_end': 4, 'info': 8}

    def __init__(self, write_properties, add_provenance, hsa_filepath=None, hsa_gene_alias_file_path=None,
                 dmel_filepath=None, dmel_gene_alias_file_path=None, chr=None, start=None, end=None):

        self.hsa_filepath = hsa_filepath
        self.hsa_gene_alias_file_path = hsa_gene_alias_file_path
        self.dmel_filepath = dmel_filepath
        self.dmel_gene_alias_file_path = dmel_gene_alias_file_path
        self.chr = chr
        self.start = start
        self.end = end
        self.label = 'gene'
        self.dataset = 'gencode_gene'
        self.type = 'gene'
        self.source = 'GENCODE'
        self.version = 'v44'                                        # TODO changing value ---> take it from data file header
        self.source_url = 'https://www.gencodegenes.org/human/'

        super(GencodeGeneAdapter, self).__init__(write_properties, add_provenance)

    def parse_info_metadata(self, info):
        parsed_info = {}
        for key, value in zip(info, info[1:]):
            if key in GencodeGeneAdapter.ALLOWED_KEYS:
                parsed_info[key] = value.replace('"', '').replace(';', '')
        return parsed_info

    # the gene alias dict will use both ensembl id and hgnc id as key
    def dmel_get_gene_alias(self, gene_alias_file_path):
        alias_dict = {}
        with gzip.open(gene_alias_file_path, 'rt') as input:
            next(input)
            for line in input:
                (tax_id, gene_id, symbol, locus_tag, synonyms, dbxrefs, chromosome, map_location, description, type_of_gene, symbol_from_nomenclature_authority,
                 full_name_from_nomenclature_authority, Nomenclature_status, Other_designations, Modification_date, Feature_type) = line.split('\t')

                split_dbxrefs = dbxrefs.split('|')
                #hgnc = ''
                ensembl = ''
                for ref in split_dbxrefs:
                    if ref.startswith('FLYBASE:'):
                        ensembl = ref[8:]
                if ensembl:
                    complete_synonyms = []
                    complete_synonyms.append(symbol)
                    for i in synonyms.split('|'):
                        complete_synonyms.append(i)
                    # if hgnc:
                    #     complete_synonyms.append(hgnc)
                    for i in Other_designations.split('|'):
                        complete_synonyms.append(i)
                    complete_synonyms.append(
                        symbol_from_nomenclature_authority)
                    complete_synonyms.append(
                        full_name_from_nomenclature_authority)
                    complete_synonyms = list(set(complete_synonyms))
                    if '-' in complete_synonyms:
                        complete_synonyms.remove('-')
                    if ensembl:
                        alias_dict[ensembl] = complete_synonyms

        return alias_dict

        # the gene alias dict will use both ensembl id and hgnc id as key
    def hsa_get_gene_alias(self, gene_alias_file_path):
        alias_dict = {}
        with gzip.open(gene_alias_file_path, 'rt') as input:
            next(input)
            for line in input:
                (tax_id, gene_id, symbol, locus_tag, synonyms, dbxrefs, chromosome, map_location, description,
                 type_of_gene, symbol_from_nomenclature_authority,
                 full_name_from_nomenclature_authority, Nomenclature_status, Other_designations, Modification_date,
                 Feature_type) = line.split('\t')

                split_dbxrefs = dbxrefs.split('|')
                hgnc = ''
                ensembl = ''
                for ref in split_dbxrefs:
                    if ref.startswith('HGNC:'):
                        hgnc = ref[5:]
                    if ref.startswith('Ensembl:'):
                        ensembl = ref[8:]
                if ensembl or hgnc:
                    complete_synonyms = []
                    complete_synonyms.append(symbol)
                    for i in synonyms.split('|'):
                        complete_synonyms.append(i)
                    if hgnc:
                        complete_synonyms.append(hgnc)
                    for i in Other_designations.split('|'):
                        complete_synonyms.append(i)
                    complete_synonyms.append(
                        symbol_from_nomenclature_authority)
                    complete_synonyms.append(
                        full_name_from_nomenclature_authority)
                    complete_synonyms = list(set(complete_synonyms))
                    if '-' in complete_synonyms:
                        complete_synonyms.remove('-')
                    if ensembl:
                        alias_dict[ensembl] = complete_synonyms
                    if hgnc:
                        alias_dict[hgnc] = complete_synonyms

        return alias_dict

    def get_nodes(self):
        dmel_alias_dict = self.dmel_get_gene_alias(self.dmel_gene_alias_file_path)
        hsa_alias_dict = self.hsa_get_gene_alias(self.hsa_gene_alias_file_path)
        #self.get_organism_nodes(self.dmel_data_filepath, dmel_alias_dict, 'gene_biotype')
        with gzip.open(self.dmel_filepath, 'rt') as input:
            for line in input:
                if line.startswith('#'):
                    continue
                #print(line)
                split_line = line.strip().split()
                if split_line[GencodeGeneAdapter.INDEX['type']] == 'gene':
                    info = self.parse_info_metadata(split_line[GencodeGeneAdapter.INDEX['info']:])
                    gene_id = info['gene_id']
                    #id = gene_id.split('.')[0]
                    alias = dmel_alias_dict.get(gene_id)
                    if not alias:                           # check this for dmel
                        hgnc_id = info.get('hgnc_id')
                        if hgnc_id:
                            alias = dmel_alias_dict.get(hgnc_id)

                    chr = split_line[GencodeGeneAdapter.INDEX['chr']]
                    start = int(split_line[GencodeGeneAdapter.INDEX['coord_start']])
                    end = int(split_line[GencodeGeneAdapter.INDEX['coord_end']])
                    props = {}
                    try:
                        if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                            if self.write_properties:
                                props = {
                                    'gene_type': info['gene_biotype'],
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'gene_name': info['gene_name'],
                                    'synonyms': alias,
                                    'taxon_id': 7227
                                }
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url

                            yield gene_id, self.label, props
                    except Exception as e:
                        logger.info(
                            f'gencode_gene_adapter.py::GencodeGeneAdapter::get_nodes-DMEL: failed to process for label to load: {self.label}, type to load: {self.type}:\n'
                            f'Exception: {e}\n'
                            f'Missing data:\n {line}'
                        )
                            #f'fail to process for label to load: {self.label}, type to load: {self.type}, data: {line}')

        # self.get_organism_nodes(self.hsa_filepath, hsa_alias_dict, 'gene_type')
        with gzip.open(self.hsa_filepath, 'rt') as input:
            for line in input:
                if line.startswith('#'):
                    continue
                # print(line)
                split_line = line.strip().split()
                if split_line[GencodeGeneAdapter.INDEX['type']] == 'gene':
                    info = self.parse_info_metadata(
                        split_line[GencodeGeneAdapter.INDEX['info']:])
                    gene_id = info['gene_id']
                    id = gene_id.split('.')[0]
                    alias = hsa_alias_dict.get(id)
                    if not alias:  # check this for dmel
                        hgnc_id = info.get('hgnc_id')
                        if hgnc_id:
                            alias = hsa_alias_dict.get(hgnc_id)
                    if gene_id.endswith('_PAR_Y'):
                        id = id + '_PAR_Y'

                    chr = split_line[GencodeGeneAdapter.INDEX['chr']]
                    start = int(split_line[GencodeGeneAdapter.INDEX['coord_start']])
                    end = int(split_line[GencodeGeneAdapter.INDEX['coord_end']])
                    props = {}
                    try:
                        if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                            if self.write_properties:
                                props = {
                                    # 'gene_id': gene_id, # TODO should this be included?
                                    'gene_type': info['gene_type'],
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'gene_name': info['gene_name'],
                                    'synonyms': alias,
                                    'taxon_id': 9606
                                }
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url

                            yield id, self.label, props
                    except:
                        logger.info(
                            f'fail to process for label to load: {self.label}, type to load: {self.type}, data: {line}')

    def get_organism_nodes(self, filepath, alias_dict, gene_type_string):
        '''
            gene_type_string    = 'gene_type' for hsa
                                = 'gene_biotype' for dmel
        '''
        with gzip.open(filepath, 'rt') as input:
            for line in input:
                if line.startswith('#'):
                    continue
                #print(line)
                split_line = line.strip().split()
                if split_line[GencodeGeneAdapter.INDEX['type']] == 'gene':
                    info = self.parse_info_metadata(
                        split_line[GencodeGeneAdapter.INDEX['info']:])
                    gene_id = info['gene_id']
                    id = gene_id.split('.')[0]
                    alias = alias_dict.get(id)
                    if not alias:                           # check this for dmel
                        hgnc_id = info.get('hgnc_id')
                        if hgnc_id:
                            alias = alias_dict.get(hgnc_id)
                    if gene_id.endswith('_PAR_Y'):
                        id = id + '_PAR_Y'

                    chr = split_line[GencodeGeneAdapter.INDEX['chr']]
                    start = int(split_line[GencodeGeneAdapter.INDEX['coord_start']])
                    end = int(split_line[GencodeGeneAdapter.INDEX['coord_end']])
                    props = {}
                    try:
                        if check_genomic_location(self.chr, self.start, self.end, chr, start, end):
                            if self.write_properties:
                                props = {
                                    # 'gene_id': gene_id, # TODO should this be included?
                                    #'gene_type': info['gene_type'],                        # to test  dmel data
                                    #'gene_type': info['gene_biotype'],
                                    'gene_type': info[gene_type_string],
                                    'chr': chr,
                                    'start': start,
                                    'end': end,
                                    'gene_name': info['gene_name'],
                                    'synonyms': alias
                                }
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url

                            yield id, self.label, props
                    except:
                        logger.info(
                            f'fail to process for label to load: {self.label}, type to load: {self.type}, data: {line}')
