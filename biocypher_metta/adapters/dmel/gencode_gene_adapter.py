
import gzip
import traceback
import sys
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger
from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable


# Dmel:
# 3R	FlyBase	gene	17750129	17763188	.	-	.	gene_id "FBgn0038542"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding";
# 3R	FlyBase	transcript	17750129	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding";
# 3R	FlyBase	exon	17758709	17758978	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "1"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E1";
# 3R	FlyBase	exon	17757024	17757709	.	-	.	gene_id "FBgn0038542"; transcript_id "FBtr0344474"; exon_number "2"; gene_name "TyrR"; gene_source "FlyBase"; gene_biotype "protein_coding"; transcript_name "TyrR-RB"; transcript_source "FlyBase"; transcript_biotype "protein_coding"; exon_id "FBtr0344474-E2";

# dmelSummaries: table
#FBgn_ID	Gene_Symbol	Summary_Source	Summary

class GencodeGeneAdapter(Adapter):
    ALLOWED_KEYS = ['gene_id', 'gene_type', 'gene_biotype', 'gene_name',  # 'gene_biotype'  key for dmel data
                    'transcript_id', 'transcript_type', 'transcript_name', 'transcript_biotype', 'hgnc_id']  # 'transcript_biotype'  key for dmel data
    INDEX = {'chr': 0, 'type': 2, 'coord_start': 3, 'coord_end': 4, 'info': 8}

    def __init__(self, write_properties, add_provenance, type = 'gene', label = 'gene',
                 filepath=None, gene_alias_file_path=None, summaries_filepath=None, chr=None, start=None, end=None):

        self.filepath = filepath
        self.gene_alias_file_path = gene_alias_file_path
        self.summaries_data = self.to_summaries_dict( FlybasePrecomputedTable(summaries_filepath) )
        self.chr = chr
        self.start = start
        self.end = end
        self.label = label
        self.dataset = 'gencode_gene'
        self.type = type
        self.source = 'GENCODE'
        self.version = 'v44'                                        # TODO changing value ---> take it from data file header
        self.source_url = 'https://www.gencodegenes.org/'

        super(GencodeGeneAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        alias_dict = self.get_gene_alias(self.gene_alias_file_path)
        with gzip.open(self.filepath, 'rt') as input:
            self.source_url = 'https://ftp.ensembl.org/pub/current_gtf/drosophila_melanogaster/'
            for line in input:
                if line.startswith('#'):
                    continue
                split_line = line.strip().split()
                if split_line[GencodeGeneAdapter.INDEX['type']] == 'gene':
                    info = self.parse_info_metadata(split_line[GencodeGeneAdapter.INDEX['info']:])
                    gene_id = info['gene_id']
                    alias = alias_dict.get(gene_id)
                    if not alias:                           # check this for dmel
                        hgnc_id = info.get('hgnc_id')
                        if hgnc_id:
                            alias = alias_dict.get(hgnc_id)

                    chr = split_line[GencodeGeneAdapter.INDEX['chr']]
                    start = int(split_line[GencodeGeneAdapter.INDEX['coord_start']])
                    end = int(split_line[GencodeGeneAdapter.INDEX['coord_end']])
                    props = {}
                    try:
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
                            try:
                                props['summary_source'] = self.summaries_data.get(gene_id)[0]
                            except:
                                logger.info(
                                    f'gencode_gene_adapter.py::GencodeGeneAdapter::get_nodes-DMEL: No summary-source for {gene_id}\n'
                                )
                            try:
                                props['summary'] = self.summaries_data.get(gene_id)[1]
                            except:
                                logger.info(
                                    f'gencode_gene_adapter.py::GencodeGeneAdapter::get_nodes-DMEL: No summary for {gene_id}\n'
                                )
                            if self.add_provenance:
                                props['source'] = self.source
                                props['source_url'] = self.source_url
                        yield gene_id, self.label, props
                        
                    except Exception as e:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        line_number = exc_traceback.tb_lineno
                        print(f"Line::::::::::::::::::--->>> {line_number}: {str(e)}")
                        logger.info(
                            f'gencode_gene_adapter.py::GencodeGeneAdapter::get_nodes-DMEL: failed to process for label to load: {self.label}, type to load: {self.type}:\n'
                            f'Exception: {e}\n'
                            f'Missing data:\n {line}'
                            f"Line::::::::::::::::::--->>> {line_number}: {str(e)}"
                        )
                        traceback.print_exc()
 

    def to_summaries_dict(self, fb_summaries_table):
        table_dict = {}
        # dmelSummaries: table
        # FBgn_ID	Gene_Symbol	Summary_Source	Summary
        for row in fb_summaries_table.get_rows():
            table_dict[row[0]] = [row[2], row[3]]
        return table_dict


    def parse_info_metadata(self, info):
        parsed_info = {}
        for key, value in zip(info, info[1:]):
            if key in GencodeGeneAdapter.ALLOWED_KEYS:
                parsed_info[key] = value.replace('"', '').replace(';', '')
        return parsed_info

    # the gene alias dict will use both ensembl id and hgnc id as key
    def get_gene_alias(self, gene_alias_file_path):
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

