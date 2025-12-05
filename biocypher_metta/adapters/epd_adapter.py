import csv
import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location
from biocypher_metta.processors import HGNCProcessor
from biocypher._logger import logger
# Example EPD bed input file:
##CHRM Start  End   Id  Score Strand -  -
# chr1 959245 959305 NOC2L_1 900 - 959245 959256
# chr1 960583 960643 KLHL17_1 900 + 960632 960643
# chr1 966432 966492 PLEKHN1_1 900 + 966481 966492
# chr1 976670 976730 PERM1_1 900 - 976670 976681

class EPDAdapter(Adapter):
    INDEX = {'chr' : 0, 'coord_start' : 1, 'coord_end' : 2, 'gene_id' : 3}

    def __init__(self, filepath, hgnc_to_ensembl_map=None, write_properties=None,
                 add_provenance=None, type='promoter', label='promoter', delimiter=' ',
                 chr=None, start=None, end=None, hgnc_processor=None):
        self.filepath = filepath

        # Use provided processor or create new one
        if hgnc_processor is None:
            self.hgnc_processor = HGNCProcessor()
            self.hgnc_processor.load_or_update()
        else:
            self.hgnc_processor = hgnc_processor

        self.type = type
        self.label = label
        self.delimiter = delimiter
        self.chr = chr
        self.start = start
        self.end = end

        self.source = 'EPD'
        self.version = '006'
        self.source_url = 'https://epd.expasy.org/ftp/epdnew/H_sapiens/'

        super(EPDAdapter, self).__init__(write_properties, add_provenance)

    def get_nodes(self):
        with gzip.open(self.filepath, 'rt') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            for line in reader:
                chr = line[EPDAdapter.INDEX['chr']]
                coord_start = int(line[EPDAdapter.INDEX['coord_start']]) + 1 # +1 since it is 0 indexed coordinate
                coord_end = int(line[EPDAdapter.INDEX['coord_end']])
                #CURIE ID Format
                # for promoter SO:0000167 is the exact Sequence Ontology (SO) term for a promoter
                promoter_id = f"SO:{build_regulatory_region_id(chr, coord_start, coord_end)}"

                if check_genomic_location(self.chr, self.start, self.end, chr, coord_start, coord_end):
                    props = {}
                    if self.write_properties:
                        props['chr'] = chr
                        props['start'] = coord_start
                        props['end'] = coord_end

                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield promoter_id, self.label, props

    def get_edges(self):
        with gzip.open(self.filepath, 'rt') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            for line in reader:
                chr = line[EPDAdapter.INDEX['chr']]
                coord_start = int(line[EPDAdapter.INDEX['coord_start']]) + 1 # +1 since it is 0 indexed coordinate
                coord_end = int(line[EPDAdapter.INDEX['coord_end']])
                gene_id = line[EPDAdapter.INDEX['gene_id']].split('_')[0]
                #CURIE ID Format
                # for promoter SO:0000167 is the exact Sequence Ontology (SO) term for a promoter
                ensembl_id = self.hgnc_processor.get_ensembl_id(gene_id)
                if ensembl_id is None:
                    logger.warning(f"Couldn't find Ensembl ID for gene {gene_id}")
                    continue
                ensembl_gene_id = f"ENSEMBL:{ensembl_id}"
                
                if check_genomic_location(self.chr, self.start, self.end, chr, coord_start, coord_end):
                    #CURIE ID Format
                    # for promoter SO:0000167 is the exact Sequence Ontology (SO) term for a promoter
                    promoter_id = f"SO:{build_regulatory_region_id(chr, coord_start, coord_end)}"
                    props = {}
                    if self.write_properties:
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield promoter_id, ensembl_gene_id, self.label, props
