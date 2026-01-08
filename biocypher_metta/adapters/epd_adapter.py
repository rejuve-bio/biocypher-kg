import csv
import gzip
import pickle
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id, check_genomic_location

# Human data:
# https://epd.expasy.org/ftp/epdnew/H_sapiens/

# Example EPD bed input file:
##CHRM Start  End   Id  Score Strand -  -
# chr1 959245 959305 NOC2L_1 900 - 959245 959256
# chr1 960583 960643 KLHL17_1 900 + 960632 960643
# chr1 966432 966492 PLEKHN1_1 900 + 966481 966492
# chr1 976670 976730 PERM1_1 900 - 976670 976681


# Fly data:
# https://epd.expasy.org/ftp/epdnew/D_melanogaster/

# chr2L 7456 7516 CG11023_1 900 + 7505 7516
# chr2L 18617 18677 l(2)gl_1 900 - 18617 18628
# chr2L 25167 25227 Ir21a_1 900 - 25167 25178
# chr2L 59231 59291 Cda5_1 900 - 59231 59242


# Mouse data:
# https://epd.expasy.org/ftp/epdnew/M_musculus/ 

# Rat data:
# https://epd.expasy.org/ftp/epdnew/R_norvegicus/


class EPDAdapter(Adapter):
    INDEX = {'chr' : 0, 'coord_start' : 1, 'coord_end' : 2, 'gene_id' : 3}

    CURIE_PREFIX = {
        7227: 'FlyBase',
        9606: 'ENSEMBL'
    }

    def __init__(self, filepath, hgnc_to_ensembl_map, write_properties, add_provenance, taxon_id,
                 type='promoter', label='promoter', delimiter=' ', chr=None, start=None, end=None):
        self.filepath = filepath
        self.hgnc_to_ensembl_map = pickle.load(open(hgnc_to_ensembl_map, 'rb'))
        self.type = type
        self.label = label
        self.delimiter = delimiter
        self.chr = chr
        self.start = start
        self.end = end
        self.taxon_id = taxon_id
        self.source = 'EPD'
        self.version = '006'
        self.source_url = 'https://epd.expasy.org/ftp/epdnew/H_sapiens/'

        super(EPDAdapter, self).__init__(write_properties, add_provenance)

    def get_nodes(self):
        """
        Build a node for each promoter in the EPD BED file
        """
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
                        props['taxon_id'] = f'{self.taxon_id}'

                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url

                    yield promoter_id, self.label, props

    def get_edges(self):
        """
        Build an edge for each promoter-gene interaction in the EPD BED file.
        """
        with gzip.open(self.filepath, 'rt') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            not_found_symbols = 0
            for line in reader:
                chr = line[EPDAdapter.INDEX['chr']]
                coord_start = int(line[EPDAdapter.INDEX['coord_start']]) + 1 # +1 since it is 0 indexed coordinate
                coord_end = int(line[EPDAdapter.INDEX['coord_end']])
                gene_id = line[EPDAdapter.INDEX['gene_id']].split('_')[0]
                if self.taxon_id == 7227:
                    pass
                elif self.taxon_id == 9606:
                    ensembl_gene_id = f"ENSEMBL:{self.hgnc_to_ensembl_map.get(gene_id, None)}"
                if ensembl_gene_id is None:
                    continue
                
                # if ensembl_gene_id is None:
                #     not_found_symbols += 1
                    # print(f"gene_id: {gene_id}  // {ensembl_gene_id}   --->  {self.taxon_id}")                       
                #CURIE ID Format
                # for promoter SO:0000167 is the exact Sequence Ontology (SO) term for a promoter
                ensembl_gene_id = f"{ensembl_gene_id}"
                
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
            # print(f"not found symbols: {not_found_symbols}")

