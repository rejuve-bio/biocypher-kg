'''
# https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data_.28genotype_phenotype_data_.2A.tsv.29
genotype phenotype:
  represented_as: node
  input_label: genotype_phenotype
  is_a: biological entity
  #inherit_properties: true
  properties:
    genotype_FBids: str
    genotype_symbols: str       # string composed of one or more allele symbol(s)
    phenotype_id: str           # The Flybase Anatomy id (FBbt#)  or Flybase Controlled Vocabulary id (FBcv#)
    phenotype_name: str
    qualifier_names: str[]
    qualifier_ids: str[]        # zero or more FBcv# and/or FBdv# to add information to the phenotype
    reference: str              # FBrf#
    taxon_id: int                        # 7227 for dmel / 9606 for hsa


#genotype_symbols	genotype_FBids	phenotype_name	phenotype_id	qualifier_names	qualifier_ids	reference
064Ya[064Ya]	FBal0119724	chemical sensitive	FBcv:0000440			FBrf0131396
1.1.3[1.1.3]	FBal0190078	abnormal eye color	FBcv:0000355			FBrf0190779
1.1.3[1.1.3]	FBal0190078	pigment cell	FBbt:00004230			FBrf0190779
106y[106y]	FBal0151008	fertile	FBcv:0000374			FBrf0141372
106y[106y]	FBal0151008	viable	FBcv:0000349			FBrf0141372
106y[106y]	FBal0151008	abnormal courtship behavior	FBcv:0000399	female	FBcv:0000334	FBrf0141372
14-3-3zeta[P1188]/14-3-3zeta[P2335]	FBal0059629/FBal0134434	lethal	FBcv:0000351			FBrf0208682
14-3-3zeta[P1188]/14-3-3zeta[P2335] 14-3-3zeta[LI.15.hs]	FBal0059629/FBal0134434 FBal0134436	abnormal learning	FBcv:0000397			FBrf0139731
14-3-3zeta[P1188]/14-3-3zeta[P2335] 14-3-3zeta[LI.15.hs]	FBal0059629/FBal0134434 FBal0134436	viable	FBcv:0000349			FBrf0139731
14-3-3zeta[P1188]/14-3-3zeta[P2335] 14-3-3zeta[LII.2.hs] 14-3-3zeta[LI.15.hs]	FBal0059629/FBal0134434 FBal0134435 FBal0134436	viable	FBcv:0000349			FBrf0139731

'''


from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
#from flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
#from biocypher._logger import logger
import re

class GenotypePhenotypeAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_filepath=None):
        self.dmel_filepath = dmel_filepath
        self.label = 'genotype_phenotype'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'
        super(GenotypePhenotypeAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        fb_gg_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fb_gg_table.extract_date_string(self.dmel_filepath)
        #header:
        #genotype_symbols	genotype_FBids	phenotype_name	phenotype_id	qualifier_names	qualifier_ids	reference
        rows = fb_gg_table.get_rows()
        for row in rows:
            if "genotype_FBids" in row[1]:     # to skip header (columns' names)
                continue
            props = {}
            props['genotype_symbols'] = row[0].replace(' ', '@')
            genotype_FBids = row[1].replace(' ', '@')
            props['phenotype_name'] = row[2]
            props['phenotype_id'] = row[3]
            if row[4] != '':
                props['qualifier_names'] = [ name for name in row[4].split('|') ]
            if row[5] != '':
                props['qualifier_ids'] = [ name for name in row[5].split('|') ]
            props['reference'] = row[6]
            props['taxon_id'] = 7227
            #print(f'{props}')
            yield genotype_FBids, self.label, props
