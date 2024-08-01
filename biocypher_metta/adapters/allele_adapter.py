'''
# Human:  to be defined…
#
#
# Fly:
# FB https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Alleles_.3C.3D.3E_Genes_.28fbal_to_fbgn_fb_.2A.tsv.29
allele:
  represented_as: node
  input_label: allele
  is_a: genomic variant
  inherit_properties: true
  properties:
      gene_id: str
      allele_symbol: str
      taxon_id: int                        # 7227 for dmel / 9606 for hsa
  description: >-
      Different versions of the same variant (in a specific  locus) are called alleles[1, 2]. Most commonly used referring to genes.

# FB table columns:
#AlleleID	AlleleSymbol	GeneID	GeneSymbol
FBal0137236	gukh[142]	FBgn0026239	gukh
FBal0137618	Xrp1[142]	FBgn0261113	Xrp1
FBal0092786	Ecol\lacZ[T125]	FBgn0014447	Ecol\lacZ
FBal0100372	Myc[P0]	FBgn0262656	Myc
FBal0009407	kst[01318]	FBgn0004167	kst
FBal0091321	Ecol\lacZ[kst-01318]	FBgn0014447	Ecol\lacZ
FBal0091320	Ecol\lacZ[mam-04615]	FBgn0014447	Ecol\lacZ

'''
from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
#from flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
#from biocypher._logger import logger
import re

class AlleleAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_filepath=None):
        self.dmel_filepath = dmel_filepath
        self.label = 'allele'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'
        super(AlleleAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        fb_gg_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fb_gg_table.extract_date_string(self.dmel_filepath)
        #header:
        #AlleleID	AlleleSymbol	GeneID	GeneSymbol
        rows = fb_gg_table.get_rows()
        for row in rows:
            props = {}
            allele_id = row[0]
            props['allele_symbol'] = row[1]
            props['gene_id'] = row[2]
            props['taxon_id'] = 7227

            yield allele_id, self.label, props