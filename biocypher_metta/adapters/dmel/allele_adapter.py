'''
# Human:  to be defined…
#
#
# Fly:
# FB https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Alleles_.3C.3D.3E_Genes_.28fbal_to_fbgn_fb_.2A.tsv.29

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


class AlleleAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_filepath=None, label='allele'):
        self.dmel_filepath = dmel_filepath
        self.label = label
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'
        super(AlleleAdapter, self).__init__(write_properties, add_provenance)
 

    def get_nodes(self):
        fbal_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fbal_table.extract_date_string(self.dmel_filepath)
        #header:
        #AlleleID	AlleleSymbol	GeneID	GeneSymbol
        rows = fbal_table.get_rows()
        for row in rows:
            props = {}
            allele_id = f'FlyBase:{row[0]}'
            props['allele_symbol'] = row[1]
            props['taxon_id'] = 7227

            yield allele_id, self.label, props      # here label is 'allele'

    def get_edges(self):
        fbal_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fbal_table.extract_date_string(self.dmel_filepath)
        #header:
        #AlleleID	AlleleSymbol	GeneID	GeneSymbol
        rows = fbal_table.get_rows()
        for row in rows:
            props = {}
            source = f'FlyBase:{row[0].lower()}' # allele
            target = f'FlyBase:{row[2].lower()}' # gene
            props['taxon_id'] = 7227

            yield source, target, self.label, props     # here label is 'variant_of'
