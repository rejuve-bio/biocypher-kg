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
import psycopg2


class AlleleAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_filepath=None, label='allele', taxon_id=7227):
        self.dmel_filepath = dmel_filepath
        self.label = label
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'
        self.taxon_id = taxon_id
        super(AlleleAdapter, self).__init__(write_properties, add_provenance)
        self.snp_cache = self._load_snp_cache()
        
    def _load_snp_cache(self):
        """Fetch all SNPs and their locations from FlyBase in one fast query."""
        snp_cache = {}
        try:
            conn = psycopg2.connect(
                host="chado.flybase.org",
                database="flybase",
                user="flybase",
                password="flybase" 
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT f.uniquename, fl.fmin
                FROM feature f
                LEFT JOIN featureloc fl ON f.feature_id = fl.feature_id
                WHERE f.type_id=733 AND f.is_obsolete=FALSE AND f.is_analysis=FALSE AND f.organism_id=1
            """)
            for row in cursor.fetchall():
                uniquename, fmin = row
                snp_cache[uniquename] = fmin
            conn.close()
        except Exception as e:
            print(f"Error connecting to or querying FlyBase: {e}")
        return snp_cache

    def get_nodes(self):
        fbal_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fbal_table.extract_date_string(self.dmel_filepath)
        #header:
        #AlleleID	AlleleSymbol	GeneID	GeneSymbol
        rows = fbal_table.get_rows()
            
        for row in rows:
            props = {}
            fbal_id = row[0]       # AlleleID e.g. FBal0137236
            allele_symbol = row[1] # AlleleSymbol e.g. gukh[142] — used as uniquename in FlyBase feature table
            allele_id = f'FlyBase:{fbal_id}'
            props['allele_symbol'] = allele_symbol
            props['taxon_id'] = self.taxon_id

            if allele_symbol in self.snp_cache:
                snp_props = props.copy()
                fmin = self.snp_cache[allele_symbol]
                if fmin is not None:
                    snp_props['start'] = fmin
                    snp_props['end'] = fmin + 1
                yield allele_id, 'snp', snp_props
            else:
                yield allele_id, self.label, props      # here label is 'allele'

    def get_edges(self):
        fbal_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fbal_table.extract_date_string(self.dmel_filepath)
        #header:
        #AlleleID	AlleleSymbol	GeneID	GeneSymbol
        rows = fbal_table.get_rows()
            
        for row in rows:
            props = {}
            fbal_id = row[0]      
            allele_symbol = row[1] # AlleleSymbol e.g. gukh[142] — used as uniquename in FlyBase feature table
            source = f'FlyBase:{fbal_id}'
            target = f'FlyBase:{row[2]}'
            props['taxon_id'] = self.taxon_id

            is_snp = allele_symbol in self.snp_cache

            if self.label == 'snp_in_gene':
                # Only yield located_in edges for SNPs
                if is_snp:
                    yield source, target, self.label, props
            else:
                if is_snp:
                    yield source, target, 'snp_variant_of', props
                else:
                    yield source, target, self.label, props
