'''
# Human:
#
# Fly:
#
# From FB:  https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Drosophila_Paralogs_.28dmel_paralogs_fb_.2A.tsv.gz.29
# FB table columns:
## FBgn_ID	GeneSymbol	Arm/Scaffold	Location	Strand	Paralog_FBgn_ID	Paralog_GeneSymbol	Paralog_Arm/Scaffold	Paralog_Location	Paralog_Strand	DIOPT_score
paralogy association:
  description: >-
  Non-directional association between two genes indicating that there is an paralogy relation among them:  “Historical homology that involves genes that diverged after a duplication event (http://purl.obolibrary.org/obo/RO_HOM0000011)"
  is_a: related to at instance level
  represented_as: edge
  input_label: paralogs_genes
  source: gene
  target: gene
  properties:
    taxon_id: int                        # 7227 for dmel / 9606 for hsa    (TO BE INHERITED BY ALL TYPES)


# FB table columns:
## FBgn_ID	GeneSymbol	Arm/Scaffold	Location	Strand	Paralog_FBgn_ID	Paralog_GeneSymbol	Paralog_Arm/Scaffold	Paralog_Location	Paralog_Strand	DIOPT_score
FBgn0000003	7SLRNA:CR32864	3R	6822500..6822798	1	FBgn0261504	7SLRNA:CR42652	3R	6820131..6820429	-1	1
FBgn0000008	a	2R	22136968..22172834	1	FBgn0029830	Grip	X	5966279..5986699	1	1
FBgn0000008	a	2R	22136968..22172834	1	FBgn0029835	CG5921	X	6050828..6065646	1	2
FBgn0000008	a	2R	22136968..22172834	1	FBgn0001263	inaD	2R	22855688..22858794	-1	2
FBgn0000008	a	2R	22136968..22172834	1	FBgn0067864	Patj	3L	1798803..1802184	1	3
FBgn0000008	a	2R	22136968..22172834	1	FBgn0000163	baz	X	17160006..17200728	1	1
FBgn0000008	a	2R	22136968..22172834	1	FBgn0026313	X11L	X	17599608..17606879	1	1
FBgn0023023	CRMP	3R	5571108..5579189	1	FBgn0003189	r	X	16655359..16668712	1	1
FBgn0023076	Clk	3L	7763233..7775603	-1	FBgn0003068	per	X	2685580..2692780	1	1
FBgn0023076	Clk	3L	7763233..7775603	-1	FBgn0002723	Met	X	11616124..11621280	1	3

'''

from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
#from flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger


class ParalogyAssociationAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_data_filepath):
        self.dmel_data_filepath = dmel_data_filepath
        self.label = 'paralogs_genes'
        self.type = 'paralogy association'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        super(ParalogyAssociationAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        fb_paralogs_table= FlybasePrecomputedTable(self.dmel_data_filepath)
        self.version = fb_paralogs_table.extract_date_string(self.dmel_data_filepath)
        # header:
        ## FBgn_ID	GeneSymbol	Arm/Scaffold	Location	Strand	Paralog_FBgn_ID	Paralog_GeneSymbol	Paralog_Arm/Scaffold	Paralog_Location	Paralog_Strand	DIOPT_score
        rows = fb_paralogs_table.get_rows()
        for row in rows:
            props = {}
            try:
                source = row[0]
                props['source_symbol'] = row[1]
                target = row[5]
                props['target_symbol'] = row[6]
                props['DIOPT_score'] = int(row[10])
                props['taxon_id'] = 7227
            except Exception as e:
                print(f'\nDeffective ROW:\n{row}\nException: {e}')
                continue
            yield source, target, self.label, props
