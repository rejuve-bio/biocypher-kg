'''
# Dmel to hsa data:
# From FB:  https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Human_Orthologs_.28dmel_human_orthologs_disease_fb_.2A.tsv.gz.29
# FB table columns:
##Dmel_gene_ID	Dmel_gene_symbol	Human_gene_HGNC_ID	Human_gene_OMIM_ID	Human_gene_symbol	DIOPT_score	OMIM_Phenotype_IDs	OMIM_Phenotype_IDs[name]

orthology association:
  description: >-
  Non-directional association between two genes indicating that there is an orthology relation among them:  “Historical homology that involves genes that diverged after a speciation event (http://purl.obolibrary.org/obo/RO_HOM0000017)"
  is_a: related to at instance level
  #inherit_properties: true
  represented_as: edge
  input_label: orthologs_genes
  source: gene
  target: gene
  properties:
    DIOPT_score: int
    hsa_hgnc_id: str
    hsa_omim_id: str
    hsa_omim_phenotype_ids: str[]
    hsa_omim_phenotype_ids_names: str[]
    source_organism: str
    target_organism: str
    taxon_id: int                        # 7227 for dmel / 9606 for hsa

# FB table columns:
##Dmel_gene_ID	Dmel_gene_symbol	Human_gene_HGNC_ID	Human_gene_OMIM_ID	Human_gene_symbol	DIOPT_score	OMIM_Phenotype_IDs	OMIM_Phenotype_IDs[name]
FBgn0031081	Nep3	HGNC:8918	OMIM:300550	PHEX	5	307800	307800[HYPOPHOSPHATEMIC RICKETS, X-LINKED DOMINANT; XLHR]
FBgn0031081	Nep3	HGNC:14668	OMIM:618104	MMEL1	8
FBgn0031081	Nep3	HGNC:7154	OMIM:120520	MME	7	617017,617018	617017[CHARCOT-MARIE-TOOTH DISEASE, AXONAL, TYPE 2T; CMT2T],617018[SPINOCEREBELLAR ATAXIA 43; SCA43]
FBgn0031081	Nep3	HGNC:13275	OMIM:610145	ECE2	12
FBgn0031081	Nep3	HGNC:3147	OMIM:605896	ECEL1	7	615065	615065[ARTHROGRYPOSIS, DISTAL, TYPE 5D; DA5D]
FBgn0031081	Nep3	HGNC:3146	OMIM:600423	ECE1	14	145500,61387	145500[HYPERTENSION, ESSENTIAL],613870[HIRSCHSPRUNG DISEASE, CARDIAC DEFECTS, AND AUTONOMIC DYSFUNCTION; HCAD]
FBgn0031081	Nep3	HGNC:6308	OMIM:613883	KEL	5	110900	110900[BLOOD GROUP--KELL SYSTEM; KEL]
FBgn0031081	Nep3	HGNC:53615		EEF1AKMT4-ECE2	13

'''

from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
#from flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger
import pickle

class OrthologyAssociationAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, dmel_data_filepath, hsa_hgnc_to_ensemble_map):
        self.dmel_data_filepath = dmel_data_filepath
        self.label = 'orthologs_genes'
        self.type = 'orthology association'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        with open(hsa_hgnc_to_ensemble_map, "rb") as f:
            self.hsa_hgnc2ensemble = pickle.load(f)

        super(OrthologyAssociationAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        fb_orthologs_table= FlybasePrecomputedTable(self.dmel_data_filepath)
        self.version = fb_orthologs_table.extract_date_string(self.dmel_data_filepath)
        # header:
        #Dmel_gene_ID	Dmel_gene_symbol	Human_gene_HGNC_ID	Human_gene_OMIM_ID	Human_gene_symbol	DIOPT_score	OMIM_Phenotype_IDs	OMIM_Phenotype_IDs[name]
        rows = fb_orthologs_table.get_rows()
        for row in rows:
            props = {}
            source = row[0]
            hsa_hgnc_id = row[2]
            try:
                target = self.hsa_hgnc2ensemble[ hsa_hgnc_id ]
            except KeyError as ke:
                logger.info(
                    f'orthology_adapter.py::OrthologyAdapter::get_edges-DMEL: failed to process for label to load: {self.label}, type to load: {self.type}:\n'
                    f'Exception: {ke}\n'
                    f'Missing data:\n {row}'
                )
                continue
            props['hsa_hgnc_id'] = hsa_hgnc_id
            props['hsa_omim_id'] = row[3]
            props['hsa_hgnc_symbol'] = row[4]
            props['DIOPT_score'] = int(row[5])
            props['hsa_omim_phenotype_ids'] = row[6]
            props['hsa_omim_phenotype_ids_names'] = row[7]
            props['source_organism'] = 'Drosophila melanogaster'
            props['target_organism'] = 'Homo sapiens'

            yield source, target, self.label, props
