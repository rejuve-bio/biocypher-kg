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

    def __init__(self, write_properties, add_provenance, filepath=None, gene_id_map_filepath=None, label='orthologs_genes', 
                 source_taxon_id=7227, target_taxon_id=9606, source_prefix='FlyBase', target_prefix='Ensembl', 
                 source_organism_name='Drosophila melanogaster', target_organism_name='Homo sapiens',
                 dmel_data_filepath=None, hsa_hgnc_to_ensemble_map=None):
    
        if dmel_data_filepath and not filepath:
            filepath = dmel_data_filepath
        if hsa_hgnc_to_ensemble_map and not gene_id_map_filepath:
            gene_id_map_filepath = hsa_hgnc_to_ensemble_map
            
        if not filepath:
            raise ValueError("filepath must be provided")

        self.filepath = filepath
        self.label = label
        self.type = 'orthology association'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'
        
        self.source_taxon_id = source_taxon_id
        self.target_taxon_id = target_taxon_id
        self.source_prefix = source_prefix
        self.target_prefix = target_prefix
        self.source_organism_name = source_organism_name
        self.target_organism_name = target_organism_name

        if gene_id_map_filepath:
            self.gene_id_map = pickle.load(open(gene_id_map_filepath, 'rb'))
        else:
            self.gene_id_map = None

        super(OrthologyAssociationAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        fb_orthologs_table = FlybasePrecomputedTable(self.filepath)
        self.version = fb_orthologs_table.extract_date_string(self.filepath)
        # header:
        #Dmel_gene_ID	Dmel_gene_symbol	Human_gene_HGNC_ID	Human_gene_OMIM_ID	Human_gene_symbol	DIOPT_score	OMIM_Phenotype_IDs	OMIM_Phenotype_IDs[name]
        rows = fb_orthologs_table.get_rows()
        no_map_id = 0
        total_rows = len(rows)
        for row in rows:
            props = {}
            source = row[0]
            external_id = row[2]
            
            target = external_id #default
            if self.gene_id_map:
                try:
                    target = self.gene_id_map[external_id]
                except KeyError as ke:
                    no_map_id += 1
                    logger.info(
                        f'orthology_adapter.py::OrthologyAdapter::get_edges: failed to map ID: {external_id}\n'
                        f'Missing data in row: {row}\n'
                        f'Unmapped count: {no_map_id} / {total_rows}'                    
                    )
                    continue

            props['hsa_hgnc_id'] = external_id
            props['hsa_omim_id'] = row[3] if len(row) > 3 else None
            props['hsa_hgnc_symbol'] = row[4] if len(row) > 4 else None
            props['DIOPT_score'] = int(row[5]) if len(row) > 5 and row[5].isdigit() else None
            props['hsa_omim_phenotype_ids'] = row[6] if len(row) > 6 else None
            props['hsa_omim_phenotype_ids_names'] = row[7] if len(row) > 7 else None
            
            props['source_organism'] = self.source_organism_name
            props['target_organism'] = self.target_organism_name
            
            yield f'{self.source_prefix}:{source}', f'{self.target_prefix}:{target}', self.label, props
