'''
# FB data: https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Human_disease_model_data_.28disease_model_annotations_fb_.2A.tsv.gz.29
disease model:
  represented_as: node
  preferred_id: do_id
  input_label: disease_model
  is_a: biological entity
  #inherit_properties: true
  properties:
    taxon_id: int                        # 7227 for dmel / 9606 for hsa    (TO BE INHERITED BY ALL TYPES)
    gene_id: str
    do_qualifier: str
    do_term_id: str                        # only this should be enough
    do_term_name: str                        # this is in the DO
    allele_id: str
    ortholog_hgnc_id: str
    ortholog_hgnc_symbol: str
    evidence_code: str

## FBgn ID	Gene symbol	HGNC ID	DO qualifier	DO ID	DO term	Allele used in model (FBal ID)	Allele used in model (symbol)	Based on orthology with (HGNC ID)	Based on orthology with (symbol)	Evidence/interacting alleles	Reference (FBrf ID)
FBgn0000008	a		exacerbates	DOID:2129	atypical teratoid rhabdoid tumor	FBal0299140	a[HMC03685]			modeled by FLYBASE:Snr1[KK101602]; FB:FBal0231379	FBrf0241270
FBgn0000014	abd-A		ameliorates	DOID:1319	brain cancer	FBal0346203	abd-A[UAS.Tag:HA]			modeled by FLYBASE:pros[JF02308]; FB:FBal0220677	FBrf0253491
FBgn0000014	abd-A		model of	DOID:1240	leukemia	FBal0346203	abd-A[UAS.Tag:HA]			CEA	FBrf0247817
FBgn0000014	abd-A		model of	DOID:1240	leukemia	FBal0346203	abd-A[UAS.Tag:HA]			is ameliorated by FLYBASE:Psc[1]; FB:FBal0013980	FBrf0247817
FBgn0000014	abd-A		model of	DOID:1240	leukemia	FBal0346203	abd-A[UAS.Tag:HA]			is ameliorated by FLYBASE:esc[2]; FB:FBal0003822	FBrf0247817
FBgn0000015	Abd-B		model of	DOID:0111568	congenital vertical talus			HGNC:5133	HOXD10	IEA	FBrf0241599
FBgn0000017	Abl		ameliorates	DOID:12858	Huntington's disease	FBal0095895	Abl[UAS.cFa]			modeled by FLYBASE:Hsap\HTT[128Q.1-231.UAS]; FB:FBal0368452	FBrf0249076

'''

from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
#from flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
#from biocypher._logger import logger
import re

class DiseaseModelAdapter(Adapter):
#class DiseaseModelAdapter():

    def __init__(self, write_properties, add_provenance, dmel_filepath=None):
        self.dmel_filepath = dmel_filepath
        self.label = 'dmel_disease_model'
        #self.type = 'DiseaseModel'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        super(DiseaseModelAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        fb_gg_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fb_gg_table.extract_date_string(self.dmel_filepath)
        #header:
        #FBgn ID	Gene symbol	HGNC ID	DO qualifier	DO ID	DO term	Allele used in model (FBal ID)	Allele used in model (symbol)	Based on orthology with (HGNC ID)	Based on orthology with (symbol)	Evidence/interacting alleles	Reference (FBrf ID)FBgn ID	Gene symbol	HGNC ID	DO qualifier	DO ID	DO term	Allele used in model (FBal ID)	Allele used in model (symbol)	Based on orthology with (HGNC ID)	Based on orthology with (symbol)	Evidence/interacting alleles	Reference (FBrf ID)
        rows = fb_gg_table.get_rows()
        id = -1
        for row in rows:
            id += 1
            props = {}
            props['taxon_id'] = 7227
            props['gene_id'] = row[0]
            #if row[2] != '':
            props['gene_hgnc_id'] = row[2]
            #if row[3] != '':
            props['do_qualifier'] = row[3]
            props['do_term_id'] = row[4]
            #if row[5] != '':
            props['do_term_name'] = row[5]
            #if row[6] != '':
            props['allele_id'] = row[6]
            #if row[8] != '':
            props['ortholog_hgnc_id'] = row[8]
            #if row[9] != '':
            props['ortholog_hgnc_symbol'] = row[9]
            #if row[10] != '':
            ev_code, alleles = self.__extract_evcode_alleles(row[10])
            if ev_code!=None:
                props['evidence_code'] = ev_code
            if alleles != None:
                props['interacting_alleles'] = alleles
            props['ev_code_interact_alleles'] = row[10]
            props['reference_id'] = row[11]
            #print(f'{props}')
            yield f'{self.label}_{id}', self.label, props



    def __extract_evcode_alleles(self, input_string):
        # Regular expression to capture the evidence_code (three uppercase letters at the beginning of the string)
        evcode_pattern = re.compile(r'^\b([A-Z]{3})\b')
        # Regular expression to capture the interacting_alleles (FBal followed by 7 to 9 digits)
        alleles_pattern = re.compile(r'(FBal\d{7,9})')

        # Find the evidence_code at the beginning of the string
        evcode_match = evcode_pattern.match(input_string)
        evidence_code = evcode_match.group(0) if evcode_match else None

        # Find all interacting_alleles
        interacting_alleles = alleles_pattern.findall(input_string)
        interacting_alleles = interacting_alleles if interacting_alleles else None

        return evidence_code, interacting_alleles
    '''                                                                                                                                                                                                ''''''

gga = DiseaseModelAdapter(False,True,
'/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/flybase/disease_model_annotations_fb_2024_02.tsv.gz')

gga.get_nodes()
    '''