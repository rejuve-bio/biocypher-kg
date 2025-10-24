'''
# https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data_.28genotype_phenotype_data_.2A.tsv.29

# https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Phenotypic_data_.28genotype_phenotype_data_.2A.tsv.29

# For Flybase data this should be a link between a set (list) of allele ids representing the genotype and a set(list)
# of phenotypes ids. Of course, we could create a link from Gene to Phenotype (like Monarch) because of the natural
# relationship between a gene and its alleles.

################################   Schemata translated in this adapter  ################################

# From the site above:
# For cases where the genotype contains more than one component, then the components are separated as follows (columns 1 and 2):
#  * Homozygous or transheterozygous combinations of classical/insertional alleles at a single locus are separated by a '/'.
#  * Hemizygous combinations affecting a single locus (classical/insertional allele over a deficiency for that locus) are separated by a '/'.
#  * Heterozygosity for a classical/insertional allele or aberration is represented by '/+'.
#  * In all other cases, other genotype components (e.g. drivers, transgenic alleles) are separated by a space.

genotype:
  represented_as: node
  input_label: genotype
  is_a: biological entity
  properties:
    genotype_ids: str           # string composed of one or more allele id(s) (FBal#)
    genotype_symbols: str       # string composed of one or more allele symbol(s)
    reference: str              # FBrf# literature ref id
    taxon_id: int               # 7227 for dmel / 9606 for hsa


phenotype:
  represented_as: node
  input_label: phenotype
  is_a: biological entity
  properties:
    phenotype_ontology_id: ontology term    
    qualifier_ids: ontology term[]         # zero or more FBcv# and/or FBdv# to add information to the phenotype.
    taxon_id: int                        # 7227 for dmel / 9606 for hsa 


phenotype to genotype:
  represented_as: edge
  input_label: genetically_informed_by
  is_a: annotation
  source: phenotype
  target: genotype
  properties:
    miniref: str
    fb_ref: str                     # FBrf#
    pmid_ref: str
    pmcid_ref: str
    doi: str
    # genotype_phenotype_id: genotype phenotype   # FB source of 'full' data record.
    taxon_id: int                        # 7227 for dmel / 9606 for hsa 

phenotype to ontology:
  represented_as: edge
  input_label: characterized_by
  is_a: annotation
  source: phenotype
  target: ontology term       # zero or more FBcv# and/or FBdv# to add information to the phenotype.
  properties:
    taxon_id: int                        # 7227 for dmel / 9606 for hsa 

    
allele to phenotype:
  represented_as: edge
  input_label: involved_in
  is_a: annotation
  source: allele
  target: phenotype
  properties:
    miniref: str
    fb_ref: str                     # FBrf#
    pmid_ref: str
    pmcid_ref: str
    doi: str
    # genotype_phenotype_id: genotype phenotype   # FB source of 'full' data record.
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
import pickle

class GenotypePhenotypeAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, label, dmel_filepath, dmel_fbrf_filepath):
        self.dmel_filepath = dmel_filepath
        self.fbrf_to_pmid_pmcid_doi_dict = self.__build_fbrf_to_pmid_pmcid_doi_dict(dmel_fbrf_filepath)
        self.label = label  
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'
        super(GenotypePhenotypeAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        fb_gp_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fb_gp_table.extract_date_string(self.dmel_filepath)
        #header:
        #genotype_symbols	genotype_FBids	phenotype_name	phenotype_id	qualifier_names	qualifier_ids	reference
        rows = fb_gp_table.get_rows()

        if self.label == 'genotype':
            id = -1
            for row in rows:
                id += 1
                props = {}
                props['genotype_ids'] = row[1].replace(' ', '@')
                props['genotype_symbols'] = row[0].replace(' ', '@')                    
                props['reference'] = f'http://flybase.org/reports/{row[6]}.htm'
                props['taxon_id'] = 7227
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                yield f'REJUVEBIO:genotype_{id}', self.label, props
        elif self.label == 'phenotype':
            id = -1
            for row in rows:
                id += 1
                props = {}
                props['phenotype_ontology_id'] = row[3].replace(':', '_').lower()   # onto: fbbt or fbcv  
                # props['genotype_phenotype_id'] = f'genotype_phenotype_{id}'
                # props['qualifier_ids'] = []
                # if row[5] != '':
                #     props['qualifier_ids'] = [ name.replace(':', '_').lower() for name in row[5].split('|') ]
                # print(f"PHENOTYPE: {props['qualifier_ids']}")
                props['reference'] = f'http://flybase.org/reports/{row[6]}.htm'
                props['taxon_id'] = 7227
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                yield f'REJUVEBIO:phenotype_{id}', self.label, props

    
    def get_edges(self):
        fb_gp_table = FlybasePrecomputedTable(self.dmel_filepath)
        self.version = fb_gp_table.extract_date_string(self.dmel_filepath)
        #header:
        #genotype_symbols	genotype_FBids	phenotype_name	phenotype_id	qualifier_names	qualifier_ids	reference
        rows = fb_gp_table.get_rows()

        # Links all alleles in a genotype_phenotype data to all phenotypes they express in order to reach genes through the alleles in the phenotype...
        if  self.label == 'involved_in':    # allele to phenotype  schema
            id = -1
            for row in rows:
                id += 1
                props = {}
                if row[6] in self.fbrf_to_pmid_pmcid_doi_dict:
                    ref = self.fbrf_to_pmid_pmcid_doi_dict[row[6]]
                    props['miniref'] = ref['miniref']
                    props['fb_ref'] = f'http://flybase.org/reports/{row[6]}.htm'                 # FBrf#
                    props['pmid_ref'] = ref['pmid']
                    props['pmcid'] = ref['pmcid']
                    props['doi'] = ref['doi']
                else:
                    props['fb_ref'] = row[6]                     # FBrf# only
                # props['genotype_phenotype_id'] = f'genotype_phenotype_{id}'
                props['taxon_id'] = 7227
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                alleles = self.get_alleles(row[1])          # gets a list of allele ids from  genotype's  genotype_ids
                for allele in alleles:
                    yield f'FLYBASE:{allele}', f'REJUVEBIO:phenotype_{id}', self.label, props    

        elif self.label == 'genetically_informed_by':                     # phenotype to genotype schema
            id = -1
            for row in rows:
                id += 1
                props = {}
                if row[6] in self.fbrf_to_pmid_pmcid_doi_dict:
                    ref = self.fbrf_to_pmid_pmcid_doi_dict[row[6]]
                    props['miniref'] = ref['miniref']
                    props['fb_ref'] = f'http://flybase.org/reports/{row[6]}.htm'                 # FBrf#
                    props['pmid_ref'] = ref['pmid']
                    props['pmcid'] = ref['pmcid']
                    props['doi'] = ref['doi']
                else:
                    props['fb_ref'] = row[6]                     # FBrf# only
                # props['genotype_phenotype_id'] = f'genotype_phenotype_{id}'
                props['taxon_id'] = 7227
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url
                yield f'REJUVEBIO:phenotype_{id}', f'REJUVEBIO:genotype_{id}', self.label, props    

        elif self.label == 'characterized_by':                          # phenotype to ontology schema (except cellular_component)
            id = -1
            for row in rows:
                id += 1
                props = {}     
                if row[6] in self.fbrf_to_pmid_pmcid_doi_dict:
                    ref = self.fbrf_to_pmid_pmcid_doi_dict[row[6]]
                    props['miniref'] = ref['miniref']
                    props['fb_ref'] = f'http://flybase.org/reports/{row[6]}.htm'                 # FBrf#
                    props['pmid_ref'] = ref['pmid']
                    props['pmcid'] = ref['pmcid']
                    props['doi'] = ref['doi']
                else:
                    props['fb_ref'] = row[6]  
                props['taxon_id'] = 7227
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url

                phenotype_ontology_id = row[3]   # onto: fbbt or fbcv or  fbdv  or go...
                if row[5] != '':
                    props['qualifier_term_ids'] = [ name for name in row[5].split('|') ]

                # if go's subontology is 'cellular_component', 'characterized_by' edge label seems 
                # to be not the best name: use inheres_in (from PATO)
                if phenotype_ontology_id.startswith('go'):
                    sub_onto_go = self.go_subontology(phenotype_ontology_id)
                    if sub_onto_go == 'cellular_component':
                        continue
                #         self.label = 'inheres_in'
                #         yield f'phenotype_{id}', phenotype_ontology_id, self.label, props            
                    # if phenotype_ontology_id == 'go_0016028':
                    #     print(f'genotype_phenotype::characterized_by edge:\nOntology different from FB ontos: {phenotype_ontology_id}')
                    #     print('Yielding it for neo4j_csv_writer')
                # else:
                yield f'REJUVEBIO:phenotype_{id}', phenotype_ontology_id, self.label, props    
                
                if row[5] != '':                                   
                    terms_ids = [ t_id for t_id in row[5].split('|') ]      #multiple ontology ids
                    for term_id in terms_ids:
                        yield f'REJUVEBIO:phenotype_{id}', term_id, self.label, props    
                    #     props['qualifier_term_ids'].append( (term_id.split('_')[0], term_id) )
        
        elif self.label == 'inheres_in':
            id = -1
            for row in rows:
                id += 1
                phenotype_ontology_id = row[3]  
                # if go's subontology is 'cellular_component', 'characterized_by' edge label seems 
                # to be not the best name: use inheres_in (from PATO)
                if phenotype_ontology_id.startswith('go'):
                    sub_onto_go = self.go_subontology(phenotype_ontology_id)
                    if sub_onto_go != 'cellular_component':
                        continue                
                props = {}     
                if row[6] in self.fbrf_to_pmid_pmcid_doi_dict:
                    ref = self.fbrf_to_pmid_pmcid_doi_dict[row[6]]
                    props['miniref'] = ref['miniref']
                    props['fb_ref'] = f'http://flybase.org/reports/{row[6]}.htm'                 # FBrf#
                    props['pmid_ref'] = ref['pmid']
                    props['pmcid'] = ref['pmcid']
                    props['doi'] = ref['doi']
                else:
                    props['fb_ref'] = row[6]  
                props['taxon_id'] = 7227
                if self.add_provenance:
                    props['source'] = self.source
                    props['source_url'] = self.source_url                
                if row[5] != '':
                    props['qualifier_term_ids'] = [ name for name in row[5].split('|') ]

                yield f'REJUVEBIO:phenotype_{id}', phenotype_ontology_id, self.label, props    


    def get_alleles(self, geno_ids: str) -> list[str]:
        '''
        According to Flybase, geno_ids is separated (or conected) by:
           * Homozygous or transheterozygous combinations of classical/insertional alleles at a single locus are separated by a '/'.
            * Hemizygous combinations affecting a single locus (classical/insertional allele over a deficiency for that locus) are separated by a '/'.
            * Heterozygosity for a classical/insertional allele or aberration is represented by '/+'.
            * In all other cases, other genotype components (e.g. drivers, transgenic alleles) are separated by a space.
        '''
        return re.split(r" |/|\+/", geno_ids) 
    

    def __build_fbrf_to_pmid_pmcid_doi_dict(self, fbrf_refs_file: str) -> dict[str, {str, str}]:
        table_fbef_to_refs = FlybasePrecomputedTable(fbrf_refs_file)
        fbrf_to_refs_dict = {}
        for row in table_fbef_to_refs.get_rows():
            fbrf_to_refs_dict[row[0]] = {
                'miniref': row[5],
                'pmid': row[1],
                'pmcid': row[2],
                'doi': row[3]
            }
        return fbrf_to_refs_dict
    
    def go_subontology(self, go_id):
        go_mapping_file = './aux_files/go_subontology_mapping.pkl'
        with open(go_mapping_file, 'rb') as f:
            go_subontology_mapping = pickle.load(f)
        return go_subontology_mapping[go_id]
