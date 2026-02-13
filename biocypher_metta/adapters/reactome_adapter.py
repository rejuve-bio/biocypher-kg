#import requests
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import JSONDecodeError
from tqdm import tqdm
import csv
from biocypher_metta.adapters import Adapter
import psycopg2

# Example ***pathway*** input file:
# R-GGA-199992	trans-Golgi Network Vesicle Budding	Gallus gallus
# R-HSA-164843	2-LTR circle formation	Homo sapiens
# R-HSA-73843	5-Phosphoribose 1-diphosphate biosynthesis	Homo sapiens
# R-HSA-1971475	A tetrasaccharide linker sequence is required for GAG synthesis	Homo sapiens
# R-HSA-5619084	ABC transporter disorders	Homo sapiens

# for reaction nodes, we're using files that map DB entities to their Reactome reactions, such as (Ensembl2ReactomeReactions.txt):
# ENSG00000055950	R-HSA-9937724	https://reactome.org/PathwayBrowser/#/R-HSA-9937724	55S mitochondrial ribosome:non-stop mRNA binds ICT1 (MRPL58)	TAS	Homo sapiens
# ENST00000000233	R-HSA-6807866	https://reactome.org/PathwayBrowser/#/R-HSA-6807866	GBF1 recruits ARF:GDP to the ERGIC	TAS	Homo sapiens
# ENSP00000038176	R-HSA-5626982	https://reactome.org/PathwayBrowser/#/R-HSA-5626982	TNF-α:TNFR1:NSMAF binds RACK1	TAS	Homo sapiens
# FBgn0000008	R-DME-983140	https://reactome.org/PathwayBrowser/#/R-DME-983140	Transfer of Ub from E2 to substrate and release of E2	IEA	Drosophila melanogaster
# FBpp0070043	R-DME-8948039	https://reactome.org/PathwayBrowser/#/R-DME-8948039	FUNDC1 is phosphorylated by CK2	IEA	Drosophila melanogaster
# FBpp0070044	R-DME-6791219	https://reactome.org/PathwayBrowser/#/R-DME-6791219	32S pre-rRNA is nucleolytically processed at site 4 (4a) to yield 12S pre-rRNA and 28S rRNA	IEA	Drosophila melanogaster


class ReactomeAdapter(Adapter):

    def __init__(self, filepath, pubmed_map_path, write_properties, add_provenance, label, taxon_id, name_filepath=None):

        self.filepath = filepath
        self.pubmed_map_path = pubmed_map_path
        self.load_pubmed_map()
        self.label = label
        # self.dataset = 'pathway'
        self.source = "REACTOME"
        self.source_url = "https://reactome.org"
        self.taxon_id = taxon_id
        super(ReactomeAdapter, self).__init__(write_properties, add_provenance)
    
    def get_nodes(self):
        if self.label == 'pathway':
            with open(self.filepath) as input:
                for line in input:
                    id, name, species = line.strip().split('\t')                
                    pathway_id = f"{id}"
                    # if self.taxon_id == None:           # this could be used to load pathway for all available species
                    if self.taxon_id == 9606:
                        if species == 'Homo sapiens':
                            props = {}
                            if self.write_properties:
                                props['pathway_name'] = name                             
                                pubmed_id = self.pubmed_map.get(id, None)
                                if pubmed_id is not None:
                                    pubmed_url = f"https://pubmed.ncbi.nlm.nih.gov/{self.pubmed_map[id]}"
                                    props['evidence'] = pubmed_url
                                
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url
                                    props['taxon_id'] = f'{self.taxon_id}'
                            yield pathway_id, self.label, props
                    elif self.taxon_id == 7227:           
                        if species == 'Drosophila melanogaster':
                            props = {}
                            if self.write_properties:
                                props['pathway_name'] = name                        
                                pubmed_id = self.pubmed_map.get(id, None)
                                if pubmed_id is not None:
                                    pubmed_url = f"https://pubmed.ncbi.nlm.nih.gov/{self.pubmed_map[id]}"
                                    props['evidence'] = pubmed_url                                
                                if self.add_provenance:
                                    props['source'] = self.source
                                    props['source_url'] = self.source_url
                                    props['taxon_id'] = f'{self.taxon_id}'

                            yield pathway_id, self.label, props
        elif self.label == 'reaction':
            organism_taxon_map = {
                'R-DME': 7227,  # Drosophila melanogaster (dmel)
                'R-HSA': 9606,  # Homo sapiens (hsa)
                'R-CEL': 6239,  # Caenorhabditis elegans (cel)
                # Add more organisms here as needed
                'R-MMU': 10090,   # Mus musculus (mmu)
                'R-RNO': 10116,   # Rattus norvegicus
            }   
            # nodes = set()   
            with open(self.filepath) as input_file:
                base_props = {}                
                if self.write_properties and self.add_provenance:
                    base_props['taxon_id'] = self.taxon_id
                    base_props['source'] = self.source
                    base_props['source_url'] = self.source_url
                for line in input_file:
                    data = line.strip().split('\t')                       
                    yield from self._get_reaction_nodes(data, organism_taxon_map, base_props)

    def load_pubmed_map(self):
        self.pubmed_map = {}
        with open(self.pubmed_map_path, "r") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                pathway_id, pubmed_id = row[0], row[0]
                self.pubmed_map[pathway_id] = pubmed_id


    def _get_reaction_nodes(self, data, organism_taxon_map, base_props):               
        reaction_id = data[1]
        organism_pathway_prefix = reaction_id[:5]  # e.g., 'R-DME', 'R-HSA'
        
        reaction_id = f'{reaction_id}'
        if organism_pathway_prefix in organism_taxon_map:
            if self.taxon_id == organism_taxon_map[organism_pathway_prefix]:
                props = base_props.copy()
                props['reaction_name'] = data[3]
                props['evidence'] = data[4]
                props['reaction_url'] = data[2].replace("PathwayBrowser/#", "content/detail")
                props['taxon_id'] = f'{self.taxon_id}'
                pubmed_id = self.pubmed_map.get(reaction_id, None)
                if pubmed_id is not None:
                    pubmed_url = f"https://pubmed.ncbi.nlm.nih.gov/{pubmed_id}"
                    props['pubmed_url'] = pubmed_url
                yield reaction_id, self.label, props
        elif organism_pathway_prefix == 'R-NUL':
            # Drosophila only
            props = base_props.copy()
            props['evidence'] = data[4]
            props['reaction_url'] = data[2].replace("PathwayBrowser/#", "content/detail")
            props['taxon_id'] = f'{self.taxon_id}'
            if self.taxon_id == 7227 and data[5] == 'Drosophila melanogaster' and data[0].startswith('FB'):
                yield reaction_id, self.label, props


