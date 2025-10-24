import os
import pickle
from biocypher_metta.adapters import Adapter

# Example Pathways2GoTerms_Human  TXT input files
# Identifier	Name	GO_Term
# R-HSA-73843	5-Phosphoribose 1-diphosphate biosynthesis	GO:0006015
# R-HSA-1369062	ABC transporters in lipid homeostasis	GO:0006869
# R-HSA-382556	ABC-family proteins mediated transport	GO:0055085
# R-HSA-9660821	ADORA2B mediated anti-inflammatory cytokines production	GO:0002862
# R-HSA-418592	ADP signalling through P2Y purinoceptor 1	GO:0030168
# R-HSA-392170	ADP signalling through P2Y purinoceptor 12	GO:0030168
# R-HSA-198323	AKT phosphorylates targets in the cytosol	GO:0043491

class ReactomePathwayGOAdapter(Adapter):
    """
    Adapter for Reactome Pathway to specific GO subontology mappings.
    Filters pathways to only include terms from the specified subontology.
    """
    
    def __init__(self, filepath, write_properties, add_provenance, taxon_id,
                 subontology, mapping_file='aux_files/go_subontology_mapping.pkl'):
        super().__init__(write_properties, add_provenance)
        
        if subontology not in ['biological_process', 'molecular_function', 'cellular_component']:
            raise ValueError("Invalid subontology specified")
            
        self.filepath = filepath
        self.taxon_id = taxon_id
        self.subontology = subontology
        self.source = "REACTOME"
        self.source_url = "https://reactome.org"
        self.skip_first_line = True
        
        # Load GO subontology mapping
        if not os.path.exists(mapping_file):
            raise FileNotFoundError(f"Mapping file not found: {mapping_file}")
            
        with open(mapping_file, 'rb') as f:
            self.subontology_mapping = pickle.load(f)

    def get_edges(self):
        with open(self.filepath) as f:
            if self.skip_first_line:
                next(f)
                
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) < 3:
                    continue
                    
                pathway_id, pathway_name, go_term = parts[0], parts[1], parts[2]
                
                # Clean and standardize GO term
                clean_go_term = go_term.replace('GO:', '').replace('go:', '')
                full_go_term = f"GO:{clean_go_term}"
                
                if not pathway_id.startswith('R-HSA'):
                    continue
                
                # Get subontology from mapping - try different variations
                go_type = None
                for term_variant in [full_go_term, go_term, clean_go_term]:
                    if term_variant in self.subontology_mapping:
                        go_type = self.subontology_mapping[term_variant]
                        break
                
                # Skip if GO term not found in mapping or doesn't match target subontology
                if go_type is None or go_type != self.subontology:
                    continue
                
                # Prepare base properties
                properties = {
                    'pathway_name': pathway_name,
                    'go_term_id': full_go_term,
                    'subontology': go_type, 
                }
                
                if self.add_provenance:  
                    properties.update({
                        'source': self.source,
                        'source_url': self.source_url
                    })
                
                # Only yield edges that match the specified subontology
                yield (
                    f"REACT:{pathway_id}",  # source
                    full_go_term,           # target
                    f"pathway_to_{go_type}", # label
                    properties
                )