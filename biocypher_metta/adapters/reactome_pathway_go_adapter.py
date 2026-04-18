from biocypher_metta.adapters import Adapter
from biocypher_metta.processors import GOSubontologyProcessor

# Example Pathways2GoTerms_Human  TXT input files
# Identifier	Name	GO_Term
# R-HSA-73843	5-Phosphoribose 1-diphosphate biosynthesis	GO:0006015
# R-HSA-1369062	ABC transporters in lipid homeostasis	GO:0006869
# R-HSA-382556	ABC-family proteins mediated transport	GO:0055085
# R-HSA-9660821	ADORA2B mediated anti-inflammatory cytokines production	GO:0002862
# R-HSA-418592	ADP signalling through P2Y purinoceptor 1	GO:0030168
# R-HSA-392170	ADP signalling through P2Y purinoceptor 12	GO:0030168
# R-HSA-198323	AKT phosphorylates targets in the cytosol	GO:0043491


# Reaction to GO Term file:
# Identifier	Name	GO_Term
# R-HSA-1008248	Adenylate Kinase 3 is a GTP-AMP phosphotransferase	GO:0046899
# R-HSA-1013012	Binding of Gbeta/gamma to GIRK/Kir3 channels	GO:0004965
# R-HSA-1013013	Association of GABA B receptor with G protein beta-gamma subunits	GO:0004965
# R-HSA-1013020	Activation of GIRK/Kir3 Channels	GO:0015467
# R-HSA-1022129	ST3GAL4 transfers Neu5Ac to terminal Gal of N-glycans	GO:0003836
# R-HSA-1022133	ST8SIA2,3,6 transfer Neu5Ac to terminal Gal of N-glycans	GO:0003828
# R-HSA-1028788	FUT8 transfers fucosyl group from GDP-Fuc to GlcNAc of NGP	GO:0008424


class ReactomePathwayGOAdapter(Adapter):
    """
    Adapter for Reactome Pathway or Reaction to specific GO subontology mappings.
    Filters pathways or reactions to only include terms from the specified subontology.
    """
    
    def __init__(self, filepath, write_properties, add_provenance, label, taxon_id,
                 subontology, go_subontology_processor=None):
        super().__init__(write_properties, add_provenance)

        if subontology not in ['biological_process', 'molecular_function', 'cellular_component']:
            raise ValueError("Invalid subontology specified")

        self.filepath = filepath
        self.label = label
        self.taxon_id = taxon_id
        self.subontology = subontology
        self.label = label if label else f"pathway_to_{subontology}"
        self.source = "REACTOME"
        self.source_url = "https://reactome.org"
        self.skip_first_line = True

        # Use provided GO subontology processor or create new one
        if go_subontology_processor is None:
            self.go_subontology_processor = GOSubontologyProcessor()
            self.go_subontology_processor.load_or_update()
        else:
            self.go_subontology_processor = go_subontology_processor

        self.subontology_mapping = self.go_subontology_processor.mapping

    def get_edges(self):
        with open(self.filepath) as f:
            if self.skip_first_line:
                next(f)
                
            processed = 0;
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
                
                processed += 1
                # Only yield edges that match the specified subontology
                yield (
                    f"{pathway_id}",  # source
                    full_go_term,     # target
                    self.label,       # label from config
                    properties
                )
            print(f'Processed records: {processed}')