import gzip
from Bio import SeqIO
from biocypher_metta.adapters import Adapter

# Data file for genes_pathways: https://reactome.org/download/current/Ensembl2Reactome_All_Levels.txt
# data format:
# Source database identifier, e.g. UniProt, ENSEMBL, NCBI Gene or ChEBI identifier
# Reactome Pathway Stable identifier
# URL
# Event (Pathway or Reaction) Name
# Evidence Code
# Species

# Example file:
# ENSDART00000193986	R-DRE-5653656	https://reactome.org/PathwayBrowser/#/R-DRE-5653656	Vesicle-mediated transport	IEA	Danio rerio
# ENSG00000000419	R-HSA-162699	https://reactome.org/PathwayBrowser/#/R-HSA-162699	Synthesis of dolichyl-phosphate mannose	TAS	Homo sapiens
# ENSG00000000419	R-HSA-163125	https://reactome.org/PathwayBrowser/#/R-HSA-163125	Post-translational modification: synthesis of GPI-anchored proteins	TAS	Homo sapiens
# ENSG00000000419	R-HSA-1643685	https://reactome.org/PathwayBrowser/#/R-HSA-1643685	Disease	TAS	Homo sapiens
# ENSG00000000419.14	R-HSA-162699	https://reactome.org/PathwayBrowser/#/R-HSA-162699	Synthesis of dolichyl-phosphate mannose	TAS	Homo sapiens

# Data file for parent_pathway_of and child_pathway_of: https://reactome.org/download/current/ReactomePathwaysRelation.txt
# example file:
# R-BTA-109581	R-BTA-109606
# R-BTA-109581	R-BTA-169911
# R-BTA-109581	R-BTA-5357769
# R-BTA-109581	R-BTA-75153
# R-BTA-109582	R-BTA-140877


class ReactomeAdapter(Adapter):

    ALLOWED_LABELS = ['genes_pathways',
                      'parent_pathway_of', 'child_pathway_of']

    def __init__(self, filepath, label, write_properties, add_provenance, ensembl_uniprot_map_path=None):
        if label not in ReactomeAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid label. Allowed values: ' +
                             ', '.join(ReactomeAdapter.ALLOWED_LABELS))
        self.filepath = filepath
        self.dataset = label
        self.label = label
        self.source = "REACTOME"
        self.source_url = "https://reactome.org"
        
        # Load the Ensembl to UniProt mapping if provided
        self.ensembl_uniprot_map = {}
        if ensembl_uniprot_map_path:
            try:
                import pickle
                with open(ensembl_uniprot_map_path, 'rb') as f:
                    self.ensembl_uniprot_map = pickle.load(f)
                print(f"Loaded {len(self.ensembl_uniprot_map)} Ensembl-UniProt mappings")
            except Exception as e:
                print(f"Warning: Could not load Ensembl-UniProt mapping: {e}")
                self.ensembl_uniprot_map = {}

        super(ReactomeAdapter, self).__init__(write_properties, add_provenance)

    def _get_source_type_and_id(self, ensembl_id):
        # Remove version number if present (e.g., ENSG00000000419.14 -> ENSG00000000419)
        clean_id = ensembl_id.split('.')[0]
        
        if clean_id.startswith('ENSG'):
            return 'gene', clean_id
        elif clean_id.startswith('ENST'):
            return 'transcript', clean_id
        elif clean_id.startswith('ENSP'):
            # Skip protein entries if no UniProt mapping is available
            if self.ensembl_uniprot_map and clean_id in self.ensembl_uniprot_map:
                uniprot_id = self.ensembl_uniprot_map[clean_id]
                return 'protein', uniprot_id
            else:
                # Skip protein entries without UniProt mapping
                return None
        else:
            # Skip non-human IDs
            return None

    def get_edges(self):
        with open(self.filepath) as input:
            _props = {}
            if self.write_properties and self.add_provenance:
                _props['source'] = self.source
                _props['source_url'] = self.source_url
                
            for line in input:
                if self.label == 'genes_pathways':
                    data = line.strip().split('\t')
                    pathway_id = data[1]
                    
                    # Only process human pathways (R-HSA prefix)
                    if pathway_id.startswith('R-HSA'):
                        ensembl_id = data[0]
                        result = self._get_source_type_and_id(ensembl_id)
                        
                        # Skip if not a human Ensembl ID or protein without UniProt mapping
                        if result is None:
                            continue
                            
                        source_type, formatted_source_id = result
                        target_id = pathway_id
                        
                        # Return the source with type information for BioCypher
                        yield (source_type, formatted_source_id), target_id, self.label, _props
                        
                else:
                    # Handle pathway-pathway relationships
                    parent, child = line.strip().split('\t')
                    if parent.startswith('R-HSA'):
                        
                        if self.label == 'parent_pathway_of':
                            yield parent, child, self.label, _props
                        elif self.label == 'child_pathway_of':
                            yield child, parent, self.label, _props