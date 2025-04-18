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

    def __init__(self, filepath, label, write_properties, add_provenance, taxon_id = None):
        """
            If taxon_id == None then all pathways in the get_edges::organism_taxon_map will be 
            translated to be fed into atom space.
            Otherwise, only the specified species'  data will be processed.
        """
        if label not in ReactomeAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid label. Allowed values: ' +
                             ', '.join(ReactomeAdapter.ALLOWED_LABELS))
        self.filepath = filepath
        self.dataset = label
        self.label = label
        self.taxon_id = taxon_id
        self.source = "REACTOME"
        self.source_url = "https://reactome.org"

        super(ReactomeAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        organism_taxon_map = {
            'R-DME': 7227,  # Drosophila melanogaster (dmel)
            'R-HSA': 9606,  # Homo sapiens (hsa)
            # Add more organisms here as needed
        }

        with open(self.filepath) as input:
            base_props = {}
            if self.write_properties and self.add_provenance:
                base_props['source'] = self.source
                base_props['source_url'] = self.source_url
            for line in input:
                data = line.strip().split('\t')

                if self.label == 'genes_pathways':
                    entity_id, pathway_id = data[0], data[1]

                    prefix = pathway_id[:5]  # Extract prefix (e.g., R-DME, R-HSA)
                    if prefix in organism_taxon_map:
                        taxon_id = organism_taxon_map[prefix]
                        if self.taxon_id == None:
                            source_type = self._get_entity_type(entity_id)                        
                            props = base_props.copy()
                            props['taxon_id'] = taxon_id
                            if prefix == 'R-HSA':
                                entity_id = entity_id.split('.')[0]
                            source = (source_type, entity_id)
                            target = pathway_id

                            yield source, target, self.label, props


                else:
                    parent, child = data[0], data[1]

                    prefix = parent[:5]  # Extract prefix from parent id
                    if prefix in organism_taxon_map:
                        taxon_id = organism_taxon_map[prefix]
                        props = base_props.copy()
                        props['taxon_id'] = taxon_id

                        if self.label == 'parent_pathway_of':
                            source, target = parent, child
                        elif self.label == 'child_pathway_of':
                            source, target = child, parent

                        yield source, target, self.label, props

    
    def _get_entity_type(self, entity_id):
        """Return the entity type based on its identifier prefix."""
        if entity_id.startswith(("FBgn", "ENSG")):
            return "gene"
        elif entity_id.startswith(("FBpp", "ENSP")):
            return "protein"
        else:
            return "transcript"

    # def get_edges(self):
    #     with open(self.filepath) as input:
    #         _props = {}
    #         if self.write_properties and self.add_provenance:
    #             _props['source'] = self.source
    #             _props['source_url'] = self.source_url
    #         for line in input:
    #             if self.label == 'genes_pathways':
    #                 data = line.strip().split('\t')
    #                 pathway_id = data[1]
    #                 if pathway_id.startswith('R-HSA'):
    #                     ensg_id = data[0].split('.')[0]
    #                     _id = ensg_id + '_' + pathway_id
    #                     _source = ensg_id
    #                     _target = pathway_id
    #                     yield _source, _target, self.label, _props
    #             else:
    #                 parent, child = line.strip().split('\t')
    #                 if parent.startswith('R-HSA'):
    #                     if self.label == 'parent_pathway_of':
    #                         _id = parent + '_' + child
    #                         _source = parent
    #                         _target = child

    #                         yield _source, _target, self.label, _props
    #                     elif self.label == 'child_pathway_of':
    #                         _id = child + '_' + parent
    #                         _source =  child
    #                         _target =  parent
    #                         yield  _source, _target, self.label, _props
