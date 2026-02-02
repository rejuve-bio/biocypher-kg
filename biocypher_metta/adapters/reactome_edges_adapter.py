# 2025/09: started changes to handle multiple species data.

import psycopg2
from biocypher_metta.adapters import Adapter

# Data file for genes_pathways: https://reactome.org/download/current/Ensembl2Reactome_All_Levels.txt
# data format:
# Source database identifier, e.g. UniProt, ENSEMBL, NCBI Gene or ChEBI identifier
# Reactome Pathway Stable identifier
# URL
# Event (Pathway or Reaction) Name
# Evidence Code
# Species

# Source file for gene_or_gene_product_reaction:
# https://reactome.org/download/current/Ensembl2ReactomeReactions.txt
# Excerpt:
# 2RSSE.1b.1	R-CEL-8981637	https://reactome.org/PathwayBrowser/#/R-CEL-8981637	RHOA GAPs stimulate RHOA GTPase activity	IEA	Caenorhabditis elegans
# 2RSSE.1b.1	R-CEL-9013144	https://reactome.org/PathwayBrowser/#/R-CEL-9013144	RAC1 GAPs stimulate RAC1 GTPase activity	IEA	Caenorhabditis elegans
# ENSBTAG00000007186.6	R-BTA-9693125	https://reactome.org/PathwayBrowser/#/R-BTA-9693125	RHOF binds effectors at the plasma membrane	IEA	Bos taurus
# ENSBTAG00000007186.6	R-BTA-9693282	https://reactome.org/PathwayBrowser/#/R-BTA-9693282	RHOF GAPs stimulate RHOF GTPase activity	IEA	Bos taurus
# ENSBTAG00000007189	R-BTA-110316	https://reactome.org/PathwayBrowser/#/R-BTA-110316	POLH binds monoUb:K164-PCNA at damaged TT-CPD-DNA template	IEA	Bos taurus

# For all entities (gene, protein, etc) Reactome's mapping files have the same format, except for ChEBI: "CHEBI" prefix is absent. Eg:
# # https://reactome.org/download/current/ChEBI2Reactome_All_Levels.txt:
# 10093	R-CEL-372790	https://reactome.org/PathwayBrowser/#/R-CEL-372790	Signaling by GPCR	IEA	Caenorhabditis elegans
# 10093	R-CEL-373076	https://reactome.org/PathwayBrowser/#/R-CEL-373076	Class A/1 (Rhodopsin-like receptors)	IEA	Caenorhabditis elegans
# 10055	R-DME-372790	https://reactome.org/PathwayBrowser/#/R-DME-372790	Signaling by GPCR	IEA	Drosophila melanogaster
# 10055	R-DME-373076	https://reactome.org/PathwayBrowser/#/R-DME-373076	Class A/1 (Rhodopsin-like receptors)	IEA	Drosophila melanogaster
# 10055	R-HSA-372790	https://reactome.org/PathwayBrowser/#/R-HSA-372790	Signaling by GPCR	TAS	Homo sapiens
# 10055	R-HSA-373076	https://reactome.org/PathwayBrowser/#/R-HSA-373076	Class A/1 (Rhodopsin-like receptors)	TAS	Homo sapiens


# Source file for genes_pathways:
# https://reactome.org/download/current/Ensembl2Reactome_All_Levels.txt
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

class ReactomeEdgesAdapter(Adapter):

    ALLOWED_LABELS = ['genes_pathways', 'gene_or_gene_product_reaction',
                      'small_molecule_to_pathway', 'small_molecule_to_reaction'
                      'parent_pathway_of', 'child_pathway_of']

    def __init__(self, filepath, label, write_properties, add_provenance, taxon_id, ensembl_uniprot_map_path=None):
        """
        Added taxon_id parameter to handle multiple species data.

        If taxon_id is None, all pathways defined in organism_taxon_map
        will be translated into Atom space. Otherwise, only data for
        the specified species will be processed.
        """        
        if label not in ReactomeEdgesAdapter.ALLOWED_LABELS:
            raise ValueError('Invalid label. Allowed values: ' +
                             ', '.join(ReactomeEdgesAdapter.ALLOWED_LABELS))
        self.filepath = filepath
        self.dataset = label
        self.label = label
        self.source = "REACTOME"
        self.source_url = "https://reactome.org"
        self.fbpp_to_uniprot = {}               # dict to map FBpp to UniProt ids and to avoid remote connections during runtime
        self.taxon_id = taxon_id
        if self.taxon_id == 7227:
            self.connection = self.connect_to_flybase()
        # Load the Ensembl to UniProt mapping if provided
        self.ensembl_uniprot_map = {}
        if ensembl_uniprot_map_path:
            try:
                import pickle
                # with open(ensembl_uniprot_map_path, 'rb') as f:
                #     self.ensembl_uniprot_map = pickle.load(f)
                self.ensembl_uniprot_map = pickle.load(open(ensembl_uniprot_map_path, 'rb')) if ensembl_uniprot_map_path else None 
                print(f"Loaded {len(self.ensembl_uniprot_map)} Ensembl-UniProt mappings")
            except Exception as e:
                print(f"Warning: Could not load Ensembl-UniProt mapping: {e}")
                self.ensembl_uniprot_map = {}
        super(ReactomeEdgesAdapter, self).__init__(write_properties, add_provenance)

    def get_edges(self):
        # this is being used only as a list  :/
        organism_taxon_map = {
            'R-DME': 7227,  # Drosophila melanogaster (dmel)
            # 'R-NUL': 7227,  # some reactions of Drosophila melanogaster (dmel)  <----:: CAUTION: some other reactome entities use R-NULL
            'R-HSA': 9606,  # Homo sapiens (hsa)
            # Add more organisms here as needed
            'R-MMU': 10090,   # Mus musculus (mmu)
            'R-RNO': 10116,   # Rattus norvegicus
        }
        with open(self.filepath) as input_file:
            base_props = {}
            if self.write_properties and self.add_provenance:
                base_props['source'] = self.source
                base_props['source_url'] = self.source_url
            not_mapped_no_processing = 0
            total_in_species_records = 0
            for line in input_file:
                data = line.strip().split('\t')

                if not (self.label == 'parent_pathway_of' or self.label == 'child_pathway_of'):  # handles pathways or reactions  edges
                # if self.label == 'genes_pathways' or self.label == 'gene_or_gene_product_reaction': 
                    entity_id, pathway_id = data[0].strip(), data[1].strip()
                    organism_pathway_prefix = pathway_id[:5]  # e.g., 'R-DME', 'R-HSA'
                    an_url = data[2].replace("PathwayBrowser/#", "content/detail")                    
                    pathway_id = f'{pathway_id}'
                    if organism_pathway_prefix in organism_taxon_map or organism_pathway_prefix.startswith('R-NUL'):
                        # taxon = organism_taxon_map[organism_pathway_prefix]
                        props = base_props.copy()
                        if self.label == 'genes_pathways':
                            props['pathway_url'] = an_url
                        else:
                            props['reaction_url'] = an_url                        
                        props['evidence'] = data[4]
                        props['taxon_id'] = f'{self.taxon_id}'

                        if self.label.startswith('small_molecule'):  # It seems that this is a mistake in ChEBI data from Reactome
                            entity_id = f'CHEBI:{entity_id}' 
                        total_in_species_records += 1
                        source_type = self._get_entity_type(entity_id)
                        if source_type is None:
                            print(f'Invalid type for entity {entity_id}.\nRecord: {data}')
                            not_mapped_no_processing += 1
                            continue                        

                        # Drosophila only
                        if self.taxon_id == 7227 and (organism_pathway_prefix == 'R-DME' or organism_pathway_prefix == 'R-NUL'):
                            if entity_id.lower().startswith('fbpp'):
                                uniprot_id = self.ensembl_uniprot_map.get(entity_id)
                                if uniprot_id is None:
                                    # print(f'{entity_id} not found in Ensembl-to-UniProt map.')
                                    
                                    uniprot_id = self.get_uniprot_id_from_FB(self.connection, entity_id)
                                    if uniprot_id is None:
                                        print(f'No UniProt ID for protein {entity_id} of dmel.\nReactome {pathway_id} will not be linked.')
                                        not_mapped_no_processing += 1
                                        continue
                                curie_entity_id = f'UniProtKB:{uniprot_id}'
                            elif entity_id.lower().startswith('fb'):                                
                                curie_entity_id = f'FlyBase:{entity_id}'
                            elif entity_id.startswith('CHEBI'):
                                curie_entity_id = entity_id
                            # print(f'ID for protein: {curie_entity_id}, entity: {entity_id}, type {source_type} of dmel.\tReactome {pathway_id}')
                            source = (source_type, curie_entity_id)
                            # target = pathway_id
                            # Mandatory property for KGXWriter
                            props['id'] = f'{curie_entity_id}_{self.label}_{pathway_id}'
                            yield source, pathway_id, self.label, props
                        # Human only
                        elif self.taxon_id == 9606 and (organism_pathway_prefix == 'R-HSA'  or organism_pathway_prefix == 'R-NUL'):
                            # source_type = self._get_entity_type(entity_id)
                            # Remove version number if present (e.g., ENSG00000000419.14 -> ENSG00000000419)
                            entity_id = entity_id.split('.')[0]
                            
                            if entity_id.startswith(("ENSG", "ENST")):
                                curie_entity_id = f"Ensembl:{entity_id}"
                            elif entity_id.startswith('CHEBI'):
                                curie_entity_id = entity_id
                            else:
                                # Skip protein entries if no UniProt mapping is available
                                if self.ensembl_uniprot_map and entity_id in self.ensembl_uniprot_map:
                                    entity_id = self.ensembl_uniprot_map.get(entity_id)
                                    curie_entity_id = f"{entity_id}"                                    
                                else:
                                    print(f"No UniProt mapping for {entity_id} of H. sapiens.\nRecord: {data}")
                                    not_mapped_no_processing += 1
                                    continue

                            source = (source_type, curie_entity_id)
                            # target = pathway_id
                            # Mandatory property for KGXWriter
                            # props['id'] = f'{curie_entity_id}_{self.label}_{pathway_id}'
                            # print(f"reactome: {props['id']}")
                            yield source, pathway_id, self.label, props
                else:
                    # Handle pathway-pathway relationships
                    parent, child = data[0], data[1]
                    organism_pathway_prefix = parent[:5]
                    if organism_pathway_prefix in organism_taxon_map:
                        parent = f'{parent}'
                        child = f'{child}'
                        # taxon = organism_taxon_map[organism_pathway_prefix]
                        props = base_props.copy()
                        props['taxon_id'] = self.taxon_id
                        if self.label == 'parent_pathway_of':
                            source, target = parent, child
                        else:  # 'child_pathway_of'
                            source, target = child, parent
                        yield source, target, self.label, props
            print(f'Entities not mapped to Uniprot IDs: {not_mapped_no_processing} out of {total_in_species_records}')

    # @todo, I (Saulo) need to change this to make it easier to new species.
    def _get_entity_type(self, entity_id):
        """Return the entity type based on its identifier prefix."""
        if entity_id.startswith(("FBgn", "ENSG")):
            return "gene"
        elif entity_id.startswith(("FBpp", "ENSP")):
            return "protein"
        elif entity_id.startswith(("FBtr", "ENST")):
            return "transcript"
        elif entity_id.startswith('CHEBI'):
            return 'small_molecule'
        else:
            return None

    def connect_to_flybase(self):
        """Establish a connection to the FlyBase PostgreSQL database."""
        try:
            conn = psycopg2.connect(
                host="chado.flybase.org",
                database="flybase",
                user="flybase",
                password="flybase" 
            )
            # print("Connection to FlyBase established successfully!")
            return conn
        except Exception as e:
            print(f"Error connecting to FlyBase: {e}")
            return None

    def get_uniprot_id_from_FB(self, conn, polypeptide_id):
        """
        Retrieve the UniProt ID for a given polypeptide from FlyBase.
        Caches results in fbpp_to_uniprot to avoid repeated queries.
        """
        # Check if we already have this UniProt ID cached
        if polypeptide_id in self.fbpp_to_uniprot:
            return self.fbpp_to_uniprot[polypeptide_id]

        try:
            cursor = conn.cursor()
            # First, get the feature_id (pp_id) for this uniquename (polypeptide_id)
            cursor.execute("""
                SELECT feature_id AS pp_id
                FROM feature
                WHERE is_obsolete = FALSE
                  AND is_analysis = FALSE
                  AND uniquename = %s;
            """, (polypeptide_id,))
            result = cursor.fetchone()

            if result is None:
                print(f"No feature_id found for uniquename: {polypeptide_id}")
                return None
            pp_id = result[0]

            # Next, fetch the UniProt accession(s) for that feature_id
            cursor.execute("""
                SELECT DISTINCT accession
                FROM feature_dbxref fdbx
                JOIN dbxref dbx ON dbx.dbxref_id = fdbx.dbxref_id
                JOIN db ON db.db_id = dbx.db_id
                WHERE fdbx.is_current = TRUE
                  AND db.name IN ('UniProt/Swiss-Prot', 'UniProt/TrEMBL')
                  AND fdbx.feature_id = %s;
            """, (pp_id,))
            uniprot_ids = cursor.fetchall()

            if uniprot_ids:
                # Cache and return the first UniProt ID found
                self.fbpp_to_uniprot[polypeptide_id] = uniprot_ids[0][0]
                return uniprot_ids[0][0]
            else:
                return None

        except Exception as e:
            print(f"Error executing query: {e}")
            return None

        finally:
            cursor.close()
