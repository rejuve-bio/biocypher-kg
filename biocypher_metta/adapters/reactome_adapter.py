
import psycopg2
import pickle
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

    def __init__(self, filepath, label, write_properties, add_provenance, ensembl_to_uniprot_map, taxon_id = None):
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
        with open(ensembl_to_uniprot_map, "rb") as f:
            self.ensembl2uniprot = pickle.load(f)
        self.fbpp_to_uniprot = {}
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
        if self.taxon_id == 7227:
            connection = self.connect_to_flybase()

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
                        if self.taxon_id == None:           # insert data for all organisms
                            source_type = self._get_entity_type(entity_id)                        
                            props = base_props.copy()
                            props['taxon_id'] = taxon_id
                            if prefix == 'R-HSA':
                                entity_id = entity_id.split('.')[0]                            
                            if entity_id.startswith('ENSP'):
                                entity_id = self.ensembl2uniprot[entity_id]
                            source = (source_type, entity_id)
                            target = pathway_id
                            yield source, target, self.label, props

                        elif self.taxon_id == 7227:
                            if prefix == 'R-DME':
                                source_type = self._get_entity_type(entity_id)                        
                                props = base_props.copy()
                                props['taxon_id'] = taxon_id
                                if entity_id.startswith('FBpp'):
                                    uniprot_id = self.ensembl2uniprot.get(entity_id)
                                    if uniprot_id == None:
                                        print(f'{entity_id}  not in ensemble to uniprot dict.')
                                        uniprot_id = self.get_uniprot_id(connection, entity_id)
                                        if uniprot_id == None:
                                            print(f'No Uniprot ID for protein {entity_id}. Reactome {pathway_id} will not be linked to it...')
                                            continue
                                    # print(f'{entity_id}  <---> {uniprot_id}')
                                    entity_id = uniprot_id
                                source = (source_type, entity_id)
                                target = pathway_id
                                # print(f'Inserted: {source} | {target} | {self.label}:\n{props}')
                                yield source, target, self.label, props

                        elif self.taxon_id == 9606:
                            if prefix == 'R-HSA':
                                source_type = self._get_entity_type(entity_id)                        
                                props = base_props.copy()
                                props['taxon_id'] = taxon_id                                
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


    def connect_to_flybase(self):
        """Establishes the connection to the Flybase database."""
        try:
            conn = psycopg2.connect(
                host="chado.flybase.org",
                database="flybase",
                user="flybase",
                password="flybase"  # Replace with your password
            )
            print("Connection established successfully!")
            return conn
        except Exception as e:
            print(f"Error connecting to Flybase: {e}")
            return None


    def get_uniprot_id(self, conn, polypeptide_id):
        """Retrieves the UniProt ID for a polypeptide from Flybase based on the uniquename.
        The result is stored in the fbpp_to_uniprot dictionary to avoid repeated queries."""
        # Verifica se o UniProt ID já foi obtido para este polypeptide_id
        if polypeptide_id in self.fbpp_to_uniprot:
            # print(f"UniProt ID for {polypeptide_id} already cached.")
            return self.fbpp_to_uniprot[polypeptide_id]
        
        try:
            # Primeiro, obtemos o feature_id (pp_id) associado ao uniquename
            cursor = conn.cursor()
            cursor.execute("""
                SELECT feature_id as pp_id 
                FROM feature 
                WHERE is_obsolete = false 
                AND is_analysis = false 
                AND uniquename = %s;
            """, (polypeptide_id,))
            pp_id = cursor.fetchone()
            
            if pp_id is None:
                print(f"No polypeptide ID found for uniquename: {polypeptide_id}")
                return None
            
            pp_id = pp_id[0]

            # Agora, usamos o pp_id para obter o UniProt ID
            cursor.execute("""
                SELECT DISTINCT accession
                FROM feature_dbxref fdbx
                JOIN dbxref dbx ON (dbx.dbxref_id = fdbx.dbxref_id)
                JOIN db ON (db.db_id = dbx.db_id)
                WHERE fdbx.is_current = true
                AND db.name IN ('UniProt/Swiss-Prot', 'UniProt/TrEMBL') 
                AND fdbx.feature_id = %s;
            """, (pp_id,))
            
            uniprot_ids = cursor.fetchall()

            if uniprot_ids:
                # Armazena o UniProt ID no dicionário e retorna o primeiro encontrado
                self.fbpp_to_uniprot[polypeptide_id] = uniprot_ids[0][0]
                return uniprot_ids[0][0]
            else:
                # print(f"No UniProt ID found for feature_id: {pp_id}")
                return None
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cursor.close()
