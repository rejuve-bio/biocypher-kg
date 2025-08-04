import psycopg2
import pickle
from biocypher_metta.adapters import Adapter

# Data file for genes_pathways: https://reactome.org/download/current/Ensembl2Reactome_All_Levels.txt
# Data format:
# - Source database identifier (e.g., UniProt, ENSEMBL, NCBI Gene, or ChEBI identifier)
# - Reactome Pathway Stable Identifier
# - URL
# - Event (Pathway or Reaction) Name
# - Evidence Code
# - Species
#
# Example lines:
# ENSDART00000193986	R-DRE-5653656	https://reactome.org/PathwayBrowser/#/R-DRE-5653656	Vesicle-mediated transport	IEA	Danio rerio
# ENSG00000000419	R-HSA-162699	https://reactome.org/PathwayBrowser/#/R-HSA-162699	Synthesis of dolichyl-phosphate mannose	TAS	Homo sapiens
# ENSG00000000419	R-HSA-163125	https://reactome.org/PathwayBrowser/#/R-HSA-163125	Post-translational modification: synthesis of GPI-anchored proteins	TAS	Homo sapiens
# ENSG00000000419	R-HSA-1643685	https://reactome.org/PathwayBrowser/#/R-HSA-1643685	Disease	TAS	Homo sapiens
# ENSG00000000419.14	R-HSA-162699	https://reactome.org/PathwayBrowser/#/R-HSA-162699	Synthesis of dolichyl-phosphate mannose	TAS	Homo sapiens

# Data file for parent_pathway_of and child_pathway_of: https://reactome.org/download/current/ReactomePathwaysRelation.txt
# Example lines:
# R-BTA-109581	R-BTA-109606
# R-BTA-109581	R-BTA-169911
# R-BTA-109581	R-BTA-5357769
# R-BTA-109581	R-BTA-75153
# R-BTA-109582	R-BTA-140877


class ReactomeAdapter(Adapter):

    ALLOWED_LABELS = ['genes_pathways',
                      'parent_pathway_of', 'child_pathway_of']

    def __init__(
        self,
        filepath,
        label,
        write_properties,
        add_provenance,
        ensembl_to_uniprot_map,
        taxon_id=None
    ):
        """
        If taxon_id is None, all pathways defined in organism_taxon_map
        will be translated into Atom space. Otherwise, only data for
        the specified species will be processed.
        """
        if label not in ReactomeAdapter.ALLOWED_LABELS:
            raise ValueError(
                'Invalid label. Allowed values: '
                + ', '.join(ReactomeAdapter.ALLOWED_LABELS)
            )

        self.filepath = filepath
        self.dataset = label
        self.label = label

        with open(ensembl_to_uniprot_map, "rb") as f:
            self.ensembl2uniprot = pickle.load(f)

        self.fbpp_to_uniprot = {}
        self.taxon_id = taxon_id
        self.source = "REACTOME"
        self.source_url = "https://reactome.org"

        super().__init__(write_properties, add_provenance)

    def get_edges(self):
        organism_taxon_map = {
            'R-DME': 7227,  # Drosophila melanogaster (dmel)
            'R-HSA': 9606,  # Homo sapiens (hsa)
            # Add more organisms here as needed
        }

        if self.taxon_id == 7227:
            connection = self.connect_to_flybase()

        with open(self.filepath) as input_file:
            base_props = {}
            if self.write_properties and self.add_provenance:
                base_props['source'] = self.source
                base_props['source_url'] = self.source_url

            for line in input_file:
                data = line.strip().split('\t')

                if self.label == 'genes_pathways':
                    entity_id, pathway_id = data[0], data[1]
                    prefix = pathway_id[:5]  # e.g., 'R-DME', 'R-HSA'

                    if prefix in organism_taxon_map:
                        taxon = organism_taxon_map[prefix]
                        props = base_props.copy()
                        props['taxon_id'] = taxon

                        # All organisms
                        if self.taxon_id is None:
                            source_type = self._get_entity_type(entity_id)
                            if prefix == 'R-HSA':
                                entity_id = entity_id.split('.')[0]
                            if entity_id.startswith('ENSP'):
                                entity_id = self.ensembl2uniprot[entity_id]
                            source = (source_type, entity_id)
                            target = pathway_id
                            yield source, target, self.label, props

                        # Drosophila only
                        elif self.taxon_id == 7227 and prefix == 'R-DME':
                            source_type = self._get_entity_type(entity_id)
                            if entity_id.startswith('FBpp'):
                                uniprot_id = self.ensembl2uniprot.get(entity_id)
                                if uniprot_id is None:
                                    print(f'{entity_id} not found in Ensembl-to-UniProt map.')
                                    uniprot_id = self.get_uniprot_id(connection, entity_id)
                                    if uniprot_id is None:
                                        print(
                                            f'No UniProt ID for protein {entity_id}. '
                                            f'Reactome {pathway_id} will not be linked.'
                                        )
                                        continue
                                entity_id = uniprot_id
                            source = (source_type, entity_id)
                            target = pathway_id
                            yield source, target, self.label, props

                        # Human only
                        elif self.taxon_id == 9606 and prefix == 'R-HSA':
                            source_type = self._get_entity_type(entity_id)
                            entity_id = entity_id.split('.')[0]
                            source = (source_type, entity_id)
                            target = pathway_id
                            yield source, target, self.label, props

                else:
                    parent, child = data[0], data[1]
                    prefix = parent[:5]
                    if prefix in organism_taxon_map:
                        taxon = organism_taxon_map[prefix]
                        props = base_props.copy()
                        props['taxon_id'] = taxon
                        if self.label == 'parent_pathway_of':
                            source, target = parent, child
                        else:  # 'child_pathway_of'
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
        """Establish a connection to the FlyBase PostgreSQL database."""
        try:
            conn = psycopg2.connect(
                host="chado.flybase.org",
                database="flybase",
                user="flybase",
                password="flybase" 
            )
            print("Connection to FlyBase established successfully!")
            return conn
        except Exception as e:
            print(f"Error connecting to FlyBase: {e}")
            return None

    def get_uniprot_id(self, conn, polypeptide_id):
        """
        Retrieve the UniProt ID for a given polypeptide from FlyBase.
        Caches results in fbpp_to_uniprot to avoid repeated queries.
        """
        # Check if we already have this UniProt ID cached
        if polypeptide_id in self.fbpp_to_uniprot:
            return self.fbpp_to_uniprot[polypeptide_id]

        try:
            cursor = conn.cursor()
            # First, get the feature_id (pp_id) for this uniquename
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
