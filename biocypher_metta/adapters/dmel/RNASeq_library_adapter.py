'''

# The unique ID for a library at Flybase is its FBlc#
RNASeq library:
  is_a: entity
  represented_as: node
  inherit_properties: false
  input_label: rnaseq_library
  description: >-
      A library/cluster/sample that is a set of genes and their expression values.
  properties:
    name: str
    experiment_info: str[]
    tissue_info: str[]
    cell_type_id: str
    taxon_id: int                        # 7227 for dmel / 9606 for hsa
    source: str

    source_url: str

FB  data:
https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#High-Throughput_Gene_Expression_.28high-throughput_gene_expression_fb_.2A.tsv.gz.29
https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Single_Cell_RNA-Seq_Gene_Expression_.28scRNA-Seq_gene_expression_fb_.2A.tsv.gz.29
https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#RNA-Seq_RPKM_values_.28gene_rpkm_report_fb_.2A.tsv.gz.29

Flybase uses "Garland Organ" and Fly Cell Atlas 2 uses "Garland Cells" in the data.         |
Flybase uses "Brain" and Fly Cell Atlas 2 uses "Brain / CNS" in the data.                   | Flybase is the authority...

#########################################################################################################################
Mike comments (MM direct message August 21th) about the above tables (I've NOT verified in this first round):
the first & third overlap, the samples in the third that aren't in the first sound like they have different measurement
units that we would have to convert to make them comparable if possible

the second from EBI Single Cell Expression Atlas we would want to make comparable to fca & afca
#########################################################################################################################
FB scRNASeq table:
#Pub_ID	Pub_miniref	Clustering_Analysis_ID	Clustering_Analysis_Name	Source_Tissue_Sex	Source_Tissue_Stage	Source_Tissue_Anatomy	Cluster_ID	Cluster_Name	Cluster_Cell_Type_ID	Cluster_Cell_Type_Name	Gene_ID	Gene_Symbol	Mean_Expression	Spread
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0031081	Nep3	1022.4949	0.00016897600540723216
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0031088	CG15322	269.05170000000004	0.0005069280162216965
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0053217	CG33217	439.74384371428573	0.026022304832713755
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0052350	Vps11	585.499525895105	0.024163568773234202
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0024733	RpL10	3497.867660248448	0.9793849273403177
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0040372	G9a	602.1811133469388	0.05795876985468063
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0000316	cin	582.4078043088889	0.03801960121662724
FBrf0245988	Cattenoz et al., 2020, EMBO J. 39(12): e104486	FBlc0003731	scRNAseq_2020_Cattenoz_NI_seq_clustering		larval stage	embryonic/larval hemolymph	FBlc0003732	scRNAseq_2020_Cattenoz_NI_seq_clustering_plasmatocytes	FBbt:00001685	embryonic/larval plasmatocyte	FBgn0024989	CG3777	354.646665	0.0003379520108144643


FB high-throughput table
#High_Throughput_Expression_Section	Dataset_ID	Dataset_Name	Sample_ID	Sample_Name	Gene_ID	Gene_Symbol	Expression_Unit	Expression_Value
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003619	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Brain	FBgn0000003	7SLRNA:CR32864	FPKM	202396
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003619	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Brain	FBgn0289744	EndoG	FPKM	12
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003619	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Brain	FBgn0289745	Est17	FPKM	0
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003619	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Brain	FBgn0289868	Or67c	FPKM	0
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003620	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Crop	FBgn0000003	7SLRNA:CR32864	FPKM	240220
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003620	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Crop	FBgn0000008	a	FPKM	2
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003620	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Crop	FBgn0000014	abd-A	FPKM	0
FlyAtlas2 Anatomy RNA-Seq	FBlc0003498	FlyAtlas2	FBlc0003620	RNA-Seq_Profile_FlyAtlas2_Adult_Female_Crop	FBgn0000015	Abd-B	FPKM	0

FB RPKM report table:
# Release_ID	FBgn#	GeneSymbol	Parent_library_FBlc#	Parent_library_name	RNASource_FBlc#	RNASource_name	RPKM_value	Bin_value	Unique_exon_base_count	Total_exon_base_count	Count_used
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000060	BCM_1_RNAseq	FBlc0000068	BCM_1_FA3d	85	5	299	299	Unique
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000060	BCM_1_RNAseq	FBlc0000069	BCM_1_MA3d	119	6	299	299	Unique
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000060	BCM_1_RNAseq	FBlc0000070	BCM_1_P	53	5	299	299	Unique
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000060	BCM_1_RNAseq	FBlc0000071	BCM_1_L	55	5	299	299	Unique
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000060	BCM_1_RNAseq	FBlc0000072	BCM_1_A17d	37	4	299	299	Unique
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000085	modENCODE_mRNA-Seq_development	FBlc0000086	mE_mRNA_em0-2hr	2	1	299	299	Unique
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000085	modENCODE_mRNA-Seq_development	FBlc0000087	mE_mRNA_em2-4hr	1	1	299	299	Unique
Dmel_R6.58	FBgn0000003	7SLRNA:CR32864	FBlc0000085	modENCODE_mRNA-Seq_development	FBlc0000088	mE_mRNA_em4-6hr	1	1	299	299	Unique

'''

import psycopg2
import csv
from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable
from biocypher_metta.adapters import Adapter
from biocypher._logger import logger
import gc

class RnaseqLibraryAdapter(Adapter):

    def __init__(self, write_properties, add_provenance, filepaths: list[str]):
        self.filepaths = filepaths
        self.label = 'rnaseq_library'
        self.source = 'FLYBASE'                     # this will be set for each data file in the get_nodes method
        self.source_url = 'https://flybase.org/'    # this will be set for each data file in the get_nodes method

        super(RnaseqLibraryAdapter, self).__init__(write_properties, add_provenance)


    def get_nodes(self):
        for dmel_data_filepath in self.filepaths:
            expression_table = FlybasePrecomputedTable(dmel_data_filepath)
            if "scRNA-Seq_gene_expression_fb" in dmel_data_filepath:       
                self.version = expression_table.extract_date_string(dmel_data_filepath)
                self.source = 'FLYBASE'
                self.source_url = 'https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Single_Cell_RNA-Seq_Gene_Expression_.28scRNA-Seq_gene_expression_fb_.2A.tsv.gz.29'
                # header:
                # Pub_ID	Pub_miniref	Clustering_Analysis_ID	Clustering_Analysis_Name	Source_Tissue_Sex	Source_Tissue_Stage
                # Source_Tissue_Anatomy	Cluster_ID	Cluster_Name	Cluster_Cell_Type_ID	Cluster_Cell_Type_Name	Gene_ID	Gene_Symbol	Mean_Expression	Spread
                rows = expression_table.get_rows()
                for row in rows:
                    library_id = row[7]         # Cluster_ID
                    props = {
                        "name": row[8],         # Cluster_Name
                        "experiment_info": [row[2], row[3]],        # Clustering_Analysis_ID, Clustering_Analysis_Name
                        "tissue_info": [row[i] for i in [4,5,6] if row[i] != ''],    # Source_Tissue_Sex	Source_Tissue_Stage, Source_Tissue_Anatomy
                        "cell_type_id": row[9],     # Cluster_Cell_Type_ID (Flybase ontology FBbt:#)
                        "taxon_id": 7227,
                        "source": self.source,
                        "source_url": self.source_url
                    }
                    yield library_id, self.label, props
                
                del rows
                gc.collect()

            elif "high-throughput_gene_expression_fb" in dmel_data_filepath:
                self.version = expression_table.extract_date_string(dmel_data_filepath)
                self.source = 'FLYBASE'
                self.source_url = 'https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#High-Throughput_Gene_Expression_.28high-throughput_gene_expression_fb_.2A.tsv.gz.29'
                # FB high-throughput table header
                # High_Throughput_Expression_Section	Dataset_ID	Dataset_Name	Sample_ID	Sample_Name	Gene_ID	Gene_Symbol	Expression_Unit	Expression_Value
                rows = expression_table.get_rows()
                for row in rows:
                    library_id = row[3]     # Sample_ID
                    props = {
                        "name": row[4],         # Sample_Name
                        "experiment_info": [row[1], row[2]],        # Dataset_ID	Dataset_Name
                        "taxon_id": 7227,
                        "source": self.source,
                        "source_url": self.source_url
                    }
                    yield library_id, self.label, props
            elif "gene_rpkm_report_fb" in dmel_data_filepath:
                self.version = expression_table.extract_date_string(dmel_data_filepath)
                self.source = 'FLYBASE'
                self.source_url = 'https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#RNA-Seq_RPKM_values_.28gene_rpkm_report_fb_.2A.tsv.gz.29'
                # FB RPKM report table header
                # Release_ID	FBgn#	GeneSymbol	Parent_library_FBlc#	Parent_library_name	RNASource_FBlc#	RNASource_name	RPKM_value	Bin_value	Unique_exon_base_count	Total_exon_base_count	Count_used
                rows = expression_table.get_rows()
                for row in rows:
                    library_id = row[5]     # RNASource_FBlc#
                    props = {
                        "name": row[6],         # RNASource_name
                        "experiment_info": [row[0], row[3], row[4]],        # Release_ID	Parent_library_FBlc Parent_library_name
                        "taxon_id": 7227,
                        "source": self.source,
                        "source_url": self.source_url
                    }
                    yield library_id, self.label, props
            
            # FCA2 gene expression:
            # The fca2 file contents were generated in the "scripts/get_flyatlas2_gene_data.py" script by method
            # convert_gene_files()
            elif "fca2" in dmel_data_filepath:
                fca2_tissues_file_path = 'aux_files/fca2_to_fb_tissues.tsv'                    
                if "transcript" in dmel_data_filepath:      # transcript data files use "Brain" but the others use "Brain / CNS"
                    _, tissue_library_dict = self.build_fca2_fb_tissues_libraries_ids_dicts(fca2_tissues_file_path)
                else:
                    tissue_library_dict, _ = self.build_fca2_fb_tissues_libraries_ids_dicts(fca2_tissues_file_path)
                self.version = expression_table.extract_date_string(dmel_data_filepath)
                self.source = 'FlyCellAtlas2'
                self.source_url = 'https://flyatlas.gla.ac.uk/FlyAtlas2/index.html?page=help'

                # Flybase naming inconsistency does not append "Whole" to these libraries' names:
                tissue_library_dict["microRNA_Adult Male_Whole body"] = ("Whole", "FBlc0005729", "microRNA-Seq_TPM_FlyAtlas2_Adult_Male")
                tissue_library_dict["microRNA_Adult Female_Whole body"] = ("Whole", "FBlc0005730", "microRNA-Seq_TPM_FlyAtlas2_Adult_Female")  
                
                # library_data: (FB Tissue, FB library ID (FBlc#), FB library name)
                for tissue_key, library_data in tissue_library_dict.items():
                    library_id = library_data[1]
                    props = {
                        "name": library_data[2],
                        "experiment_info": [library_data[0], tissue_key.split('_')[0]], #, f'FCA2 tissue {row[2]}'],        # FB Tissue, Tissue Stage, Tissue Sex
                        "taxon_id": 7227,
                        "source": self.source,
                        "source_url": self.source_url
                    }
                    yield library_id, self.label, props

            elif "afca" in dmel_data_filepath:
                self.version = expression_table.extract_date_string(dmel_data_filepath)
                self.source = 'AgingFlyCellAtlas'
                self.source_url = 'https://hongjielilab.shinyapps.io/AFCA/'
                #self.source_url = 'https://bcmedu-my.sharepoint.com/:u:/g/personal/u239500_bcm_edu/EXgUmzr6BFJJhAHJ106pd1sBU8ksAMg4VHxkyqncy3J7Qw?e=mY8Jce'
                libs_names = expression_table.get_header()
                id = -1
                for lib_name in libs_names[1:]:
                    id += 1
                    library_id = f"AFCA_{self.label}_{lib_name.replace(' ', '_')}"
                    props = {
                        "name": lib_name,
                        "experiment_info": ["Expression is the averaged log values in each cluster (cell type). Appended to the cell type is the time point (5, 30, 50 or 70 days)."],
                        "taxon_id": 7227,
                        "source": self.source,
                        "source_url": self.source_url
                    }
                    yield library_id, self.label, props



    def build_fca2_fb_tissues_libraries_ids_dicts(self,file_path):
        gene_tissue_library_dict = {}
        transcript_tissue_library_dict = {} 
            
        # Establish a connection to the FlyBase database
        conn = psycopg2.connect(
            host = "chado.flybase.org",
            database = "flybase",
            user = "flybase"
        )    
        cur = conn.cursor()
        cur.execute("SELECT uniquename, name FROM library WHERE library.name LIKE 'RNA-Seq_Profile_FlyAtlas2_%';")
        results = cur.fetchall() 
        mir_cur = conn.cursor()
        mir_cur.execute("SELECT uniquename, name FROM library WHERE library.name LIKE 'microRNA-Seq_TPM_FlyAtlas2_%';")
        #mir_cur.execute("SELECT uniquename, name FROM library WHERE library.name LIKE 'microRNA-Seq%';")
        mir_results = mir_cur.fetchall()
        # print(mir_results)
        # print(len(mir_results))
        
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t')
            # row:
            # Tissue stage and sex	    _gene file tissue	    _transcriptGene file tissue	    FB_tissue
            for row in reader:
                for result in results:
                    # print(f'reg: {result[-1]}')
                    # print(f"tiss: {row['FB_tissue'].replace(' ', '_')}")
                    if row['Tissue stage and sex'] == 'Larval':
                        tss = "L3"
                    else:
                        tss = row['Tissue stage and sex']
                    # print(f"prob_: {tss}_{row['_gene file tissue']}")
                    # print(f"ends: {tss.replace(' ', '_')}_{row['FB_tissue'].replace(' ', '_')}")
                    if result[-1].endswith(f"{tss.replace(' ', '_')}_{row['FB_tissue'].replace(' ', '_')}"):
                        library_name = result[-1] if result else None
                        library_uniquename = result[0] if result else None
                        gene_key = f"{row['Tissue stage and sex']}_{row['_gene file tissue']}"                        
                        gene_value = (row['FB_tissue'], library_uniquename, library_name)
                        #print(f"gk: {gene_key}|||{gene_value}")
                        # Add the value to the tissue_library_dict
                        if gene_key not in gene_tissue_library_dict:
                            gene_tissue_library_dict[gene_key] = gene_value

                        transcript_key = f"{row['Tissue stage and sex']}_{row['_transcriptGene file tissue']}"
                        transcript_value = (row['FB_tissue'], library_uniquename, library_name)

                        # Add the value to the transcript_dict
                        if transcript_key not in transcript_tissue_library_dict:
                            transcript_tissue_library_dict[transcript_key] = transcript_value
                        break
                for result in mir_results:
                    # print(f'mir: {result[-1]}')
                    # print(f"tiss: {row['FB_tissue'].replace(' ', '_')}")
                    if row['Tissue stage and sex'] == 'Larval':
                        tss = "L3"
                    else:
                        tss = row['Tissue stage and sex']
                    # print(f"prob_: {tss}_{row['_gene file tissue']}")
                    # print(f"ends: {tss.replace(' ', '_')}_{row['FB_tissue'].replace(' ', '_')}")
                    if result[-1].endswith(f"{tss.replace(' ', '_')}_{row['FB_tissue'].replace(' ', '_')}"):
                        library_name = result[-1] if result else None
                        library_uniquename = result[0] if result else None
                        gene_key = f"microRNA_{row['Tissue stage and sex']}_{row['_gene file tissue']}"
                        # print(f"gk: {gene_key}")#microRNA_Larval_Garland cells
                        gene_value = (row['FB_tissue'], library_uniquename, library_name)
                        #print(f"gk: {gene_key}///{gene_value}")

                        # Add the value to the tissue_library_dict
                        if gene_key not in gene_tissue_library_dict:
                            gene_tissue_library_dict[gene_key] = gene_value

                        transcript_key = f"microRNA_{row['Tissue stage and sex']}_{row['_transcriptGene file tissue']}"
                        transcript_value = (row['FB_tissue'], library_uniquename, library_name)

                        # Add the value to the transcript_dict
                        if transcript_key not in transcript_tissue_library_dict:
                            transcript_tissue_library_dict[transcript_key] = transcript_value
                        break
                
        cur.close()
        mir_cur.close()
        conn.close()

        return gene_tissue_library_dict, transcript_tissue_library_dict
