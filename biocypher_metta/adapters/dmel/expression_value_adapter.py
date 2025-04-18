'''

expression value:
  is_a: entity
  represented_as: edge
  inherit_properties: false
  input_label: expression_value
  source: [gene, transcript, exon]
  target: RNASeq library
  description: >-
    A set of values for numeric estimation of single gene/transcript expression as measured by a given methodology (represented by the library).
  properties:
    value_and_description: list[tuple]]
    taxon_id: int                        # 7227 for dmel / 9606 for hsa
    source: str
    source_url: str


FB  data:
https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#High-Throughput_Gene_Expression_.28high-throughput_gene_expression_fb_.2A.tsv.gz.29
https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#Single_Cell_RNA-Seq_Gene_Expression_.28scRNA-Seq_gene_expression_fb_.2A.tsv.gz.29
https://wiki.flybase.org/wiki/FlyBase:Downloads_Overview#RNA-Seq_RPKM_values_.28gene_rpkm_report_fb_.2A.tsv.gz.29

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

class ExpressionValueAdapter(Adapter):
    def __init__(self, write_properties, add_provenance, filepaths):
        self.filepaths = filepaths
        self.label = 'expression_value'
        self.source = 'FLYBASE'
        self.source_url = 'https://flybase.org/'

        super(ExpressionValueAdapter, self).__init__(write_properties, add_provenance)


    def get_edges(self):
        for dmel_data_filepath in self.filepaths:
            expression_table = FlybasePrecomputedTable(dmel_data_filepath)
            self.version = expression_table.extract_date_string(dmel_data_filepath)
            if "scRNA-Seq_gene_expression_fb" in dmel_data_filepath:                
                self.source = 'FLYBASE'
                self.source_url = 'https://flybase.org/'
                # header:
                # Pub_ID	Pub_miniref	Clustering_Analysis_ID	Clustering_Analysis_Name	Source_Tissue_Sex	Source_Tissue_Stage
                # Source_Tissue_Anatomy	Cluster_ID	Cluster_Name	Cluster_Cell_Type_ID	Cluster_Cell_Type_Name	Gene_ID	Gene_Symbol	Mean_Expression	Spread
                rows = expression_table.get_rows()
                for row in rows:
                    props = {}
                    #source_ids = row[1].split('|')
                    _source = ('gene', row[11])    # gene FBgn#
                    _target = row[7]     # library FBlc = Cluster ID#

                    props['value_and_description'] = [
                        ( row[13],        # Mean_Expression
                          "Mean_Expression"#: is the average level of expression of the gene across all "
                                           #       "cells of the cluster in which the gene is detected at all"
                        ),
                        ( row[14],    # Spread
                          "Spread"#: is the proportion of cells in the cluster in which the gene is detected"
                        )
                    ]
                    props['taxon_id'] = 7227
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                    yield _source, _target, self.label, props
                
                del rows

                
            elif "high-throughput_gene_expression_fb" in dmel_data_filepath:
                self.source = 'FLYBASE'
                self.source_url = 'https://flybase.org/'
                # header:
                # High_Throughput_Expression_Section	Dataset_ID	Dataset_Name	Sample_ID	Sample_Name	Gene_ID
                # Gene_Symbol	Expression_Unit	Expression_Value
                rows = expression_table.get_rows()
                for row in rows:
                    props = {}
                    # source_ids = row[1].split('|')
                    _source = ('gene', row[5])  # Gene_ID   FBgn#
                    _target = row[3]    # Sample_ID  FBlc#

                    props['value_and_description'] = [
                        (row[8],        # Expression_Value
                         str(row[7]).replace('"', '').replace("'", '')         # Expression_Unit
                         ),
                    ]
                    props['taxon_id'] = 7227
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                    yield _source, _target, self.label, props
            elif "gene_rpkm_report_fb" in dmel_data_filepath:
                self.source = 'FLYBASE'
                self.source_url = 'https://flybase.org/'
                # header:
                # Release_ID	FBgn#	GeneSymbol	Parent_library_FBlc#	Parent_library_name	RNASource_FBlc#
                # RNASource_name	RPKM_value	Bin_value	Unique_exon_base_count	Total_exon_base_count	Count_used
                rows = expression_table.get_rows()
                for row in rows:
                    props = {}
                    _source = ('gene', row[1]) # FBgn#
                    _target = row[5]  # RNASource_FBlc#

                    props['value_and_description'] = [
                        (row[7], # RPKM_value
                         "RPKM",
                         row[11]        #	Count_used in the RPKM (Total or Unique)
                         ),
                        (row[8],  # Bin_value
                         "Bin_value"#: The expression bin classification of this gene in this RNA-Seq experiment, "
                         #"based on RPKM value. Bins range from 1 (no/extremely low expression) to 8 (extremely high expression)"
                         ),
                        (row[9],  # Unique_exon_base_count
                         "Unique_exon_base_count"#: The number of exonic bases unique to the gene (not overlapping exons of other genes). Field "
                         #"will be blank for genes derived from dicistronic/polycistronic transcripts"
                         ),
                        (row[10],  # Total_exon_base_count
                         "Total_exon_base_count"#: The number of bases in all exons of this gene"
                         ),
                    ]
                    props['taxon_id'] = 7227
                    if self.add_provenance:
                        props['source'] = self.source
                        props['source_url'] = self.source_url
                    yield _source, _target, self.label, props

            # FCA2 gene expression:
            # The fca2 file contents were generated in the "scripts/get_flyatlas2_gene_data.py" script by method
            # convert_gene_files()            
            elif "fca2" in dmel_data_filepath:
                self.source = 'FlyCellAtlas2'
                self.source_url = 'https://flyatlas.gla.ac.uk/FlyAtlas2/index.html?page=help'
                fca2_tissues_file_path = 'aux_files/fca2_to_fb_tissues.tsv'                    
                if "transcript" in dmel_data_filepath:
                    _, tissue_library_dict = self.build_fca2_fb_tissues_libraries_ids_dicts(fca2_tissues_file_path)
                else:
                    tissue_library_dict, _ = self.build_fca2_fb_tissues_libraries_ids_dicts(fca2_tissues_file_path)                

                self.version = expression_table.extract_date_string(dmel_data_filepath)                
                rows = expression_table.get_rows()
                
                # fca2_fbgn_gene header:
                # FBgene ID	      Tissue stage and sex	    Tissue	    FPKM	SD	Enrichment                
                if 'fbgn_gene' in dmel_data_filepath:
                    for row in rows:
                        try:
                            # Tissue stage and sex']}_{row['_gene file tissue']}"
                            library_data = tissue_library_dict[ f'{row[1]}_{row[2]}' ]
                        except KeyError:
                            print(f"No library data for key: {f'{row[1]}_{row[2]}'}. Gene: {row[0]}")
                            if f'{row[1]}_{row[2]}' == "Larval_Garland cells":
                                library_data = ("Garland Organ", "FBlc0006089", "RNA-Seq_Profile_FlyAtlas2_L3_Garland_Organ") 
                        # target
                        library_id = library_data[1]
                        _source = ('gene',row[0])
                        props['value_and_description'] = [
                            (
                                row[3],         # Expression_Value,
                                'FPKM',        # Expression_Unit
                            ),
                            (
                                row[4],         # Expression_Value
                                'SD',          # Expression_Unit
                            ),
                            (
                                row[5],         # Expression_Value
                                'Enrichment',  # Expression_Unit
                            ),
                        ]                        
                        props['taxon_id'] = 7227
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                        
                        yield _source, library_id, self.label, props

                # fca2_fbgn_Mir_gene header:
                # FBgene ID	      Tissue stage and sex	    Tissue	    TPM 	SD	Enrichment                
                elif 'mir_gene' in dmel_data_filepath:
                    for row in rows:
                        try:
                            # Tissue stage and sex']}_{row['_gene file tissue']}"
                            library_data = tissue_library_dict[ f'microRNA_{row[1]}_{row[2]}' ]
                        except KeyError:
                            print(f"No library data for key: {f'{row[1]}_{row[2]}'}. MicroRNA gene: {row[0]}")
                            if f'{row[1]}_{row[2]}' == "Adult Male_Whole body":
                                library_data = ("Whole", "FBlc0005729", "microRNA-Seq_TPM_FlyAtlas2_Adult_Male")
                            elif f'{row[1]}_{row[2]}' == "Adult Female_Whole body":
                                library_data = ("Whole", "FBlc0005730", "microRNA-Seq_TPM_FlyAtlas2_Adult_Female")                            
                        _source = ('gene',row[0])
                        # target
                        library_id = library_data[1]
                        props['value_and_description'] = [
                            (
                                row[3],         # Expression_Value
                                'TPM',         # Expression_Unit
                            ),
                            (
                                row[4],         # Expression_Value
                                'SD',          # Expression_Unit
                            ),
                            (
                                row[5],         # Expression_Value
                                'Enrichment',  # Expression_Unit
                            ),
                        ]
                        props['taxon_id'] = 7227
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                        
                        yield _source, library_id, self.label, props

                # fca2_fbgn_transcriptGene header:
                # FBgene ID     Tissue stage and sex	Tissue    FBtranscript ID	    FPKM	SD
                elif "transcriptGene" in dmel_data_filepath:
                    for row in rows:
                        try:
                            # Tissue stage and sex']}_{row['_gene file tissue']}"
                            library_data = tissue_library_dict[ f'{row[1]}_{row[2]}' ]
                        except KeyError:
                            print(f"No library data for key: {f'{row[1]}_{row[2]}'}. transcriptGene: {row[0]}")
                            if f'{row[1]}_{row[2]}' == "Larval_Garland cells":
                                library_data = ("Garland Organ", "FBlc0006089", "RNA-Seq_Profile_FlyAtlas2_L3_Garland_Organ")                            
                        
                        library_data = tissue_library_dict[ f'{row[1]}_{row[2]}' ]
                        # target
                        library_id = library_data[1]                        
                        _source = ('transcript', row[3].split('/')[-1])
                        props['value_and_description'] = [
                            (
                                row[4],         # Expression_Value
                                'FPKM',        # Expression_Unit
                            ),
                            (
                                row[5],         # Expression_Value
                                'SD',          # Expression_Unit
                            ),
                        ]
                        props['taxon_id'] = 7227
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                        
                        yield _source, library_id, self.label, props

                # fca2_fbgn_Mir_transcript header:
                # FBgene ID     Tissue stage and sex	Tissue    FBtranscript ID	    TPM 	SD              
                elif 'mir_transcript' in dmel_data_filepath:
                    for row in rows:
                        try:
                            # Tissue stage and sex']}_{row['_gene file tissue']}"
                            library_data = tissue_library_dict[ f'microRNA_{row[1]}_{row[2]}' ]
                        except KeyError:
                            if f'{row[1]}_{row[2]}' == "Adult Male_Whole body":
                                library_data = ("Whole", "FBlc0005729", "microRNA-Seq_TPM_FlyAtlas2_Adult_Male")
                            elif f'{row[1]}_{row[2]}' == "Adult Female_Whole body":
                                library_data = ("Whole", "FBlc0005730", "microRNA-Seq_TPM_FlyAtlas2_Adult_Female")

                        # target
                        library_id = library_data[1]
                        _source = ('transcript', row[3].split('/')[-1])
                        props['value_and_description'] = [
                            (
                                row[4],         # Expression_Value
                                'TPM',        # Expression_Unit
                            ),
                            (
                                row[5],         # Expression_Value
                                'SD',          # Expression_Unit
                            ),
                        ]   
                        props['taxon_id'] = 7227
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                        
                        yield _source, library_id, self.label, props

            #afca_afca_annotation_group_by_mean header FORMAT:
            #FB gene symbol	cell_type1_5	cell_type1_30	cell_type1_50	cell_type1_70	cell_type2_5	cell_type2_30...
            elif 'afca' in dmel_data_filepath:
                self.source = 'AgingFlyCellAtlas'
                self.source_url = 'https://hongjielilab.org/afca/'
                gene_symbol_to_fbgn = self.build_gene_symbol_to_fbgn_dict()
                libraries = expression_table.get_header()
                rows = expression_table.get_rows()
                for row in rows:
                    fbgn = gene_symbol_to_fbgn.get(row[0])
                    if fbgn is None:            # genes that have not been localized to the reference genome assembly for a given Drosophila species.
                        fbgn = self.get_fbgn_from_flybase(row[0])
                        if fbgn is None:
                            print(f'Gene {row[0]} is not in Flybase or is not a fresh Flybase record: EXCLUDED FROM ATOM SPACE...')
                            continue                        
                    _source = ('gene', fbgn)                    
                    for exp_value, library_id in zip(row[1:], libraries[1:]):
                        props['value_and_description'] = [
                            (
                                exp_value,         # Expression_Value
                                '?',         # Expression_Unit  TODO: thi is not a correct description (look at author's email...)
                            ),                            
                        ]
                        props['taxon_id'] = 7227
                        if self.add_provenance:
                            props['source'] = self.source
                            props['source_url'] = self.source_url
                        
                        yield _source, library_id, self.label, props
                        


    def build_gene_symbol_to_fbgn_dict(self):
        '''
            From Flybase downloads overview web page one can learn that fbgn_fbtr_fbpp_expanded_fb_ files exclude:                
                genes that have not been localized to the reference genome assembly for a given (Drosophila) species.

        '''
        symbols_to_fbgn_file = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/flybase/fbgn_fbtr_fbpp_expanded_fb_2024_06.tsv.gz"
        #symbols_to_fbgn_file = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase/fbgn_fbtr_fbpp_expanded_fb_2024_06.tsv.gz"

        symbols_to_fbgn_table = FlybasePrecomputedTable(symbols_to_fbgn_file)
        symbols_to_fbgn_dict = {}
        for row in symbols_to_fbgn_table.get_rows():
            if row[3] not in symbols_to_fbgn_dict:
                symbols_to_fbgn_dict[row[3]] = row[2]
        return symbols_to_fbgn_dict
    

    def get_fbgn_from_flybase(self, gene_symbol: str):
        # Establish a connection to the FlyBase database
        conn = psycopg2.connect(
            host="chado.flybase.org",
            database="flybase",
            user="flybase"
        )    
        cur = conn.cursor()
        cur.execute(f"SELECT uniquename, name, is_obsolete FROM feature WHERE feature.name LIKE '{gene_symbol}';")
        results = cur.fetchall() 
        for gene_data in results:
            if gene_data[-1] == False:    # is uniquename obsolete???
                return gene_data[0]
        '''
            if nothing is returned try an alternative query: Flybase appends :1, :2,... to symbols and FBgns
        '''
        cur.execute(f"SELECT uniquename, name, is_obsolete FROM feature WHERE feature.name LIKE '{gene_symbol}:1';")
        results = cur.fetchall() 
        #print(f'results for {gene_symbol}:\n{results}')
        for gene_data in results:
            if gene_data[-1] == False:    # uniquename is not obsolete
                return gene_data[0].split(':')[0]
            
        '''
            if nothing is returned try to get gene_symbol as a synonym...
        '''
        cur.execute(f"SELECT synonym.synonym_id FROM synonym WHERE synonym.name LIKE '{gene_symbol}';")
        results = cur.fetchall() 
        results = list(set(results))
        for res in results:
            cur.execute(f"SELECT feature_id FROM feature_synonym WHERE synonym_id={res[0]};")
            results = cur.fetchall() 
            results = list(set(results))
            for result in results:
                cur.execute(f"SELECT uniquename, is_obsolete FROM feature WHERE feature_id={result[0]};")
                feature_results = cur.fetchall() 
                for feature in feature_results:
                    if feature[-1] == False:
                        return feature[0]
        cur.close()
        conn.close()
        
        return None


    def build_fca2_fb_tissues_libraries_ids_dicts(self, file_path):
        gene_tissue_library_dict = {}
        transcript_tissue_library_dict = {} 
            
        # Establish a connection to the FlyBase database
        conn = psycopg2.connect(
            host="chado.flybase.org",
            database="flybase",
            user="flybase"
        )    
        cur = conn.cursor()
        cur.execute("SELECT uniquename, name FROM library WHERE library.name LIKE 'RNA-Seq_Profile_FlyAtlas2_%';") #RNA-Seq_Profile_FlyAtlas2_
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
        # Flybase naming inconsistency does not append "Whole" to these libraries' names:
        gene_tissue_library_dict["microRNA_Adult Male_Whole body"] = ("Whole", "FBlc0005729", "microRNA-Seq_TPM_FlyAtlas2_Adult_Male")
        gene_tissue_library_dict["microRNA_Adult Female_Whole body"] = ("Whole", "FBlc0005730", "microRNA-Seq_TPM_FlyAtlas2_Adult_Female")  
        transcript_tissue_library_dict["microRNA_Adult Male_Whole body"] = ("Whole", "FBlc0005729", "microRNA-Seq_TPM_FlyAtlas2_Adult_Male")
        transcript_tissue_library_dict["microRNA_Adult Female_Whole body"] = ("Whole", "FBlc0005730", "microRNA-Seq_TPM_FlyAtlas2_Adult_Female")  
        cur.close()
        mir_cur.close()
        conn.close()

        return gene_tissue_library_dict, transcript_tissue_library_dict
