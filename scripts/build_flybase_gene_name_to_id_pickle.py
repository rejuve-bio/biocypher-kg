
import pickle
import sys
import os


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..'))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable


def build_flybase_gene_symbol_to_id_pickle():
    """
    Get a dictionary of flybase gene symbols to their corresponding IDs (FBgn#) from the flybase precomputedtsv file.
    E.g. {'CG12345': 'FBgn0000012345', 'Abd-B': 'FBgn0000015'}
    """
    gene_data_table = FlybasePrecomputedTable("./aux_files/dmel/best_gene_summary_fb_2025_03.tsv.gz")
    #header:
    #FBgn_ID	Gene_Symbol	Summary_Source	Summary
    rows = gene_data_table.get_rows()
    gene_symbol_to_id_dict = {}
    for row in rows:
        gene_name = row[1].strip().upper()  # human dict uses HGNC that uses uppercase
        gene_id = row[0]
        gene_symbol_to_id_dict[gene_name] = gene_id

    # annotation ID in Flybase are preceds gene names. Some other DBs keep using annotation IDs as gene names. 
    # So, wee try to get FBgn from annotatin ID as well...
    gene_data_table = FlybasePrecomputedTable("./aux_files/dmel/fbgn_annotation_ID_fb_2025_03.tsv.gz")
    #header:
    #FBgn_ID	Gene_Symbol	Summary_Source	Summary
    rows = gene_data_table.get_rows()
    for row in rows:
        gene_annot_ids = [row[4].strip().upper()]  # human dict uses HGNC that uses uppercase
        gene_annot_ids.extend(row[5].strip().split(","))
        for gene_annot_id in gene_annot_ids:
            gene_id = row[2]
            gene_symbol_to_id_dict[gene_annot_id.upper()] = gene_id


    pickle.dump(gene_symbol_to_id_dict, open("./aux_files/dmel/gene_name_to_id_.pkl", "wb"))


def main():
    build_flybase_gene_symbol_to_id_pickle()
if __name__ == "__main__":
    main()
    