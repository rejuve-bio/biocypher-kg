
import csv
import gzip
import pickle

'''
    To crete a (pickled) dictionary mapping Entrez ids to Ensembl ids. 
    Useful for TFLinkAdapter class.
'''
def extract_gene_dbxrefs(gz_tsv_filename, pickle_filename):
    gene_dbxrefs = {}

    with gzip.open(gz_tsv_filename, 'rt') as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter='\t')
        for row in reader:
            gene_id = row['GeneID']
            dbxrefs = row['dbXrefs'].split('|')
            flybase_id = next((xref.split(':')[1] for xref in dbxrefs if xref.startswith('FLYBASE')), None)
            if flybase_id:
                gene_dbxrefs[gene_id] = flybase_id

    with open(pickle_filename, 'wb') as pickle_file:
        pickle.dump(gene_dbxrefs, pickle_file)

# Example usage
tsv_filename = 'aux_files/Drosophila_melanogaster.gene_info.gz'
pickle_filename = 'aux_files/dmel_entrez_to_ensembl.pkl'
extract_gene_dbxrefs(tsv_filename, pickle_filename)
