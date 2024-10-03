import gzip
import sys
from Bio import SeqIO
import pickle

def create_ensembl_to_uniprot_dict(input_uniprot, ensembl_to_uniprot_output):
    ensembl_uniprot_ids = {}
    with gzip.open(input_uniprot, 'rt') as input_file:
        records = SeqIO.parse(input_file, 'swiss')
        for record in records:
            dbxrefs = record.dbxrefs
            for item in dbxrefs:
                if item.startswith('STRING'):            
                    try:
                        ensembl_id = item.split(':')[-1].split('.')[1]
                        #uniprot_id = record.name.replace("_DROME", "")
                        uniprot_id = record.id
                        if ensembl_id:
                            ensembl_uniprot_ids[ensembl_id] = uniprot_id
                    except:
                        print(f'fail to process record: {record.name}')
    with open(ensembl_to_uniprot_output, 'wb') as pickle_file:
        pickle.dump(ensembl_uniprot_ids, pickle_file)


dmel_uniprot_input_file = '/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/full/uniprot/uniprot_sprot_invertebrates_DMEL.dat.gz'
dmel_ensembl_to_uniprot_output_file = 'aux_files/dmel_string_ensembl_uniprot_map.pkl'
create_ensembl_to_uniprot_dict(dmel_uniprot_input_file, dmel_ensembl_to_uniprot_output_file)
