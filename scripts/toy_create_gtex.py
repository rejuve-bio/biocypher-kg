import os
import gzip
import shutil


#input_dir = '/mnt/hdd_2/abdu/biocypher_data/gtex/eqtl/GTEx_Analysis_v8_eQTL'
#output_dir = '/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/toy/gtex/eqtl/GTEx_Analysis_v8_eQTL'
input_dir = '/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/full/flybase'
output_dir = '/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/toy/flybase'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for filename in os.listdir(input_dir):
    if filename.endswith('.gz'):
        input_path = os.path.join(input_dir, filename)
        output_filename = filename[:-3] 
        output_path = os.path.join(output_dir, output_filename)
        
        with gzip.open(input_path, 'rt') as input_file, open(output_path, 'w') as output_file:
            for _ in range(30):
                line = input_file.readline()
                if not line:
                    break
                output_file.write(line)
                

