import os

def concatenate_files(input_directory, output_file):
    with open(output_file, 'w') as outfile:
        for root, _, files in os.walk(input_directory):
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith('.metta'):
                    with open(file_path, 'r') as infile:
                        outfile.write(infile.read() + '\n')

# NET ACT:
# input_dir = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/output/net_act"
# out_file = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/output/net_act.metta"

# TOY:
input_dir = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/output/"
# out_file = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/output/net_act.metta"
out_file = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/output/4tfs_net_act.metta"


concatenate_files(input_dir, out_file)
# Usage example:
# concatenate_files('/path/to/input/directory', 'output_file.txt')
