import os
import pandas as pd
import re
import gzip
import shutil
#from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable

# expanded_genes_list = ["Su(var)205", "FBgn0003607", "Top3beta", "FBgn0026015", "Mef2", "FBgn0011656", "Clk", "FBgn0023076",
#                        "Dref", "FBgn0015664", "TfIIB", "FBgn0004915", "Myc", "FBgn0262656", "AGO2", "FBgn0087035",
#                        "Nipped-B", "FBgn0026401", "Cp190", "FBgn0000283", "TfIIA-L", "FBgn0011289", "Trl", "FBgn0013263",
#                        "ash1", "FBgn0005386", "Raf", "FBgn0003079", "Abd-B", "FBgn0000015", "Orc2", "FBgn0286788",
#                        "Rbf", "FBgn0015799", "mof", "FBgn0014340", "msl-1", "FBgn0005617", "Hmr", "FBgn0001206"]

ROUND = 0

def match_word(word: str, text: str):
    '''
    ROUND = 1 exact word
    '''
    if ROUND == 0:
        return word in text
    elif ROUND == 1:
        return re.search(rf"\b{re.escape(word)}\b", text) is not None



########################################################################################################################

def netact_extract_dmel_data_from_uniprot_invertebrate_data(input_file_name, output_file_name, expanded_genes_list):
    #with gzip.open(input_file_name, 'rt') as input_file, open(output_file_name, 'w') as output_file:
    with gzip.open(input_file_name, 'rt') as input_file, gzip.open(output_file_name, 'wt') as output_file:
        next_line = input_file.readline()
        data_lines = []
        netact = False
        while next_line:
            if next_line.startswith("ID") and "_DROME" in next_line:
                #output_file.write(next_line)
                data_lines.append(next_line)
                if any(match_word(gene_symbol, next_line) for gene_symbol in expanded_genes_list):
                        netact = True
                # for netact_comp in expanded_genes_list:
                #     # if netact_comp.lower() in next_line.lower():        # include because any mention to a netact component
                #     if match_word(netact_comp, next_line):
                #     # if match_word(netact_comp.lower(), next_line.lower()):
                        #print(next_line)
                        # netact = True
                next_line = input_file.readline()
                while next_line and not next_line.startswith("ID"):
                    if any(match_word(gene_symbol, next_line) for gene_symbol in expanded_genes_list):
                        netact = True
                    # for netact_comp in expanded_genes_list:
                    #     # if netact_comp.lower() in next_line.lower():  # include because of a mention to a netact component
                    #     if match_word(netact_comp, next_line):
                    #         #print(next_line)
                    #         netact = True
                    data_lines.append(next_line)
                    next_line = input_file.readline()
                    if next_line and next_line.startswith("ID"):
                        if netact:
                            for line in data_lines:
                                output_file.write(line)
                        netact = False
                        data_lines = []
            else:
                next_line = input_file.readline()
        output_file.close()



def netact_extract_dmel_data_from_gz_txt_noheader(input_file_name, output_file_name, expanded_genes_list):
    #print(f"File:::::::::::::-->  {input_file_name}")

    if input_file_name.endswith("gz"):
        file = gzip.open(input_file_name, 'rt')
        with gzip.open(output_file_name, 'wt') as output_file: #with gzip.open(input_file_name, 'rt') as input_file, gzip.open(output_file_name, 'wt') as output_file:
            while True:
                row = file.readline()
                if not row:
                    break
                # strip() added to handle "blank" rows in some TSVs
                if not row.strip():
                    continue
                if ('ReactomePathways.' in input_file_name) or ('ReactomeReactions.' in input_file_name):
                    if 'Drosophila melanogaster' in row:
                        print(f'{input_file_name}:\n {row}')
                        output_file.write(row)
                        # if any(match_word(gene_symbol.upper(), row.upper()) for gene_symbol in expanded_genes_list):
                        #    print(row)                           
                elif any(match_word(gene_symbol.upper(), row.upper()) for gene_symbol in expanded_genes_list):
                    print(f'oooooooooooo{input_file_name}:\n {row}')
                    output_file.write(row)

                # for gene_symbol in expanded_genes_list:
                #     # if gene_symbol.lower() in row.lower():
                #     word = gene_symbol #.lower()
                #     text = row #.lower()  
                #     if 'fly.name_2_string.' in input_file_name or '7227.protein.links.' in input_file_name:   
                #         print(F'STRING')               
                #         if word.startswith('FBpp') and '7227.FBpp' in text:
                #             if word in text:
                #                 output_file.write(row)
                #     elif 'ReactomePathways.' in input_file_name:
                #         if 'Drosophila melanogaster' in text:
                #             if match_word(word.upper(), text.upper()):
                #                 output_file.write(row)  
                #     else:
                #         if match_word(word, text):
                #             output_file.write(row)
            output_file.close()
    else:
        file = open(input_file_name, 'r')
        with open(output_file_name, 'w') as output_file: #with gzip.open(input_file_name, 'rt') as input_file, gzip.open(output_file_name, 'wt') as output_file:
            while True:
                row = file.readline()
                if not row:
                    break
                # strip() added to handle "blank" rows in some TSVs
                if not row.strip():
                    continue                
                if ('ReactomePathways.' in input_file_name) or ('ReactomeReactions.' in input_file_name):
                    if 'Drosophila melanogaster' in row:
                        # if any(match_word(gene_symbol.upper(), row.upper()) for gene_symbol in expanded_genes_list):
                        output_file.write(row)   
                elif any(match_word(gene_symbol, row) for gene_symbol in expanded_genes_list):
                    output_file.write(row)

                # for gene_symbol in expanded_genes_list:
                #     word: str = gene_symbol #.lower()
                #     text = row #.lower()  
                #     if 'fly.name_2_string.' in input_file_name or '7227.protein.links.' in input_file_name:                           
                #         if word.startswith('FBpp') and '7227.FBpp' in text:
                #             if word in text:
                #                 output_file.write(row)
                #     elif 'ReactomePathways.' in input_file_name:
                #         if 'Drosophila melanogaster' in text:
                #             if match_word(word.upper(), text.upper()):
                #                 output_file.write(row)  
                #     else:
                #         if match_word(word, text):
                #             output_file.write(row)
        output_file.close()
    file.close()



def netact_extract_dmel_data_from_FB_GAF(input_file_name, output_file_name, expanded_genes_list):
    #print(f"File:::::::::::::-->  {input_file_name}")

    if input_file_name.endswith("gz"):
        file = gzip.open(input_file_name, 'rt')
        with gzip.open(output_file_name, 'wt') as output_file: #with gzip.open(input_file_name, 'rt') as input_file, gzip.open(output_file_name, 'wt') as output_file:
            while True:
                row = file.readline()                
                if not row:
                    break
                if row.startswith('!'):
                    output_file.write(row)
                    continue
                # strip() added to handle "blank" rows in some TSVs
                if not row.strip():
                    continue
                if any(match_word(gene_symbol.upper(), row.upper()) for gene_symbol in expanded_genes_list):
                    output_file.write(row)
    
    output_file.close()
    file.close()


def netact_extract_dmel_data_for_first_row_header(input_file_name, output_file_name, expanded_genes_list):
    #print(f"File:::::::::::::-->  {input_file_name}")
    header = None
    if input_file_name.endswith(".gz"):
        file = gzip.open(input_file_name, 'rt')
        with gzip.open(output_file_name, 'wt') as output_file:
            output_file.write( file.readline() )
            while True:
                row = file.readline()
                if not row:
                    break
                # strip() added to handle "blank" rows in some TSVs
                if not row.strip():
                    continue
                if any(match_word(gene_symbol, row) for gene_symbol in expanded_genes_list):
                    output_file.write(row)
                # for gene_symbol in expanded_genes_list:
                #     # if gene_symbol.lower() in row.lower():
                #     word = gene_symbol #.lower()
                #     text = row #.lower()
                #     if 'fly.name_2_string.' in input_file_name or '7227.protein.links.' in input_file_name:                  
                #         if word.startswith('FBpp') and '7227.FBpp' in text:
                #             if word in text:
                #                 output_file.write(row)
                #     else:
                #         if match_word(word, text):
                #             output_file.write(row)
    else:
        file = open(input_file_name, 'r')
        with open(output_file_name, 'w') as output_file:
            output_file.write( file.readline() )
            while True:
                row = file.readline()
                if not row:
                    break
                # strip() added to handle "blank" rows in some TSVs
                if not row.strip():
                    continue
                if any(match_word(gene_symbol, row) for gene_symbol in expanded_genes_list):
                    output_file.write(row)
                # for gene_symbol in expanded_genes_list:
                #     # if gene_symbol.lower() in row.lower():
                #     # if match_word(gene_symbol.lower(), row.lower()):
                #     # if match_word(gene_symbol, row):
                #     #     output_file.write(row)
                #     word = gene_symbol #.lower()
                #     text = row #.lower()
                #     if 'fly.name_2_string.' in input_file_name or '7227.protein.links.' in input_file_name:                  
                #         if word.startswith('FBpp') and '7227.FBpp' in text:
                #             if word in text:
                #                 output_file.write(row)
                #     else:
                #         if match_word(word, text):
                #             output_file.write(row)

    file.close()


def list_files(directories_list: list[str]):
    file_list = []
    for directory in directories_list:
        for root, _, files in os.walk(directory):
            for file in files:
                full_path = os.path.join(root, file)
                #full_path = os.path.abspath(os.path.join(root, file))
                file_list.append(full_path)
    return file_list


def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def process_netact_input_files(input_directories: list[str], output_directory, expanded_genes_list: list[str]):
    os.makedirs(output_directory, exist_ok=True)

    input_files = list_files(input_directories)
    # print(f'{input_files}\n for round {ROUND}\nread from {input_directories}\n\n\n\n')
    # exit(9)
    for input_file in input_files:
        print(f"File:::::::::::::-->  {input_file}")
        input_path = input_file  # os.path.join(input_directory, input_file)
        input_path_tokens = input_path.split('/')
        if output_directory.endswith('/'):
            output_path = output_directory + input_path_tokens[-2] + '/' + input_path_tokens[-1][-1]  #.replace(".gz", '')
        else:
            output_path = output_directory + '/' +  input_path_tokens[-2] + '/' +  input_path_tokens[-1]    #.replace(".gz", '')  
        ensure_directory_exists(output_path)
        print(output_path)
        not_input_files_marks = [".tar", ".zip", ".sample", "gtex.forgedb.csv.gz"]
        if any(substring in input_file for substring in not_input_files_marks):
            print(f"{input_file} is not a valid input file... Skipping it!")
        elif "ReactionPMIDS.txt" in input_file or "ReactomePathwaysRelation.txt" in input_file:
            shutil.copy2(input_file, output_path)
            print(f'Reactome pathways relations and literature are full loaded in Net_act...')
            continue            
        elif "_group_" in input_file or "_groups_" in input_file:
            shutil.copy2(input_file, output_path)
            print(f'Group files are unchanged in this Net_act version...')
            continue         
        elif "coxpressdb" in input_file or 'rna_central' in input_file:
            shutil.copy2(input_file, output_path)
            print("CoxpresDB and RNA Central files are kept intact in this simple Net_act")
        elif "_sprot_" in input_file:
            netact_extract_dmel_data_from_uniprot_invertebrate_data(input_path, output_path, expanded_genes_list)
            print(f'Finished for UNIPROT {output_path}\n')
        # elif "Reactome" in input_file or "Drosophila_melanogaster.BDGP6.46.59.gtf.gz" in input_file:
        elif "Reactome" in input_file or "Drosophila_melanogaster.BDGP6.54.62.chr.gtf.gz" in input_file: 
            print(input_file)
            netact_extract_dmel_data_from_gz_txt_noheader(input_path, output_path, expanded_genes_list)
            print(f'Finished for REACTOME or gencode GTF or GAF no header gz {output_path}\n')
        elif "TFLink" in input_file or "tflink" in input_file or "string" in input_file or "afca_afca" in input_file:
            netact_extract_dmel_data_for_first_row_header(input_path, output_path, expanded_genes_list)
            print(f'Finished for TFLink or String {output_path}\n')
        elif "gtex" in input_file:
            netact_extract_dmel_data_for_first_row_header(input_path, output_path, expanded_genes_list)
            print(f'Finished for gtex gzipped {output_path}\n')
        elif "gene_association.fb.gz" in input_file:
            netact_extract_dmel_data_from_FB_GAF(input_path, output_path, expanded_genes_list)
            print(f'Finished for Flybase GAF gz {output_path}\n')
        elif input_file.endswith(".gz"):
            # with gzip.open(input_path, 'rt') as file:
            #     with open(output_path, 'w') as output_file:
            file = gzip.open(input_path, 'rt')
            with gzip.open(output_path, 'wt') as output_file:
                header = None
                previous = None
                lines = file.readlines()
                #print(f'Lines to read: {len(lines)}')
                for i in range(0, len(lines)):
                #for row in input:
                    row = lines[i]
                    # strip() added to handle "blank" row in TSVs
                    if not row or not row.strip():
                        continue

                    if not row.startswith("#"):
                        if header is None and previous is not None:
                            header = previous.lstrip("\t ")
                            output_file.write(header)
                            # print(f'header: {header}')   
                        if any(match_word(gene_symbol, row) for gene_symbol in expanded_genes_list):
                            output_file.write(row)

                        # for gene_symbol in expanded_genes_list:
                        #     # if gene_symbol.lower() in row.lower():
                        #     # if match_word(gene_symbol.lower(), row.lower()):
                        #     if match_word(gene_symbol, row):
                        #         output_file.write(row)

                    if not row.startswith("#-----") and not row.startswith("## Finished "):
                        previous = row

        else:
            file = open(input_path, 'r')
            with open(output_path, 'w') as output_file:
                header = None
                previous = None
                lines = file.readlines()
                #print(f'Lines to read: {len(lines)}')
                for i in range(0, len(lines)):
                    row = lines[i]
                    # strip() added to handle "blank" row in TSVs
                    if not row or not row.strip():
                        continue

                    if not row.startswith("#"):
                        if header is None and previous is not None:
                            header = previous.lstrip("\t ")
                            output_file.write(header)
                            # print(f'header: {header}')                    
                        if any(match_word(gene_symbol, row) for gene_symbol in expanded_genes_list):
                            output_file.write(row)

                    if not row.startswith("#-----") and not row.startswith("## Finished "):
                        previous = row

                output_file.close()
                print(f'Finished for gzipped {output_path}\n')
            file.close()


def add_data(gene_symbol_list, row):
    #organism	gene_type	gene_ID	gene_symbol	gene_fullname	annotation_ID	transcript_type	transcript_ID	transcript_symbol	polypeptide_ID	polypeptide_symbol
    col_data = row.split('\t')
    if col_data[2] not in gene_symbol_list:
        gene_symbol_list.append(col_data[2])  # gene_ID
    if col_data[4] not in gene_symbol_list:
        gene_symbol_list.append(col_data[4])  #	gene_fullname
    if col_data[5] not in gene_symbol_list:
        gene_symbol_list.append(col_data[5])  # annotation_ID
    if col_data[7] not in gene_symbol_list:
        gene_symbol_list.append(col_data[7])  # transcript_ID
    if col_data[8] not in gene_symbol_list:
        gene_symbol_list.append(col_data[8])  # transcript_symbol
    if col_data[9] not in gene_symbol_list:
        gene_symbol_list.append(col_data[9])  # polypeptide_ID
    if col_data[10] not in gene_symbol_list:
        gene_symbol_list.append(col_data[10].rstrip())  # polypeptide_symbol


def expand_gene_list_data(genes_list, fb_fbgn_to_fbtr_fbpp_file):
    expanded_dict = {}
    for gene_symbol in genes_list:
        expanded_dict[gene_symbol]= []

    with gzip.open(fb_fbgn_to_fbtr_fbpp_file, 'rt') as file:
        header = None
        previous = None
        while True:
            row = file.readline()
            if not row:
                break

            # strip() added to handle "blank" rows in some TSVs
            if not row.strip():
                continue

            if not row.startswith("#"):
                if header is None:
                    header = previous.lstrip("#")
                for gene_symbol in genes_list:
                    #if gene_symbol.lower() in row.lower():
                    # if match_word(gene_symbol.lower(), row.lower()):
                    if match_word(gene_symbol, row):
                        add_data(expanded_dict[gene_symbol], row)
            # if not row.startswith("#-----"):
            if not row.startswith("#-----") and not row.startswith("## Finished "):
                previous = row

    expanded_list = []
    for gene_symbol in expanded_dict.keys():
        expanded_list.append(gene_symbol)
        expanded_list.extend(expanded_dict[gene_symbol])
    return expanded_list


genes_list = ["Su(var)205", "Top3beta", "Mef2", "Clk", "Dref", "TfIIB", "Myc", "AGO2", "Nipped-B", "Cp190", "TfIIA-L",
               "Trl", "ash1", "Raf", "Abd-B", "Orc2", "Rbf", "mof", "msl-1", "Hmr"]

four_tf_gene_list = ["Abd-B", "Clk", "Mef2", "Myc"]

tr_pp_file = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase/fbgn_fbtr_fbpp_expanded_fb_2024_06.tsv.gz"
tr_pp_file = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/full/flybase/fbgn_fbtr_fbpp_expanded_fb_2025_05.tsv.gz"
# include net_act gene's transcripts and proteins IDs and symbols in the net_act 
expanded_genes_list = expand_gene_list_data(genes_list, tr_pp_file)
# expanded_genes_list = expand_gene_list_data(four_tf_gene_list, tr_pp_file)

for id in expanded_genes_list:
    print(id)
# exit(9)
# expanded_genes_list = ["Su(var)205", "FBgn0003607", "Top3beta", "FBgn0026015", "Mef2", "FBgn0011656", "Clk", "FBgn0023076",
#                        "Dref", "FBgn0015664", "TfIIB", "FBgn0004915", "Myc", "FBgn0262656", "AGO2", "FBgn0087035",
#                        "Nipped-B", "FBgn0026401", "Cp190", "FBgn0000283", "TfIIA-L", "FBgn0011289", "Trl", "FBgn0013263",
#                        "ash1", "FBgn0005386", "Raf", "FBgn0003079", "Abd-B", "FBgn0000015", "Orc2", "FBgn0286788",
#                        "Rbf", "FBgn0015799", "mof", "FBgn0014340", "msl-1", "FBgn0005617", "Hmr", "FBgn0001206"]


# Bizon's paths:
input_dirs = [
    "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/to_round_0_net_act_sources/"
    #"/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy"
]
# output_dir = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/4toy_net_act"# "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/net_act"
output_dir = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/to_round_1_net_act_sources"
ROUND = 0
process_netact_input_files(input_dirs, output_dir, expanded_genes_list)
input_dirs[0] = output_dir
output_dir = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/net_act_sources"
ROUND = 1
process_netact_input_files(input_dirs, output_dir, expanded_genes_list)

#
# process_netact_input_files(["/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy"],
#                             "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy_net_act",
#                            expanded_genes_list)
