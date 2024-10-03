import os
import pandas as pd
import gzip
#from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable

# expanded_genes_list = ["Su(var)205", "FBgn0003607", "Top3beta", "FBgn0026015", "Mef2", "FBgn0011656", "Clk", "FBgn0023076",
#                        "Dref", "FBgn0015664", "TfIIB", "FBgn0004915", "Myc", "FBgn0262656", "AGO2", "FBgn0087035",
#                        "Nipped-B", "FBgn0026401", "Cp190", "FBgn0000283", "TfIIA-L", "FBgn0011289", "Trl", "FBgn0013263",
#                        "ash1", "FBgn0005386", "Raf", "FBgn0003079", "Abd-B", "FBgn0000015", "Orc2", "FBgn0286788",
#                        "Rbf", "FBgn0015799", "mof", "FBgn0014340", "msl-1", "FBgn0005617", "Hmr", "FBgn0001206"]

def search_dataframe(target_string, df):
    # Obtém o número de linhas e colunas no DataFrame
    num_rows, num_cols = df.shape

    # Percorre cada linha do DataFrame
    for row_index in range(num_rows):
        row = df.iloc[row_index]  # Obtém a linha atual como uma Série

        # Percorre cada coluna na linha atual
        line = ""
        found = False
        for col_name in df.columns:
            cell_value = row[col_name]  # Obtém o valor da célula
            line += cell_value + "\t"
            # Verifica se o valor da célula é igual à string alvo
            if cell_value == target_string:
                # Se for igual, imprima a linha inteira
                #print(row)
                found = True
        if found:
            print(line)

def process_flybase_tsv_files(input_directory, expanded_genes_list):
    new_directory = input_directory + "_net_act"
    os.makedirs(new_directory, exist_ok=True)

    input_files = [f for f in os.listdir(input_directory) if f.endswith(".tsv")]

    for input_file in input_files:
        print(f"Table:::::::::::::-->  {input_file}")
        input_path = os.path.join(input_directory, input_file)
        output_path = os.path.join(new_directory, input_file)

        header = None
        relevant_rows = []
        with open(input_path, 'r') as file:
            with open(output_path, 'w') as output_file:
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
                            output_file.write(header)
                            print(header)

                        for gene_symbol in expanded_genes_list:
                            if gene_symbol in row:
                                output_file.write(row)
                    #if not row.startswith("#-----"):
                    if not row.startswith("#-----") and not row.startswith("## Finished "):
                        previous = row

                    #print(row)
            output_file.close()
            print(f'Finished for {input_path}')
        file.close()


def process_texttsv_files(input_directory, expanded_genes_list):
    new_directory = input_directory + "_net_act"
    os.makedirs(new_directory, exist_ok=True)

    input_files = [f for f in os.listdir(input_directory) if f.endswith(".tsv")]

    for input_file in input_files:
        print(f"Table:::::::::::::-->  {input_file}")
        input_path = os.path.join(input_directory, input_file)
        output_path = os.path.join(new_directory, input_file)

        header = None
        relevant_rows = []
        with open(input_path, 'r') as file:
            with open(output_path, 'w') as output_file:
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
                            output_file.write(header)
                            print(header)

                        for gene_symbol in expanded_genes_list:
                            if gene_symbol in row:
                                output_file.write(row)
                    #if not row.startswith("#-----"):
                    if not row.startswith("#-----") and not row.startswith("## Finished "):
                        previous = row

                    #print(row)
            output_file.close()
            print(f'Finished for {input_path}')
        file.close()
        # with open(output_path, 'w') as output_file:
        #     output_file.write(str(header))
        #     #output_file.write(''.join(relevant_rows))
        #     for aRow in relevant_rows:
        #         output_file.write(aRow)
        #     # for i in range(len(relevant_rows)):
        #     #     #output_file.write('\t'.join(map(str, line)) + '\n')
        #     #     aRow = relevant_rows[i]
        #     #     if i != (len(relevant_rows - 1)):
        #     #         output_file.write(aRow + '\n')
        #     #     else:
        #     #         output_file.write(aRow)
        # output_file.close()


def _add_row(row, __rows):
    if row not in __rows:
        __rows.append(row)


def _process_gziped_tsv(gziped_file_name, __header, __rows):
    header = None
    previous = None
    with gzip.open(gziped_file_name, 'rt') as input:
        for row in input:
            # strip() added to handle "blank" row in TSVs
            #print(row)
            if not row or not row.strip():
                continue

            if not row.startswith("#"):
                if header is None and previous is not None:
                    header = previous.lstrip("#")
                    header = [column_name.strip() for column_name in header.split('\t')]
                    __header.append(header)
                    print(__header)
                row_list = [value.strip() for value in row.split('\t')]
                _add_row(row_list, __rows)
            if not row.startswith("#-----") and not row.startswith("## Finished "):  # and not row.startswith("#START: "):
                previous = row
        # exit(9)

        
def process_gzip_files(input_directory, string_list):
    new_directory = input_directory + "_net_act"
    os.makedirs(new_directory, exist_ok=True)

    input_files = [f for f in os.listdir(input_directory) if f.endswith(".tsv.gz")]

    for input_file in input_files:
        print(f"Table:::::::::::::-->  {input_file}")
        input_path = os.path.join(input_directory, input_file)
        output_path = os.path.join(new_directory, input_file)

        __header = []
        __rows = []
        _process_gziped_tsv(input_path, __header, __rows)
        print(__header)
        #print(__rows)
        __header = __header[0]
        df = pd.DataFrame(__rows, columns=__header)
        relevant_lines = []
        df = df.fillna('')
        # for string in string_list:
        #     search_dataframe(string, df)
        #continue

        for string in string_list:
            mask = df.apply(lambda row: string in ' '.join(map(str, row)), axis=1)
            relevant_lines.extend(df[mask].values.tolist())

            #relevant_lines.extend(df[df.apply(lambda row: string in ' '.join(map(str, row)), axis=1)].values.tolist())
            #df = df[df.apply(lambda row: string in ' '.join(map(str, row)), axis=1)]
#        relevant_lines = df[df.apply(lambda row: any(string in ' '.join(map(str, row)) for string in expanded_genes_list), axis=1)]
        #df = pd.DataFrame(relevant_lines, columns=df.columns)
        #print(df)
        with open(output_path, 'w') as output_file:
            output_file.write('\t'.join(map(str, __header)) + '\n')
            for line in relevant_lines:
                output_file.write('\t'.join(map(str, line)) + '\n')


'''
def process_tsv_tables(input_directory, expanded_genes_list):
    new_directory = input_directory + "_net_act"
    os.makedirs(new_directory, exist_ok=True)

    input_files = [f for f in os.listdir(input_directory) if f.endswith(".tsv.gz")]

    for input_file in input_files:
        print(f"Table:::::::::::::-->  {input_file}")
        input_path = os.path.join(input_directory, input_file)
        output_path = os.path.join(new_directory, input_file.replace(".gz", ''))
        fb_gg_table = FlybasePrecomputedTable(input_path)
        header = fb_gg_table.get_header()
        #rows = fb_gg_table.get_rows()
        df = fb_gg_table.to_pandas_dataframe()
        relevant_lines = []
        df = df.fillna('')
        print(f"Table:::::::::::::-->  {output_path}")
        for string in expanded_genes_list:
            search_dataframe(string, df)
        #continue

        for string in expanded_genes_list:
            mask = df.apply(lambda row: string in ' '.join(map(str, row)), axis=1)
            relevant_lines.extend(df[mask].values.tolist())

            #relevant_lines.extend(df[df.apply(lambda row: string in ' '.join(map(str, row)), axis=1)].values.tolist())
            #df = df[df.apply(lambda row: string in ' '.join(map(str, row)), axis=1)]
#        relevant_lines = df[df.apply(lambda row: any(string in ' '.join(map(str, row)) for string in genes_list), axis=1)]
        #df = pd.DataFrame(relevant_lines, columns=df.columns)
        #print(df)
        with open(output_path, 'w') as output_file:
            output_file.write('\t'.join(map(str, header)) + '\n')
            for line in relevant_lines:
                output_file.write('\t'.join(map(str, line)) + '\n')
'''
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
                for netact_comp in expanded_genes_list:
                    if netact_comp in next_line:        # include because any mention to a netact component
                        #print(next_line)
                        netact = True
                next_line = input_file.readline()
                while next_line and not next_line.startswith("ID"):
                    for netact_comp in expanded_genes_list:
                        if netact_comp in next_line:  # include because any mention to a netact component
                            #print(next_line)
                            netact = True
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
                for gene_symbol in expanded_genes_list:
                    if gene_symbol in row:
                        output_file.write(row)
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
                for gene_symbol in expanded_genes_list:
                    if gene_symbol in row:
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
                for gene_symbol in expanded_genes_list:
                    if gene_symbol in row:
                        output_file.write(row)
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
                for gene_symbol in expanded_genes_list:
                    if gene_symbol in row:
                        output_file.write(row)
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
    #output_directory = input_directory + "_net_act"
    os.makedirs(output_directory, exist_ok=True)

    input_files = list_files(input_directories)
    print(input_files)
    #exit(9)
    for input_file in input_files:
        print(f"File:::::::::::::-->  {input_file}")
        input_path = input_file  # os.path.join(input_directory, input_file)
        if output_directory.endswith('/'):
            output_path = output_directory + input_path.split('/')[-1]  #.replace(".gz", '')
        else:
            output_path = output_directory + '/' + input_path.split('/')[-1]    #.replace(".gz", '')  
        ensure_directory_exists(output_path)

        not_input_files_marks = ["ReactionP", ".tar", ".zip", ".sample", "gtex.forgedb.csv.gz"]
        if any(substring in input_file for substring in not_input_files_marks):
        #if "ReactionP" in input_file or "ReactomePath" in input_file or ".tar" in input_file or ".zip" in input_file or "sample" in input_file:
            print("Not an input file...")
            continue
        elif "_sprot_" in input_file:
            netact_extract_dmel_data_from_uniprot_invertebrate_data(input_path, output_path, expanded_genes_list)
            print(f'Finished for UNIPROT {output_path}')
            continue
        elif "Reactome" in input_file or "Drosophila_melanogaster.BDGP6.46.59.gtf.gz" in input_file or ".fb.gz" in input_file:
            netact_extract_dmel_data_from_gz_txt_noheader(input_path, output_path, expanded_genes_list)
            print(f'Finished for REACTOME or gencode GTF no header gz {output_path}')
            continue
        elif "TFLink" in input_file or "tflink" in input_file or "string" in input_file:
            netact_extract_dmel_data_for_first_row_header(input_path, output_path, expanded_genes_list)
            print(f'Finished for TFLink or String {output_path}')
            continue
        elif "gtex" in input_file:
            netact_extract_dmel_data_for_first_row_header(input_path, output_path, expanded_genes_list)
            print(f'Finished for gtex gzipped {output_path}')
            continue
        if input_file.endswith(".gz"):
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
                            #header = [column_name.strip() for column_name in header.split('\t') ]
                            #self._set_header(header)
                            output_file.write(header)
                            # print(f'header: {header}')                    
                        for gene_symbol in expanded_genes_list:
                            if gene_symbol in row:
                                output_file.write(row)

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
                #for row in input:
                    row = lines[i]
                    # strip() added to handle "blank" row in TSVs
                    if not row or not row.strip():
                        continue

                    if not row.startswith("#"):
                        if header is None and previous is not None:
                            header = previous.lstrip("\t ")
                            #header = [column_name.strip() for column_name in header.split('\t') ]
                            #self._set_header(header)
                            output_file.write(header)
                            # print(f'header: {header}')                    
                        for gene_symbol in expanded_genes_list:
                            if gene_symbol in row:
                                output_file.write(row)

                    if not row.startswith("#-----") and not row.startswith("## Finished "):
                        previous = row

        output_file.close()
        print(f'Finished for gzipped {output_path}')
        file.close()
        #     continue
        # # tsv, txt,...
        # header = None
        # relevant_rows = []
        # with open(input_path, 'r') as file:
        #     with open(output_path, 'w') as output_file:
        #         while True:
        #             row = file.readline()
        #             if not row:
        #                 break
        #
        #             # strip() added to handle "blank" rows in some TSVs
        #             if not row.strip():
        #                 continue
        #
        #             if not row.startswith("#"):
        #                 if header is None:
        #                     header = previous.lstrip("#")
        #                     output_file.write(header)
        #                     print(header)
        #
        #                 for gene_symbol in expanded_genes_list:
        #                     if gene_symbol in row:
        #                         output_file.write(row)
        #             #if not row.startswith("#-----"):
        #             if not row.startswith("#-----") and not row.startswith("## Finished "):
        #                 previous = row
        #
        #             #print(row)
        #     output_file.close()
        #     print(f'Finished for {output_path}')
        # file.close()

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
                    if gene_symbol in row:
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

tr_pp_file = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase/fbgn_fbtr_fbpp_expanded_fb_2024_04.tsv.gz"
#tr_pp_file = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/full/flybase/fbgn_fbtr_fbpp_expanded_fb_2024_04.tsv.gz"
#expanded_genes_list = expand_gene_list_data(genes_list, tr_pp_file)
expanded_genes_list = expand_gene_list_data(four_tf_gene_list, tr_pp_file)

print(expanded_genes_list)
#exit(9)
# expanded_genes_list = ["Su(var)205", "FBgn0003607", "Top3beta", "FBgn0026015", "Mef2", "FBgn0011656", "Clk", "FBgn0023076",
#                        "Dref", "FBgn0015664", "TfIIB", "FBgn0004915", "Myc", "FBgn0262656", "AGO2", "FBgn0087035",
#                        "Nipped-B", "FBgn0026401", "Cp190", "FBgn0000283", "TfIIA-L", "FBgn0011289", "Trl", "FBgn0013263",
#                        "ash1", "FBgn0005386", "Raf", "FBgn0003079", "Abd-B", "FBgn0000015", "Orc2", "FBgn0286788",
#                        "Rbf", "FBgn0015799", "mof", "FBgn0014340", "msl-1", "FBgn0005617", "Hmr", "FBgn0001206"]


# Bizon's paths:
input_dirs = [
    #"/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/to_net_act/"
    "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy"
]
output_dir = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/4toy_net_act"# "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/net_act"
#output_dir = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/net_act_sources"
process_netact_input_files(input_dirs, output_dir, expanded_genes_list)

#
# process_netact_input_files(["/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy"],
#                             "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy_net_act",
#                            expanded_genes_list)
