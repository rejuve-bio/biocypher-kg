import os
import pandas as pd
import gzip
from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable

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

def process_files(input_directory, genes_list):
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

                        for gene_symbol in genes_list:
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
#        relevant_lines = df[df.apply(lambda row: any(string in ' '.join(map(str, row)) for string in genes_list), axis=1)]
        #df = pd.DataFrame(relevant_lines, columns=df.columns)
        #print(df)
        with open(output_path, 'w') as output_file:
            output_file.write('\t'.join(map(str, __header)) + '\n')
            for line in relevant_lines:
                output_file.write('\t'.join(map(str, line)) + '\n')



def process_tsv_tables(input_directory, genes_list):
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
        for string in genes_list:
            search_dataframe(string, df)
        #continue

        for string in genes_list:
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


# Example usage:
genes_list = ["Su(var)205", "Top3beta", "Mef2", "Clk", "Dref", "TfIIB", "Myc", "AGO2", "Nipped-B", "Cp190", "TfIIA-L",
               "Trl", "ash1", "Raf", "Abd-B", "Orc2", "Rbf", "mof", "msl-1", "Hmr"]
input_directory = "/home/saulo/snet/hyperon/das/das/flybase2metta/fb_data/2023_05/tmp"
input_directory = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase"
#input_directory = "/mnt/hdd_2/saulo/snet/rejuve.bio/das/shared_rep/data/input/full/flybase"
process_files(input_directory, genes_list)
#process_gzip_files(input_directory, genes_list)
#process_tsv_tables(input_directory, string_list)
# /home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase