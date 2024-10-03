import anndata as ad
from flybase_tsv_reader import FlybasePrecomputedTable
import psycopg2
import gzip

#
# def get_symbol_data_from_Flybase(feature_symbol: str):
#     # Connection setup
#     connection = psycopg2.connect(
#         host="chado.flybase.org",
#         database="flybase",
#         user="flybase",
#         password=""  # Add the password if there is one
#     )
#
#     try:
#         # Create a cursor
#         cursor = connection.cursor()
#
#         # Execute SQL query
#         query = f"SELECT uniquename FROM feature WHERE feature.name LIKE '{feature_symbol}';"
#         cursor.execute(query)
#
#         # Get the results
#         results = cursor.fetchall()
#
#         # Display the results
#         # for row in results:
#         #     print(row)
#         return results
#     except Exception as e:
#         print(f"An error occurred: {e}")
#
#     finally:
#         # Close the connection
#         if connection:
#             cursor.close()
#             connection.close()
#
# # Function to connect to the database
# def connect_to_db():
#     try:
#         connection = psycopg2.connect(
#             host="chado.flybase.org",
#             database="flybase",
#             user="flybase",
#             password=""  # Add the password if there is one
#         )
#         return connection
#     except Exception as e:
#         print(f"Error connecting to the database: {e}")
#         return None
#
# # Function to execute a query
# def execute_query(connection, query):
#     try:
#         cursor = connection.cursor()
#         cursor.execute(query)
#         results = cursor.fetchall()
#         cursor.close()
#         return results
#     except Exception as e:
#         print(f"Error executing the query: {e}")
#         return None
#
# # Example usage
# connection = connect_to_db()
#
# symbols_to_fbgn_file = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase/fbgn_fbtr_fbpp_expanded_fb_2024_03.tsv.gz"
#
# symbols_to_fbgn_table = FlybasePrecomputedTable(symbols_to_fbgn_file)
# dframe = symbols_to_fbgn_table.to_pandas_dataframe()
# transcript_list = dframe['transcript_symbol'].tolist()
# transcript_list = dframe['gene_symbol'].to_list()
# #
# print("Reading h5ad file..")
# adata = ad.read_h5ad('/home/saulo/Downloads/adata_headBody_S_v1.0.h5ad')
# found = 0
# symbs = []
# for tr_symb in transcript_list:
#     if tr_symb in adata.var_names:
#         found += 1
#         symbs.append(tr_symb)
#         #print(tr_symb)
# symbs = set(symbs)
# print(f'Found {len(symbs)} of {len(adata.var_names)}')
# not_found = []
# for gene_symbol in adata.var_names:
#     if gene_symbol not in symbs:
#         not_found.append(gene_symbol)
# print(not_found)
#
# if connection:
#     ok = 0
#     for symbol in not_found:
#         query = f"SELECT uniquename, is_obsolete FROM feature WHERE feature.name LIKE '{symbol}';"
#         results = execute_query(connection, query)
#
#         for row in results:
#             print(f'{symbol}: {row}')
#             if row[1] == False:
#                 print(f'{symbol}:  {row}')
#                 ok += 1
#             else:
#                 print(f'NOT A FRESH UNIQUENAME FOR {symbol}: {row}')
#     print(f'symbols: {len(not_found)} / Found: {ok}')
#
#     connection.close()

#exit(9)
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

import pandas as pd
import numpy as np
import anndata as ad


def group_cell_type_mean(adata, group_key, layer=None, gene_symbols=None):
    if layer is not None:
        getX = lambda x: x.layers[layer]
    else:
        getX = lambda x: x.X
    if gene_symbols is not None:
        new_idx = adata.var[idx]
    else:
        new_idx = adata.var_names    
  
    
    grouped = adata.obs.groupby(group_key, observed=False)
    print(adata.shape)
    print(len(grouped))
    #print(adata.obs)
    #exit(9)    
    out = pd.DataFrame(
        np.zeros((adata.shape[1], len(grouped)), dtype=np.float64),
        columns=list(grouped.groups.keys()),
        index=adata.var_names
    )

    for group, idx in grouped.indices.items():
        X = getX(adata[idx])
        # print(f'group:\n{out}')
        # print(f'idx:\n{idx}')
        out[group] = np.ravel(X.mean(axis=0, dtype=np.float64)).tolist()
        # print(f'group[out]:\n{out[group]}')
                
    return out


def group_time_cell_type_mean(adata, group_key, layer=None, gene_symbols=None):
    if layer is not None:
        getX = lambda x: x.layers[layer]
    else:
        getX = lambda x: x.X
    if gene_symbols is not None:
        new_idx = adata.var[idx]
    else:
        new_idx = adata.var_names    
    
    # List of ages to filter
    ages = ['5', '30', '50', '70']

    # Final DataFrame to accumulate all results
    full_out = pd.DataFrame(index=adata.var_names)

    # Iterate over each cell type and age
    for cell_type in adata.obs.afca_annotation.cat.categories:
        for age in ages:
            column = f'{cell_type}_{age}'
            print(f"Processing: {column}")

            # Filter data based on cell type and age
            tmp_data = adata[ (adata.obs.age == age) & (adata.obs.afca_annotation == cell_type) ]
            # Check if there are any rows after filtering
            if tmp_data.shape[0] == 0:
                print(f"No data found for {column}")
                continue
            
            grouped = tmp_data.obs.groupby(group_key, observed=False)

            # print(f'coluns: {list(grouped.groups.keys())}')
            # print(f'coluns: {[column]}')
            # exit(9)
            # Initialize DataFrame for grouped data of this cell type and age
            out = pd.DataFrame(
                np.zeros((tmp_data.shape[1], len(grouped)), dtype=np.float64),
                columns=list(grouped.groups.keys()),
                index=adata.var_names
            )

            for group, idx in grouped.indices.items():
                X = getX(tmp_data[idx])
                out[group] = np.ravel(X.mean(axis=0, dtype=np.float64)).tolist()

            out.columns = [column]
            # Add the results of this iteration to the final DataFrame
            full_out = pd.concat([full_out, out], axis=1)
    
    #  Add the index as a column and rename it to "#FB gene symbol"
    full_out.reset_index(inplace=True)
    full_out.rename(columns={'index': '#FB gene symbol'}, inplace=True)

    return full_out


print("Reading h5ad file..")
adata = ad.read_h5ad('/home/saulo/Downloads/adata_headBody_S_v1.0.h5ad')
print(adata)

print(adata.obs["afca_annotation"])
exit(9)

group_key = 'afca_annotation'  
group_key = 'sex_age' # :(
group_key = 'fca_annotation'  
group_key = 'afca_annotation'  
layer = None 
gene_symbols = None  

# Call the function to group the data and save the result to a TSV file
grouped_means = group_time_cell_type_mean(adata, group_key, layer=layer, gene_symbols=gene_symbols)
grouped_means.to_csv(f"/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/afca_{group_key}_group_by_mean.tsv", sep='\t', index=False)


exit(9)


# for t in adata.obs.columns:
#     print(adata.obs[t])
# exit(9)
# print(adata.obs["afca_annotation"])
# print(adata.obs["age"])
# print(adata.obs["sex_age"])
# print(adata.obs["tissue"])
# print(adata.obs["total_counts"])
# print(adata.obs["total_counts_mt"])
# #exit(9)
# print(adata.obs["afca_annotation"])
# group_key = 'sex_age'  
# group_key = 'tissue'
# group_key = 'total_counts'  
# group_key = 'total_counts_mt'  




# Display a summary of the data
print(adata)

print(f'\nAnnotations:\n{adata.obs["afca_annotation"].keys()}')
unannots = 0
for i in range(len( adata.obs["afca_annotation"] )):
    if adata.obs["afca_annotation"].iloc[i] == "unannotated":
        unannots += 1
        #print(f'{adata.obs["afca_annotation"].keys()[i]}\t{adata.obs["afca_annotation"].iloc[i]}')
print(f'{unannots} / {len( adata.obs["afca_annotation"] )}')

# Cell names
print(f'{adata.obs_names}\n\nAll obs names ({len(adata.obs_names)}):')
# for  obs_name in adata.obs_names:
#     print(obs_name)
# exit(9)
# Gene names
print(adata.var_names)
# for i in range(5000):
#     print(adata.var_names[i])
# Example of expression data visualization
print(adata.X[:50, :50])  # Displays a slice of the expression data
print(f'adata shape: {adata.shape}\nadata x: \n{adata.X}')

#
# for data in adata.X:
#     data = (str(data)).split(" (),\t\n")
#     print(f'data: {data}')
    # if (float(data[2]) < 0):
    #     print(f'Log 2: {data}')

# import numpy as np

# # Iterate over each non-zero element
# print("Checking negative values...")
# p = 0
# m = 0
# for i, j in zip(*adata.X.nonzero()):
#     value = adata.X[i, j]
#     p += 1
#     if p % 10000000 == 0:
#         print(f"10 million more points (p: {p})")
#         m += 1
#     if value < 0:
#         print(f"Index: ({i}, {j})\tValue: {value}")
# print(f'millions: {m}. non-zero points: {p}')           # there are 358312995 data points in adata_headBody_S_v1.0.h5ad file!
# exit(9)
# Check metadata
print(f'adata.uns:\n{adata.uns})')  # 'uns' may contain information about normalization
print(f'adata.obs_names_make_unique:\n{adata.obs_names_make_unique()})')

# Check if the file has data layers
if hasattr(adata, 'layers') and len(adata.layers) > 0:
    print("Available layers:", adata.layers.keys())
else:
    print("No layers available or the file does not contain 'layers'.")

# print(adata.X[:50, :50])  # Displays the first 5 values of the matrix for a sample
#
# print("Gene names (15992):")
# for  var_name in adata.var_names:
#     print(var_name)


# import h5py
#
# # Open the .h5ad file
# with h5py.File('/home/saulo/Downloads/adata_headBody_S_v1.0.h5ad', 'r') as f:
#     # List groups and datasets in the file
#     def print_attrs(name, obj):
#         print(name)
#     f.visititems(print_attrs)

























#
#
#
#
#
#
# import anndata as ad
# from flybase_tsv_reader import FlybasePrecomputedTable
# import psycopg2
# import gzip
#
# #
# # def get_symbol_data_from_Flybase(feature_symbol: str):
# #     # Configuração da conexão
# #     connection = psycopg2.connect(
# #         host="chado.flybase.org",
# #         database="flybase",
# #         user="flybase",
# #         password=""  # Adicione a senha se houver uma
# #     )
# #
# #     try:
# #         # Criação de um cursor
# #         cursor = connection.cursor()
# #
# #         # Execução da consulta SQL
# #         query = f"SELECT uniquename FROM feature WHERE feature.name LIKE '{feature_symbol}';"
# #         cursor.execute(query)
# #
# #         # Obtenção dos resultados
# #         results = cursor.fetchall()
# #
# #         # Exibição dos resultados
# #         # for row in results:
# #         #     print(row)
# #         return results
# #     except Exception as e:
# #         print(f"Ocorreu um erro: {e}")
# #
# #     finally:
# #         # Fechamento da conexão
# #         if connection:
# #             cursor.close()
# #             connection.close()
# #
# # # Função para conectar ao banco de dados
# # def connect_to_db():
# #     try:
# #         connection = psycopg2.connect(
# #             host="chado.flybase.org",
# #             database="flybase",
# #             user="flybase",
# #             password=""  # Adicione a senha se houver uma
# #         )
# #         return connection
# #     except Exception as e:
# #         print(f"Erro ao conectar ao banco de dados: {e}")
# #         return None
# #
# # # Função para executar uma consulta
# # def execute_query(connection, query):
# #     try:
# #         cursor = connection.cursor()
# #         cursor.execute(query)
# #         results = cursor.fetchall()
# #         cursor.close()
# #         return results
# #     except Exception as e:
# #         print(f"Erro ao executar a consulta: {e}")
# #         return None
# #
# # # Exemplo de uso
# # connection = connect_to_db()
# #
# # symbols_to_fbgn_file = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase/fbgn_fbtr_fbpp_expanded_fb_2024_03.tsv.gz"
# #
# # symbols_to_fbgn_table = FlybasePrecomputedTable(symbols_to_fbgn_file)
# # dframe = symbols_to_fbgn_table.to_pandas_dataframe()
# # transcript_list = dframe['transcript_symbol'].tolist()
# # transcript_list = dframe['gene_symbol'].to_list()
# # #
# # print("Reading h5ad file..")
# # adata = ad.read_h5ad('/home/saulo/Downloads/adata_headBody_S_v1.0.h5ad')
# # found = 0
# # symbs = []
# # for tr_symb in transcript_list:
# #     if tr_symb in adata.var_names:
# #         found += 1
# #         symbs.append(tr_symb)
# #         #print(tr_symb)
# # symbs = set(symbs)
# # print(f'Found {len(symbs)} of {len(adata.var_names)}')
# # not_found = []
# # for gene_symbol in adata.var_names:
# #     if gene_symbol not in symbs:
# #         not_found.append(gene_symbol)
# # print(not_found)
# #
# # if connection:
# #     ok = 0
# #     for symbol in not_found:
# #         query = f"SELECT uniquename, is_obsolete FROM feature WHERE feature.name LIKE '{symbol}';"
# #         results = execute_query(connection, query)
# #
# #         for row in results:
# #             print(f'{symbol}: {row}')
# #             if row[1] == False:
# #                 print(f'{symbol}:  {row}')
# #                 ok += 1
# #             else:
# #                 print(f'NOT A FRESH UNIQUENAME FOR {symbol}: {row}')
# #     print(f'symbols: {len(not_found)} / Found: {ok}')
# #
# #     connection.close()
#
# #exit(9)
# def add_data(gene_symbol_list, row):
#     #organism	gene_type	gene_ID	gene_symbol	gene_fullname	annotation_ID	transcript_type	transcript_ID	transcript_symbol	polypeptide_ID	polypeptide_symbol
#     col_data = row.split('\t')
#     if col_data[2] not in gene_symbol_list:
#         gene_symbol_list.append(col_data[2])  # gene_ID
#     if col_data[4] not in gene_symbol_list:
#         gene_symbol_list.append(col_data[4])  #	gene_fullname
#     if col_data[5] not in gene_symbol_list:
#         gene_symbol_list.append(col_data[5])  # annotation_ID
#     if col_data[7] not in gene_symbol_list:
#         gene_symbol_list.append(col_data[7])  # transcript_ID
#     if col_data[8] not in gene_symbol_list:
#         gene_symbol_list.append(col_data[8])  # transcript_symbol
#     if col_data[9] not in gene_symbol_list:
#         gene_symbol_list.append(col_data[9])  # polypeptide_ID
#     if col_data[10] not in gene_symbol_list:
#         gene_symbol_list.append(col_data[10].rstrip())  # polypeptide_symbol
#
#
# def expand_gene_list_data(genes_list, fb_fbgn_to_fbtr_fbpp_file):
#     expanded_dict = {}
#     for gene_symbol in genes_list:
#         expanded_dict[gene_symbol]= []
#
#     with gzip.open(fb_fbgn_to_fbtr_fbpp_file, 'rt') as file:
#         header = None
#         previous = None
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
#                 for gene_symbol in genes_list:
#                     if gene_symbol in row:
#                         add_data(expanded_dict[gene_symbol], row)
#             # if not row.startswith("#-----"):
#             if not row.startswith("#-----") and not row.startswith("## Finished "):
#                 previous = row
#
#     expanded_list = []
#     for gene_symbol in expanded_dict.keys():
#         expanded_list.append(gene_symbol)
#         expanded_list.extend(expanded_dict[gene_symbol])
#     return expanded_list
#
#
#
# # print("Reading h5ad file..")
# adata = ad.read_h5ad('/home/saulo/Downloads/adata_headBody_S_v1.0.h5ad')
#
# # Exibir um resumo dos dados
# print(adata)
#
# # Nomes das células
# print(f'{adata.obs_names}\n\nAll obs names ({len(adata.obs_names)}):')
# for  obs_name in adata.obs_names:
#     print(obs_name)
# exit(9)
# # Nomes dos genes
# print(adata.var_names)
# for i in range(5000):
#     print(adata.var_names[i])
# # Exemplo de visualização de dados de expressão
# print(adata.X[:50, :50])  # Exibe uma fatia dos dados de expressão
#
#
# # Verificar metadados
# print(adata.uns)  # 'uns' pode conter informações sobre a normalização
#
# # Verificar se o arquivo tem camadas de dados
# if hasattr(adata, 'layers') and len(adata.layers) > 0:
#     print("Camadas disponíveis:", adata.layers.keys())
# else:
#     print("Não há camadas disponíveis ou o arquivo não contém 'layers'.")
#
# print(adata.X[:50, :50])  # Exibe os primeiros 5 valores da matriz para uma amostra
#
# # import h5py
# #
# # # Abrir o arquivo .h5ad
# # with h5py.File('/home/saulo/Downloads/adata_headBody_S_v1.0.h5ad', 'r') as f:
# #     # Listar grupos e datasets no arquivo
# #     def print_attrs(name, obj):
# #         print(name)
# #     f.visititems(print_attrs)
