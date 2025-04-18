import csv
import os
import gzip
import pandas
import re

class FlybasePrecomputedTable:
    def __init__(self, tsv_file_name):
        self.__header = []
        self.__rows = []
        self._proces_input_tsv(tsv_file_name)

    def get_header(self):
        return self.__header

    def get_rows(self):
        return self.__rows

    def to_pandas_dataframe(self) -> pandas.DataFrame:
        dataframe = pandas.DataFrame(self.__rows, columns=self.__header)
        return dataframe
    
    def from_pandas_dataframe(self, df: pandas.DataFrame):
        self.__header = list(df.columns)
        self.__rows = df.values.tolist()
        return self

    def extract_date_string(self, file_name):
        pattern = r"fb_(\d{4}_\d{2})"
        match = re.search(pattern, file_name)
        if match:
            return match.group(1)
        else:
            return None

    def _set_header(self, header):
        self.__header = header

    def _add_row(self, row):
        if row[0] == self.__header[0]:
            return
        #if row not in self.__rows:
        self.__rows.append(row)


    def _proces_input_tsv(self, input_file_name: str):
        header = None
        previous: str = None
        if input_file_name.endswith(".gz"):
            input = gzip.open(input_file_name, 'rt')
        elif input_file_name.endswith(".tsv"):
            input = open(input_file_name, 'r')
        else:
            print(f'Invalid input file type. Only gzipped (.gz) or .tsv are allowed: {input_file_name}')
            return

        lines = input.readlines()
        #print(f'Lines to read: {len(lines)}')
        for i in range(0, len(lines)):
        #for row in input:
            row = lines[i]
            # print(row)
            # strip() added to handle "blank" row in TSVs
            if not row or not row.strip():
                continue

            if not row.startswith("#"):
                if header is None and previous is not None:
                    header = previous.lstrip("#\t ")
                    header = [column_name.strip() for column_name in header.split('\t') ]
                    self._set_header(header)
                row_list = [value.strip() for value in row.split('\t')]
                self._add_row(row_list)
            if not row.startswith("#-----") and not row.startswith("## Finished "):
                previous = row

            # if i > len(lines) - 1:
            #     break
            #exit(9)


    def _process_tsv(self, file_name):
        header = None
        # previous = None
        print(file_name)
        with open(file_name) as f:
            rows = csv.reader(f, delimiter="\t", quotechar='"')
            l=0
            for row in rows:
                # strip() added to handle "blank" row in TSVs
                if not row or not row[0].strip():
                    continue

                if not row[0].startswith("#"):
                    if header is None:
                        header = [previous[0].lstrip("#"), *previous[1:]]
                        print(header)
                        self._set_header(header)
                    self._add_row(row)
                    l = l+1
                if not row[0].startswith("#-----"):
                    previous = row
                # if l == 1:
                #print(row)
                # 	return


    def __process_gziped_tsv_files(self, directory):
        for filename in os.listdir(directory):
            if filename.endswith('.tsv.gz'):
                file_path = os.path.join(directory, filename)
                try:
                    self._proces_input_tsv(file_path)
                    print(f"\nFile: {filename}")
                    #print("Header:\n", self.__header)
                    #print("Rows:\n", self.__rows)
                    print(self.to_pandas_dataframe())
                    self.__rows = []
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")


    def process_tsv_files(self, directory):
        for filename in os.listdir(directory):
            if filename.endswith('.tsv'):
                file_path = os.path.join(directory, filename)
                try:
                    self._process_tsv(file_path)
                    print(f"File: {filename}")
                    # print("Header:\n", self.__header)
                    # print("Rows:\n", self.__rows)
                    print()
                    self.__rows = []
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")



    def test(self):
        dir_path = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/full/flybase"
        dir_path = "/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/flybase"
        file_names = [os.path.join(dir_path, file) for file in os.listdir(dir_path) if
                      os.path.isfile(os.path.join(dir_path, file))]

        for file in file_names:
            print(file)
            FlybasePrecomputedTable(file)

        # Set the directory containing the .tsv files
        #directory = '/home/saulo/snet/hyperon/das/das/flybase2metta/fb_data/2023_05'
        directory = '/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/flybase'

        #processor.process_tsv_files(directory)
        self.__process_gziped_tsv_files(directory)

        # Create an instance of FlybasePrecomputedTable and process .tsv files
        processor = FlybasePrecomputedTable('/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/flybase/gene_group_data_fb_2024_02.tsv.gz')
        processor._proces_input_tsv('/home/saulo/snet/hyperon/github/das-pk/shared_hsa_dmel2metta/data/toy/flybase/gene_group_data_fb_2024_02.tsv.gz')
        print(processor.to_pandas_dataframe())



