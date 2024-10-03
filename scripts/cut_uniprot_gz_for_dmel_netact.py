import sys
import gzip


expanded_genes_list = ["Su(var)205", "FBgn0003607", "Top3beta", "FBgn0026015", "Mef2", "FBgn0011656", "Clk", "FBgn0023076",
                       "Dref", "FBgn0015664", "TfIIB", "FBgn0004915", "Myc", "FBgn0262656", "AGO2", "FBgn0087035",
                       "Nipped-B", "FBgn0026401", "Cp190", "FBgn0000283", "TfIIA-L", "FBgn0011289", "Trl", "FBgn0013263",
                       "ash1", "FBgn0005386", "Raf", "FBgn0003079", "Abd-B", "FBgn0000015", "Orc2", "FBgn0286788",
                       "Rbf", "FBgn0015799", "mof", "FBgn0014340", "msl-1", "FBgn0005617", "Hmr", "FBgn0001206"]
def extract_dmel_data_from_uniprot_invert_dat(input_file_name, output_file_name):
    with gzip.open(input_file_name, 'rt') as input_file, open(output_file_name, 'w') as output_file:
        next_line = input_file.readline()
        while next_line:
            if next_line.startswith("ID") and "_DROME" in next_line:
                output_file.write(next_line)
                next_line = input_file.readline()
                while next_line and not next_line.startswith("ID"):
                    output_file.write(next_line)
                    next_line = input_file.readline()
            else:
                next_line = input_file.readline()

def netact_extract_dmel_data_from_uniprot_invert_dat(input_file_name, output_file_name, netact_genes_data):
    with gzip.open(input_file_name, 'rt') as input_file, open(output_file_name, 'w') as output_file:
        next_line = input_file.readline()
        data_lines = []
        netact = False
        while next_line:
            if next_line.startswith("ID") and "_DROME" in next_line:
                #output_file.write(next_line)
                data_lines.append(next_line)
                for netact_comp in netact_genes_data:
                    if netact_comp in next_line:        # include because any mention to a netact component
                        #print(next_line)
                        netact = True
                next_line = input_file.readline()
                while next_line and not next_line.startswith("ID"):
                    for netact_comp in netact_genes_data:
                        if netact_comp in next_line:  # include because any mention to a netact component
                            #print(next_line)
                            netact = True
                    data_lines.append(next_line)
                    next_line = input_file.readline()
                    if next_line and next_line.startswith("ID"):    # only write to file if found a netact component
                        if netact:
                            for line in data_lines:
                                output_file.write(line)
                        netact = False
                        data_lines = []
            else:
                next_line = input_file.readline()

def main():
    if len(sys.argv) < 4:
        print("Usage: python cut_uniprot_gz_for_dmel_netact.py <input_file.gz> <output_file> dmel\n"
              "or\nUsage: python cut_uniprot_gz_for_dmel_netact.py <input_file.gz> <output_file> netact" )
        sys.exit(1)

    input_file_name = sys.argv[1]
    output_file_name = sys.argv[2]

    if "dmel" in sys.argv[3]:
        extract_dmel_data_from_uniprot_invert_dat(input_file_name, output_file_name)
    elif "netact" in sys.argv[3]:
        netact_extract_dmel_data_from_uniprot_invert_dat(input_file_name, output_file_name, expanded_genes_list)


if __name__ == "__main__":
    main()
