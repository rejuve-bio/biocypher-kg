import sys
import gzip

def extract_dmel_data(input_file_name, output_file_name):
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

def main():
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file.gz> <output_file>")
        sys.exit(1)

    input_file_name = sys.argv[1]
    output_file_name = sys.argv[2]

    extract_dmel_data(input_file_name, output_file_name)

if __name__ == "__main__":
    main()
