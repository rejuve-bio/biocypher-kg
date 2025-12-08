# Author Abdulrahman S. Omar <xabush@singularitynet.io>
# Author Saulo A. P. Pinto <saulo@singularitynet.io> (dmel stuff)
import typer
import pathlib
import requests
from tqdm import tqdm
import shutil
import yaml
import os
from typing_extensions import Annotated
import gzip
from Bio import SeqIO
import pickle
import csv
import shutil
from pathlib import Path
from time import sleep


app = typer.Typer()

# def download(url, filepath):
#     r = requests.get(url, stream=True, allow_redirects=True)
#     if r.status_code != 200:
#         r.raise_for_status()
#         raise RuntimeError(f"Request to {url} returned status code {r.status_code}")

#     file_size = int(r.headers.get("Content-Length", 0))
#     desc = "(Unknown total file size)" if file_size == 0 else ""

#     with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
#         with filepath.open("wb") as f:
#             shutil.copyfileobj(r_raw, f)


def download(url, filepath: Path):
    r = requests.get(url, stream=True, allow_redirects=True)
    if r.status_code != 200:
        r.raise_for_status()
        # raise RuntimeError(f"Request to {url} returned status code {r.status_code}") 
        retries = 0
        while r.status_code != 200 and retries < 5:
            print('Retrying in 5 seconds...')
            retries += 1
            sleep(5)            
            r = requests.get(url, stream=True, allow_redirects=True)
        if retries == 5:
            raise RuntimeError(f"Request to {url} returned status code {r.status_code}") 

    file_size = int(r.headers.get("Content-Length", 0))
    progress_description = f"Downloading {filepath.name}"
    chunk_size = 8192
    with filepath.open("wb") as f:
        with tqdm(desc=progress_description, total=file_size if file_size else None, unit='B', unit_scale=True, leave=True) as pbar:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

# def download_flybase(output_dir, config):
#     print(f"Downloading from {config['name']} .....")
#     urls = config["url"]
#     save_dir = pathlib.Path(f"{output_dir}/flybase")
#     save_dir.mkdir(parents=True, exist_ok=True)
#     # p = save_dir.joinpath("gencode.annotation.gtf.gz")
#     for url in urls:        
#         filename = url.split("/")[-1]
#         p = save_dir.joinpath(filename)
#         try:
#             download(url, p)
#         except Exception as e:
#             print(f"Error downloading {url}: {e}")
#             continue


def download_flybase(output_dir, config):
    """
    Downloads all files specified in the config, retrying failed downloads
    until all files are successfully fetched or a maximum number of global attempts is reached.
    """
    print(f"Downloading from {config['name']} .....")
    
    # Create the target directory for Flybase files
    save_dir = pathlib.Path(f"{output_dir}/flybase")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare a list of (URL, FilePath) tuples for all files to be downloaded
    # We use a list of dictionaries to allow easy modification (removal)
    files_to_download_info = []
    for url_str in config["url"]:
        filename = url_str.split("/")[-1]
        filepath = save_dir.joinpath(filename)
        files_to_download_info.append({"url": url_str, "filepath": filepath})

    # Keep track of the initial total number of files
    total_files = len(files_to_download_info)
    if total_files == 0:
        print("No URLs provided to download.")
        return

    # Configuration for global retries
    max_global_attempts = 10  # Max times to iterate through all pending downloads
    global_retry_delay_sec = 15 # Delay between global attempts (e.g., after trying all pending files once)
    
    current_global_attempt = 0

    # Loop until all files are downloaded or global attempts are exhausted
    while files_to_download_info and current_global_attempt < max_global_attempts:
        current_global_attempt += 1
        print(f"\n--- Global Download Attempt {current_global_attempt}/{max_global_attempts} ---")
        print(f"Pending files: {len(files_to_download_info)}/{total_files}")

        # Create a list to hold files that still need to be downloaded in the next pass
        # This is necessary because we modify 'files_to_download_info' during iteration
        pending_in_this_pass = list(files_to_download_info) 
        
        # Clear the original list to rebuild it with only failed items
        files_to_download_info = []

        for file_info in pending_in_this_pass:
            url = file_info["url"]
            filepath = file_info["filepath"]
            
            if filepath.exists() and filepath.stat().st_size > 0:
                # If file already exists and is not empty, assume it was successfully downloaded previously
                print(f"Skipping {filepath.name}: already downloaded.")
                continue

            try:
                print(f"Attempting to download: {filepath.name} from {url}")
                download(url, filepath)
                print(f"✔️ Successfully downloaded: {filepath.name}")
            except Exception as e:
                print(f"❌ Error downloading {filepath.name} from {url}: {e}")
                # If download fails, add it back to the list for a retry in the next global attempt
                files_to_download_info.append(file_info)
        
        # If there are still files left to download and we haven't reached max attempts,
        # wait before the next global retry round.
        if files_to_download_info and current_global_attempt < max_global_attempts:
            print(f"\n{len(files_to_download_info)}/{total_files} files remaining. Retrying in {global_retry_delay_sec} seconds...")
            sleep(global_retry_delay_sec)
        elif files_to_download_info: # All attempts exhausted, but files remain
            print(f"\n⚠️ Warning: {len(files_to_download_info)}/{total_files} files could not be downloaded after {max_global_attempts} global attempts.")
            print("Remaining failed files:")
            for file_info in files_to_download_info:
                print(f"- {file_info['filepath'].name} from {file_info['url']}")
        else: # All files downloaded successfully
            print("\n🎉 All files downloaded successfully!")

def download_gencode(output_dir, config):
    print(f"Downloading from {config['name']} .....")
    urls = config["url"]
    save_dir = pathlib.Path(f"{output_dir}/gencode")
    save_dir.mkdir(parents=True, exist_ok=True)
    # p = save_dir.joinpath("gencode.annotation.gtf.gz")
    for url in urls:
        filename = url.split("/")[-1]
        p = save_dir.joinpath(filename)
        # r = requests.get(url, stream=True, allow_redirects=True,)
        # if r.status_code != 200:
        #     r.raise_for_status()
        #     raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
        # with p.open("w") as f:
        #     f.write(r.text)
        download(url, p)        

def download_uniprot(output_dir, config):
    print(f"Downloading from {config['name']} .....")
    url = config["url"]
    save_dir = pathlib.Path(f"{output_dir}/uniprot")
    save_dir.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1]
    p = save_dir.joinpath(filename)
    download(url, p)
    return p

def add_dmel_to_filename(filepath: str) -> str:
    """
    Given a path ending in .dat.gz, insert '_DMEL' before the .dat.gz extension.
    """
    # split off the .gz
    base, gz_ext = os.path.splitext(filepath)   # base ends with '.dat', gz_ext == '.gz'
    # split off the .dat
    name, dat_ext = os.path.splitext(base)      # name is without '.dat', dat_ext == '.dat'
    # assemble new filename
    return f"{name}_DMEL{dat_ext}{gz_ext}"

def save_uniprot_dmel_data(input_file_name, output_file_name=""):
    if not output_file_name:
        output_file_name = add_dmel_to_filename(input_file_name)
    with gzip.open(input_file_name, 'rt') as input_file, \
         gzip.open(output_file_name, 'wt') as output_file:
        for line in input_file:
            if line.startswith("ID") and "_DROME" in line:
                output_file.write(line)
                for line in input_file:
                    if line.startswith("ID"):
                        break
                    output_file.write(line)
    return output_file_name


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
                        uniprot_id = record.id
                        if ensembl_id:
                            ensembl_uniprot_ids[ensembl_id] = uniprot_id
                    except:
                        print(f'fail to process record: {record.name}')
    with open(ensembl_to_uniprot_output, 'wb') as pickle_file:
        pickle.dump(ensembl_uniprot_ids, pickle_file)


def download_reactome(output_dir, config):
    print(f"Downloading from {config['name']} .....")
    urls = config["url"]
    save_dir = pathlib.Path(f"{output_dir}/reactome")
    save_dir.mkdir(parents=True, exist_ok=True)
    for url in urls:
        filename = url.split("/")[-1]
        p = save_dir.joinpath(filename)
        r = requests.get(url, stream=True, allow_redirects=True,)
        if r.status_code != 200:
            r.raise_for_status()
            raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
        with p.open("w") as f:
            f.write(r.text)

def download_tflink_and_gencode(output_dir, tflink_config, gencode_config):
    download_gencode(output_dir, gencode_config)
    print(f"Downloading from {tflink_config['name']} .....")
    url = tflink_config["url"]
    # filename = "tflink_homo_sapiens_interactions.tsv.gz" # TFLink_Drosophila_melanogaster_interactions_All_simpleFormat_v1.0.tsv.gz
    filename = "TFLink_Drosophila_melanogaster_interactions_All_simpleFormat_v1.0.tsv.gz"
    save_dir = pathlib.Path(f"{output_dir}/tflink")
    save_dir.mkdir(parents=True, exist_ok=True)
    p = save_dir.joinpath(filename)
    download(url, p)
    save_dir = pathlib.Path(f"{output_dir}/gencode")
    move_gene_info_file(save_dir)
    save_gene_dbxrefs('aux_files/dmel/Drosophila_melanogaster.gene_info.gz', 'aux_files/dmel/dmel_entrez_to_ensembl.pkl')


def move_gene_info_file(output_path: Path):
    # source file path
    src = output_path / "Drosophila_melanogaster.gene_info.gz"
    # destination directory
    dest_dir = Path("aux_files/dmel")
    # ensure the directory exists
    dest_dir.mkdir(parents=True, exist_ok=True)
    # full destination file path
    dest = dest_dir / src.name
    shutil.move(src, dest)
    print(f"File moved from {src} to {dest}")


def save_gene_dbxrefs(gz_tsv_filename, pickle_filename):
    '''
    To create a (pickled) dictionary mapping Entrez ids to Ensembl (Flybase) ids. 
    Useful for TFLinkAdapter class.
    '''
    gene_dbxrefs = {}
    with gzip.open(gz_tsv_filename, 'rt') as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter='\t')
        for row in reader:
            gene_id = row['GeneID']
            dbxrefs = row['dbXrefs'].split('|')
            flybase_id = next((xref.split(':')[1] for xref in dbxrefs if xref.startswith('FLYBASE')), None)
            if flybase_id:
                gene_dbxrefs[gene_id] = flybase_id
    with open(pickle_filename, 'wb') as pickle_file:
        pickle.dump(gene_dbxrefs, pickle_file)

        
def download_string(output_dir, config):
    print(f"Downloading from {config['name']} .....")
    url = config["url"]
    # filename = "string_human_ppi_v12.0.txt.gz"  #7227.protein.links.v12.0.txt.gz
    filename = "7227.protein.links.v12.0.txt.gz"
    save_dir = pathlib.Path(f"{output_dir}/string")
    save_dir.mkdir(parents=True, exist_ok=True)
    p = save_dir.joinpath(filename)
    download(url, p)
    return p


@app.command()
def download_data(output_dir: Annotated[pathlib.Path, typer.Option(exists=False, file_okay=False, dir_okay=True)],
                  chr: str = None):
    """
    Download all the source data for biocypher-metta import
    """
    with open("config/dmel/dmel_data_source_config.yaml", "r") as f:
        try:
            config = yaml.safe_load(f)
            pathlib.Path(output_dir).mkdir(exist_ok=True, parents=True)
            download_flybase(output_dir, config["flybase"])
            # download_gencode(output_dir, config["gencode"])       # this must be called with tflink because tflink needs Drosophila_melanogaster.gene_info.gz that is downloaded with gencode data
            
            # uniprot_data_filename = download_uniprot(output_dir, config["uniprot"])
            # dmel_uniprot_data_filename = save_uniprot_dmel_data(uniprot_data_filename)
            # create_ensembl_to_uniprot_dict(dmel_uniprot_data_filename, 'aux_files/dmel/string_ensembl_uniprot_map.pkl')
            # download_reactome(output_dir, config["reactome"])

            # download_tflink_and_gencode(output_dir, config["tflink"], config["gencode"])            
            # download_string(output_dir, config["string"])

        except yaml.YAMLError as exc:
            print(f"Error parsing config file: {exc}")

if __name__ == "__main__":
    app()