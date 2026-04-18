# Author Abdulrahman S. Omar <xabush@singularitynet.io>
# Author Saulo A. P. Pinto <saulo@singularitynet.io> (dmel stuff)
import yaml
import requests
import typer
import logging
import csv
import gzip
import zipfile
import pickle
import shutil
from pathlib import Path
from tqdm import tqdm
from time import sleep
from urllib.parse import urlparse, urljoin
from html.parser import HTMLParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = typer.Typer()


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_with_retry(url, output_path, max_retries=3):
    """Download a file with retry logic and a progress bar."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))

            with open(output_path, 'wb') as f, tqdm(
                desc=output_path.name,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))
            return True
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
    return False


def already_downloaded(output_path, url):
    """Return True if a local file exists and appears to match the remote file.
    Uses content-length for comparison when available; falls back to trusting
    the local file if the server doesn't provide it or the HEAD request fails."""
    if not (output_path.exists() and output_path.stat().st_size > 0):
        return False
    try:
        response = requests.head(url, timeout=10)
        remote_size = int(response.headers.get('content-length', 0))
        # remote_size == 0 means the server didn't send content-length — trust local file
        return remote_size == 0 or output_path.stat().st_size == remote_size
    except requests.RequestException:
        # HEAD request failed — trust the local file
        return True


def extract_compressed(file_path):
    """Decompress a .gz or .zip file in place."""
    if file_path.suffix == '.gz':
        extracted_path = file_path.with_suffix('')
        with gzip.open(file_path, 'rb') as f_in, open(extracted_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        file_path.unlink()
        logger.info(f"Extracted {file_path} to {extracted_path}")
    elif file_path.suffix == '.zip':
        extract_dir = file_path.parent
        with zipfile.ZipFile(file_path, 'r') as zf:
            zf.extractall(extract_dir)
        file_path.unlink()
        logger.info(f"Extracted {file_path} to {extract_dir}")


def compress_gzip(file_path):
    """Gzip a file in place, appending .gz to its name."""
    compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
    with open(file_path, 'rb') as f_in, gzip.open(compressed_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    file_path.unlink()
    logger.info(f"Compressed {file_path} to {compressed_path}")


def parse_url_comment(url_str):
    """Split 'url # comment' into (url, comment). Comment may be None."""
    if '#' in url_str:
        url, comment = url_str.split('#', 1)
        return url.strip(), comment.strip()
    return url_str.strip(), None


def should_skip_extract(comment):
    """Return True if the inline URL comment requests keeping the file compressed."""
    return bool(comment and (
        'no extract' in comment.lower() or 'keep gzipped' in comment.lower()
    ))


# ---------------------------------------------------------------------------
# Directory scraping
# ---------------------------------------------------------------------------

class _LinkParser(HTMLParser):
    """Minimal HTML parser that collects href values from anchor tags."""
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href' and value:
                    self.links.append(value)


def scrape_directory(dir_url, output_dir, max_retries=3):
    """Fetch an HTML directory listing and download every file linked from it.
    Subdirectory links (trailing slash) are skipped.
    Files are always kept as-is — no decompression.
    Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs."""
    for attempt in range(max_retries):
        try:
            response = requests.get(dir_url, timeout=30)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed fetching directory {dir_url}: {e}")
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
    else:
        logger.error(f"Could not fetch directory listing: {dir_url}")
        return 0, 0, [dir_url]

    parser = _LinkParser()
    parser.feed(response.text)

    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded = skipped = 0
    failed_urls = []

    for href in parser.links:
        # Skip parent dir links, anchors, absolute paths, and subdirectories
        if not href or href.startswith(('?', '#', '/')) or href in ('..', './') or href.endswith('/'):
            continue

        file_url = urljoin(dir_url, href)
        filename = Path(urlparse(file_url).path).name
        if not filename:
            continue

        output_path = output_dir / filename

        if already_downloaded(output_path, file_url):
            logger.info(f"Skipped {file_url} (already exists)")
            skipped += 1
            continue

        if download_with_retry(file_url, output_path):
            logger.info(f"Downloaded {file_url}")
            downloaded += 1
        else:
            logger.error(f"Failed to download {file_url}")
            failed_urls.append(file_url)

    return downloaded, skipped, failed_urls


# ---------------------------------------------------------------------------
# URL processing
# ---------------------------------------------------------------------------

def is_filename_url_dict(d):
    """Return True if a dict uses {filename: url} pairs instead of {sub_key: url_list}."""
    return isinstance(d, dict) and all(
        isinstance(v, str) and v.startswith('http')
        for v in d.values()
    )


def download_one(url, output_path, extract, compress=None):
    """Download a single URL to output_path, optionally decompressing or compressing it.
    compress: None (no compression) or 'gzip' (gzip after download).
    Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs."""
    url, comment = parse_url_comment(url)

    # A directory at the output path means a previous run misidentified the
    # filename as a sub_key and called mkdir on it — remove it so we can write the file.
    if output_path.is_dir():
        logger.warning(f"Removing unexpected directory at {output_path} (will be replaced by file)")
        shutil.rmtree(output_path)

    if already_downloaded(output_path, url):
        logger.info(f"Skipped {url} (already exists)")
        return 0, 1, []

    if download_with_retry(url, output_path):
        if extract and not should_skip_extract(comment):
            extract_compressed(output_path)
        elif compress == 'gzip':
            compress_gzip(output_path)
        logger.info(f"Downloaded {url}")
        return 1, 0, []
    else:
        logger.error(f"Failed to download {url}")
        return 0, 0, [url]


def process_urls(urls, output_dir, source_key, sub_key=None, extract=True, compress=None):
    """Recursively process URLs from a YAML source entry.

    Supported url formats:
      - str:                  single URL, filename taken from URL path
      - list[str]:            multiple URLs, filenames taken from URL paths
      - dict{filename: url}:  explicit output filenames mapped to their URLs
      - dict{sub_key: urls}:  recurse into named subdirectories

    Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs.
    """
    downloaded = skipped = 0
    failed_urls = []
    sub_dir = output_dir / source_key / sub_key if sub_key else output_dir / source_key

    if isinstance(urls, str):
        # Single URL — filename from URL path
        sub_dir.mkdir(parents=True, exist_ok=True)
        filename = Path(urlparse(urls.split('#')[0].strip()).path).name
        return download_one(urls, sub_dir / filename, extract, compress)

    if isinstance(urls, dict):
        if is_filename_url_dict(urls):
            # {filename: url} — use the dict key as the output filename
            sub_dir.mkdir(parents=True, exist_ok=True)
            for filename, url in urls.items():
                d, s, f = download_one(url, sub_dir / filename, extract, compress)
                downloaded += d; skipped += s; failed_urls += f
        else:
            # {sub_key: url_list} — recurse into named subdirectories
            for sub, sub_urls in urls.items():
                d, s, f = process_urls(sub_urls, output_dir, source_key, sub, extract=extract, compress=compress)
                downloaded += d; skipped += s; failed_urls += f
        return downloaded, skipped, failed_urls

    # List of URLs — filename from each URL path
    sub_dir.mkdir(parents=True, exist_ok=True)
    for url in urls:
        url_clean = url.split('#')[0].strip()
        filename = Path(urlparse(url_clean).path).name
        d, s, f = download_one(url, sub_dir / filename, extract, compress)
        downloaded += d; skipped += s; failed_urls += f

    return downloaded, skipped, failed_urls


# ---------------------------------------------------------------------------
# Dmel-specific post-processing
# ---------------------------------------------------------------------------

def add_dmel_to_filename(filepath: str) -> str:
    """Insert '_DMEL' before the .dat.gz extension of a UniProt file."""
    base, gz_ext = Path(filepath).stem, Path(filepath).suffix   # e.g. .gz
    name, dat_ext = Path(base).stem, Path(base).suffix           # e.g. .dat
    return str(Path(filepath).parent / f"{name}_DMEL{dat_ext}{gz_ext}")


def save_uniprot_dmel_data(input_file_name, output_file_name=""):
    """Extract only Drosophila melanogaster (_DROME) records from a UniProt .dat.gz file."""
    if not output_file_name:
        output_file_name = add_dmel_to_filename(str(input_file_name))
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
    """Build and pickle a dict mapping STRING/Ensembl IDs to UniProt accessions."""
    from Bio import SeqIO
    ensembl_uniprot_ids = {}
    with gzip.open(input_uniprot, 'rt') as input_file:
        records = SeqIO.parse(input_file, 'swiss')
        for record in records:
            for item in record.dbxrefs:
                if item.startswith('STRING'):
                    try:
                        ensembl_id = item.split(':')[-1].split('.')[1]
                        if ensembl_id:
                            ensembl_uniprot_ids[ensembl_id] = record.id
                    except Exception:
                        logger.warning(f"Failed to process record: {record.name}")
    with open(ensembl_to_uniprot_output, 'wb') as pickle_file:
        pickle.dump(ensembl_uniprot_ids, pickle_file)


def move_gene_info_file(output_path: Path):
    """Move the Drosophila gene_info.gz file to the aux_files/dmel directory."""
    src = output_path / "Drosophila_melanogaster.gene_info.gz"
    dest_dir = Path("aux_files/dmel")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.move(src, dest)
    logger.info(f"Moved {src} to {dest}")


def save_gene_dbxrefs(gz_tsv_filename, pickle_filename):
    """Build and pickle a dict mapping Entrez IDs to Flybase/Ensembl IDs.
    Used by TFLinkAdapter."""
    gene_dbxrefs = {}
    with gzip.open(gz_tsv_filename, 'rt') as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter='\t')
        for row in reader:
            gene_id = row['GeneID']
            dbxrefs = row['dbXrefs'].split('|')
            flybase_id = next(
                (xref.split(':')[1] for xref in dbxrefs if xref.startswith('FLYBASE')), None
            )
            if flybase_id:
                gene_dbxrefs[gene_id] = flybase_id
    with open(pickle_filename, 'wb') as pickle_file:
        pickle.dump(gene_dbxrefs, pickle_file)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@app.command()
def download_data(output_dir: str = typer.Option("./data", "--output-dir", help="Output directory for downloads")):
    """Download all source data for dmel biocypher-metta import."""
    config_path = Path("config/dmel/dmel_data_source_config.yaml")
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        raise typer.Exit(1)

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    if not config:
        logger.error("Invalid config: empty or malformed")
        raise typer.Exit(1)

    output_dir = Path(output_dir)
    total_downloaded = total_skipped = 0
    all_failed_urls = []

    for source_key, source_data in config.items():
        if not isinstance(source_data, dict):
            logger.warning(f"Unexpected format for source '{source_key}' — skipping")
            continue

        extract = source_data.get('extract', True)
        compress = source_data.get('compress', None)
        has_handled = False

        # Handle 'url' key
        if 'url' in source_data:
            d, s, f = process_urls(source_data['url'], output_dir, source_key, extract=extract, compress=compress)
            total_downloaded += d; total_skipped += s; all_failed_urls += f
            has_handled = True

        # Handle 'directories' key — scrape each HTML directory listing
        if 'directories' in source_data:
            for dir_name, dir_url in source_data['directories'].items():
                dir_output = output_dir / source_key / dir_name
                d, s, f = scrape_directory(dir_url, dir_output)
                total_downloaded += d; total_skipped += s; all_failed_urls += f
            has_handled = True

        if not has_handled:
            logger.warning(f"No 'url' or 'directories' key found for source '{source_key}' — skipping")

    logger.info(
        f"Download complete: {total_downloaded} downloaded, "
        f"{total_skipped} skipped, {len(all_failed_urls)} failed"
    )
    if all_failed_urls:
        logger.warning("Failed URLs:")
        for url in all_failed_urls:
            logger.warning(f"  {url}")

    # -----------------------------------------------------------------------
    # Dmel-specific post-processing (runs after all downloads complete)
    # -----------------------------------------------------------------------
    logger.info("Running dmel post-processing steps...")

    # Extract Drosophila-only records from the full invertebrates UniProt file
    uniprot_path = output_dir / "uniprot" / "uniprot_sprot_invertebrates.dat.gz"
    if uniprot_path.exists():
        dmel_uniprot_path = save_uniprot_dmel_data(uniprot_path)
        logger.info(f"Dmel UniProt data saved to {dmel_uniprot_path}")
        create_ensembl_to_uniprot_dict(dmel_uniprot_path, 'aux_files/dmel/string_ensembl_uniprot_map.pkl')
        logger.info("Ensembl-to-UniProt mapping saved to aux_files/dmel/string_ensembl_uniprot_map.pkl")
    else:
        logger.warning(f"UniProt file not found, skipping dmel extraction: {uniprot_path}")

    # Move gene_info file to aux dir and build Entrez→Flybase mapping for TFLink
    gene_info_path = output_dir / "gencode" / "Drosophila_melanogaster.gene_info.gz"
    if gene_info_path.exists():
        move_gene_info_file(output_dir / "gencode")
        save_gene_dbxrefs(
            'aux_files/dmel/Drosophila_melanogaster.gene_info.gz',
            'aux_files/dmel/dmel_entrez_to_ensembl.pkl'
        )
        logger.info("Entrez-to-Ensembl mapping saved to aux_files/dmel/dmel_entrez_to_ensembl.pkl")
    else:
        logger.warning(f"gene_info file not found, skipping TFLink aux build: {gene_info_path}")

    logger.info("Post-processing complete.")


if __name__ == "__main__":
    app()


# # Author Abdulrahman S. Omar <xabush@singularitynet.io>
# # Author Saulo A. P. Pinto <saulo@singularitynet.io> (dmel stuff)
# import typer
# import pathlib
# import requests
# from tqdm import tqdm
# import shutil
# import yaml
# import os
# from typing_extensions import Annotated
# import gzip
# from Bio import SeqIO
# import pickle
# import csv
# import shutil
# from pathlib import Path
# from time import sleep


# app = typer.Typer()

# # def download(url, filepath):
# #     r = requests.get(url, stream=True, allow_redirects=True)
# #     if r.status_code != 200:
# #         r.raise_for_status()
# #         raise RuntimeError(f"Request to {url} returned status code {r.status_code}")

# #     file_size = int(r.headers.get("Content-Length", 0))
# #     desc = "(Unknown total file size)" if file_size == 0 else ""

# #     with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
# #         with filepath.open("wb") as f:
# #             shutil.copyfileobj(r_raw, f)


# def download(url, filepath: Path):
#     r = requests.get(url, stream=True, allow_redirects=True)
#     if r.status_code != 200:
#         r.raise_for_status()
#         # raise RuntimeError(f"Request to {url} returned status code {r.status_code}") 
#         retries = 0
#         while r.status_code != 200 and retries < 5:
#             print('Retrying in 5 seconds...')
#             retries += 1
#             sleep(5)            
#             r = requests.get(url, stream=True, allow_redirects=True)
#         if retries == 5:
#             raise RuntimeError(f"Request to {url} returned status code {r.status_code}") 

#     file_size = int(r.headers.get("Content-Length", 0))
#     progress_description = f"Downloading {filepath.name}"
#     chunk_size = 8192
#     with filepath.open("wb") as f:
#         with tqdm(desc=progress_description, total=file_size if file_size else None, unit='B', unit_scale=True, leave=True) as pbar:
#             for chunk in r.iter_content(chunk_size=chunk_size):
#                 if chunk:
#                     f.write(chunk)
#                     pbar.update(len(chunk))

# # def download_flybase(output_dir, config):
# #     print(f"Downloading from {config['name']} .....")
# #     urls = config["url"]
# #     save_dir = pathlib.Path(f"{output_dir}/flybase")
# #     save_dir.mkdir(parents=True, exist_ok=True)
# #     # p = save_dir.joinpath("gencode.annotation.gtf.gz")
# #     for url in urls:        
# #         filename = url.split("/")[-1]
# #         p = save_dir.joinpath(filename)
# #         try:
# #             download(url, p)
# #         except Exception as e:
# #             print(f"Error downloading {url}: {e}")
# #             continue


# def download_flybase(output_dir, config):
#     """
#     Downloads all files specified in the config, retrying failed downloads
#     until all files are successfully fetched or a maximum number of global attempts is reached.
#     """
#     print(f"Downloading from {config['name']} .....")
    
#     # Create the target directory for Flybase files
#     save_dir = pathlib.Path(f"{output_dir}/flybase")
#     save_dir.mkdir(parents=True, exist_ok=True)
    
#     # Prepare a list of (URL, FilePath) tuples for all files to be downloaded
#     # We use a list of dictionaries to allow easy modification (removal)
#     files_to_download_info = []
#     for url_str in config["url"]:
#         filename = url_str.split("/")[-1]
#         filepath = save_dir.joinpath(filename)
#         files_to_download_info.append({"url": url_str, "filepath": filepath})

#     # Keep track of the initial total number of files
#     total_files = len(files_to_download_info)
#     if total_files == 0:
#         print("No URLs provided to download.")
#         return

#     # Configuration for global retries
#     max_global_attempts = 10  # Max times to iterate through all pending downloads
#     global_retry_delay_sec = 15 # Delay between global attempts (e.g., after trying all pending files once)
    
#     current_global_attempt = 0

#     # Loop until all files are downloaded or global attempts are exhausted
#     while files_to_download_info and current_global_attempt < max_global_attempts:
#         current_global_attempt += 1
#         print(f"\n--- Global Download Attempt {current_global_attempt}/{max_global_attempts} ---")
#         print(f"Pending files: {len(files_to_download_info)}/{total_files}")

#         # Create a list to hold files that still need to be downloaded in the next pass
#         # This is necessary because we modify 'files_to_download_info' during iteration
#         pending_in_this_pass = list(files_to_download_info) 
        
#         # Clear the original list to rebuild it with only failed items
#         files_to_download_info = []

#         for file_info in pending_in_this_pass:
#             url = file_info["url"]
#             filepath = file_info["filepath"]
            
#             if filepath.exists() and filepath.stat().st_size > 0:
#                 # If file already exists and is not empty, assume it was successfully downloaded previously
#                 print(f"Skipping {filepath.name}: already downloaded.")
#                 continue

#             try:
#                 print(f"Attempting to download: {filepath.name} from {url}")
#                 download(url, filepath)
#                 print(f"✔️ Successfully downloaded: {filepath.name}")
#             except Exception as e:
#                 print(f"❌ Error downloading {filepath.name} from {url}: {e}")
#                 # If download fails, add it back to the list for a retry in the next global attempt
#                 files_to_download_info.append(file_info)
        
#         # If there are still files left to download and we haven't reached max attempts,
#         # wait before the next global retry round.
#         if files_to_download_info and current_global_attempt < max_global_attempts:
#             print(f"\n{len(files_to_download_info)}/{total_files} files remaining. Retrying in {global_retry_delay_sec} seconds...")
#             sleep(global_retry_delay_sec)
#         elif files_to_download_info: # All attempts exhausted, but files remain
#             print(f"\n⚠️ Warning: {len(files_to_download_info)}/{total_files} files could not be downloaded after {max_global_attempts} global attempts.")
#             print("Remaining failed files:")
#             for file_info in files_to_download_info:
#                 print(f"- {file_info['filepath'].name} from {file_info['url']}")
#         else: # All files downloaded successfully
#             print("\n🎉 All files downloaded successfully!")

# def download_gencode(output_dir, config):
#     print(f"Downloading from {config['name']} .....")
#     urls = config["url"]
#     save_dir = pathlib.Path(f"{output_dir}/gencode")
#     save_dir.mkdir(parents=True, exist_ok=True)
#     # p = save_dir.joinpath("gencode.annotation.gtf.gz")
#     for url in urls:
#         filename = url.split("/")[-1]
#         p = save_dir.joinpath(filename)
#         # r = requests.get(url, stream=True, allow_redirects=True,)
#         # if r.status_code != 200:
#         #     r.raise_for_status()
#         #     raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
#         # with p.open("w") as f:
#         #     f.write(r.text)
#         download(url, p)        

# def download_uniprot(output_dir, config):
#     print(f"Downloading from {config['name']} .....")
#     url = config["url"]
#     save_dir = pathlib.Path(f"{output_dir}/uniprot")
#     save_dir.mkdir(parents=True, exist_ok=True)
#     filename = url.split("/")[-1]
#     p = save_dir.joinpath(filename)
#     download(url, p)
#     return p

# def add_dmel_to_filename(filepath: str) -> str:
#     """
#     Given a path ending in .dat.gz, insert '_DMEL' before the .dat.gz extension.
#     """
#     # split off the .gz
#     base, gz_ext = os.path.splitext(filepath)   # base ends with '.dat', gz_ext == '.gz'
#     # split off the .dat
#     name, dat_ext = os.path.splitext(base)      # name is without '.dat', dat_ext == '.dat'
#     # assemble new filename
#     return f"{name}_DMEL{dat_ext}{gz_ext}"

# def save_uniprot_dmel_data(input_file_name, output_file_name=""):
#     if not output_file_name:
#         output_file_name = add_dmel_to_filename(input_file_name)
#     with gzip.open(input_file_name, 'rt') as input_file, \
#          gzip.open(output_file_name, 'wt') as output_file:
#         for line in input_file:
#             if line.startswith("ID") and "_DROME" in line:
#                 output_file.write(line)
#                 for line in input_file:
#                     if line.startswith("ID"):
#                         break
#                     output_file.write(line)
#     return output_file_name


# def create_ensembl_to_uniprot_dict(input_uniprot, ensembl_to_uniprot_output):
#     ensembl_uniprot_ids = {}
#     with gzip.open(input_uniprot, 'rt') as input_file:
#         records = SeqIO.parse(input_file, 'swiss')
#         for record in records:
#             dbxrefs = record.dbxrefs
#             for item in dbxrefs:
#                 if item.startswith('STRING'):            
#                     try:
#                         ensembl_id = item.split(':')[-1].split('.')[1]
#                         uniprot_id = record.id
#                         if ensembl_id:
#                             ensembl_uniprot_ids[ensembl_id] = uniprot_id
#                     except:
#                         print(f'fail to process record: {record.name}')
#     with open(ensembl_to_uniprot_output, 'wb') as pickle_file:
#         pickle.dump(ensembl_uniprot_ids, pickle_file)


# def download_reactome(output_dir, config):
#     print(f"Downloading from {config['name']} .....")
#     urls = config["url"]
#     save_dir = pathlib.Path(f"{output_dir}/reactome")
#     save_dir.mkdir(parents=True, exist_ok=True)
#     for url in urls:
#         filename = url.split("/")[-1]
#         p = save_dir.joinpath(filename)
#         r = requests.get(url, stream=True, allow_redirects=True,)
#         if r.status_code != 200:
#             r.raise_for_status()
#             raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
#         with p.open("w") as f:
#             f.write(r.text)

# def download_tflink_and_gencode(output_dir, tflink_config, gencode_config):
#     download_gencode(output_dir, gencode_config)
#     print(f"Downloading from {tflink_config['name']} .....")
#     url = tflink_config["url"]
#     # filename = "tflink_homo_sapiens_interactions.tsv.gz" # TFLink_Drosophila_melanogaster_interactions_All_simpleFormat_v1.0.tsv.gz
#     filename = "TFLink_Drosophila_melanogaster_interactions_All_simpleFormat_v1.0.tsv.gz"
#     save_dir = pathlib.Path(f"{output_dir}/tflink")
#     save_dir.mkdir(parents=True, exist_ok=True)
#     p = save_dir.joinpath(filename)
#     download(url, p)
#     save_dir = pathlib.Path(f"{output_dir}/gencode")
#     move_gene_info_file(save_dir)
#     save_gene_dbxrefs('aux_files/dmel/Drosophila_melanogaster.gene_info.gz', 'aux_files/dmel/dmel_entrez_to_ensembl.pkl')


# def move_gene_info_file(output_path: Path):
#     # source file path
#     src = output_path / "Drosophila_melanogaster.gene_info.gz"
#     # destination directory
#     dest_dir = Path("aux_files/dmel")
#     # ensure the directory exists
#     dest_dir.mkdir(parents=True, exist_ok=True)
#     # full destination file path
#     dest = dest_dir / src.name
#     shutil.move(src, dest)
#     print(f"File moved from {src} to {dest}")


# def save_gene_dbxrefs(gz_tsv_filename, pickle_filename):
#     '''
#     To create a (pickled) dictionary mapping Entrez ids to Ensembl (Flybase) ids. 
#     Useful for TFLinkAdapter class.
#     '''
#     gene_dbxrefs = {}
#     with gzip.open(gz_tsv_filename, 'rt') as tsv_file:
#         reader = csv.DictReader(tsv_file, delimiter='\t')
#         for row in reader:
#             gene_id = row['GeneID']
#             dbxrefs = row['dbXrefs'].split('|')
#             flybase_id = next((xref.split(':')[1] for xref in dbxrefs if xref.startswith('FLYBASE')), None)
#             if flybase_id:
#                 gene_dbxrefs[gene_id] = flybase_id
#     with open(pickle_filename, 'wb') as pickle_file:
#         pickle.dump(gene_dbxrefs, pickle_file)

        
# def download_string(output_dir, config):
#     print(f"Downloading from {config['name']} .....")
#     url = config["url"]
#     # filename = "string_human_ppi_v12.0.txt.gz"  #7227.protein.links.v12.0.txt.gz
#     filename = "7227.protein.links.v12.0.txt.gz"
#     save_dir = pathlib.Path(f"{output_dir}/string")
#     save_dir.mkdir(parents=True, exist_ok=True)
#     p = save_dir.joinpath(filename)
#     download(url, p)
#     return p


# @app.command()
# def download_data(output_dir: Annotated[pathlib.Path, typer.Option(exists=False, file_okay=False, dir_okay=True)],
#                   chr: str = None):
#     """
#     Download all the source data for biocypher-metta import
#     """
#     with open("config/dmel/dmel_data_source_config.yaml", "r") as f:
#         try:
#             config = yaml.safe_load(f)
#             pathlib.Path(output_dir).mkdir(exist_ok=True, parents=True)
#             download_flybase(output_dir, config["flybase"])
#             uniprot_data_filename = download_uniprot(output_dir, config["uniprot"])
#             dmel_uniprot_data_filename = save_uniprot_dmel_data(uniprot_data_filename)
#             create_ensembl_to_uniprot_dict(dmel_uniprot_data_filename, 'aux_files/dmel/string_ensembl_uniprot_map.pkl')
#             download_reactome(output_dir, config["reactome"])
#             download_tflink_and_gencode(output_dir, config["tflink"], config["gencode"])            
#             download_string(output_dir, config["string"])
#             downlo

#         except yaml.YAMLError as exc:
#             print(f"Error parsing config file: {exc}")

# if __name__ == "__main__":
#     app()