import yaml
import requests
import typer
import logging
from pathlib import Path
from tqdm import tqdm
import io
import math
import time
import gzip
import zipfile
import shutil
from urllib.parse import urlparse, urljoin
from html.parser import HTMLParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = typer.Typer()


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_with_retry(url, output_path, max_retries=5):
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
                time.sleep(2 ** attempt)
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
# Sampling
# ---------------------------------------------------------------------------

DEFAULT_SAMPLE_FRACTION = 0.01  # 1% by default


def _open_text(file_path):
    """Open a file for text reading, transparently decompressing .gz."""
    if file_path.suffix == '.gz':
        return gzip.open(file_path, 'rt', errors='replace')
    return open(file_path, 'r', errors='replace')


def create_sample(file_path, output_dir, sample_root, fraction=DEFAULT_SAMPLE_FRACTION):
    """Write a line-based sample of file_path to the mirrored path under sample_root.

    The sample has the exact same filename and compression as the original:
      - .gz  → sample is also gzipped
      - plain text → sample is plain text

    Args:
        file_path:   Path to the fully post-processed file.
        output_dir:  Root output directory (used to compute relative path).
        sample_root: Root directory for samples (output_dir / 'sample').
        fraction:    Fraction of lines to sample (default 1%).
    """
    try:
        rel_path = file_path.relative_to(output_dir)
    except ValueError:
        logger.warning(f"Cannot compute relative path for sample: {file_path}")
        return

    sample_path = sample_root / rel_path
    if sample_path.exists() and sample_path.stat().st_size > 0:
        logger.info(f"Sample already exists, skipping: {sample_path}")
        return

    sample_path.parent.mkdir(parents=True, exist_ok=True)

    # Count lines
    try:
        total_lines = sum(1 for _ in _open_text(file_path))
    except Exception as e:
        logger.warning(f"Skipping sample for {file_path.name}: cannot read as text ({e})")
        return

    if total_lines == 0:
        logger.warning(f"Skipping sample for {file_path.name}: file is empty")
        return

    n_sample = max(1, math.ceil(total_lines * fraction))

    try:
        buf = io.StringIO()
        with _open_text(file_path) as f_in:
            for i, line in enumerate(f_in):
                if i >= n_sample:
                    break
                buf.write(line)

        sample_bytes = buf.getvalue().encode()

        if file_path.suffix == '.gz':
            with gzip.open(sample_path, 'wb') as f_out:
                f_out.write(sample_bytes)
        else:
            with open(sample_path, 'wb') as f_out:
                f_out.write(sample_bytes)

        logger.info(f"Sample created: {sample_path} ({n_sample}/{total_lines} lines, {fraction*100:.1f}%)")

    except Exception as e:
        logger.warning(f"Failed to create sample for {file_path.name}: {e}")


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
                time.sleep(2 ** attempt)
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


def _post_processed_path(output_path, extract, compress, comment, move_to_dest):
    """Return the path where the file will end up after all post-processing.
    Used to skip re-downloading files that were already processed in a previous run."""
    p = output_path
    # After extraction: .gz/.zip → decompressed (only if file is actually compressed)
    if extract and not should_skip_extract(comment) and p.suffix in ('.gz', '.zip'):
        if p.suffix == '.gz':
            p = p.with_suffix('')
    # After compression: plain file → .gz appended
    elif compress == 'gzip' and p.suffix != '.gz':
        p = p.with_suffix(p.suffix + '.gz')
    # After move_to: file lands in a different directory
    if move_to_dest:
        p = move_to_dest / p.name
    return p


def download_one(url, output_path, extract, compress=None, move_to_dest=None,
                 output_dir=None, sample_root=None, sample_fraction=DEFAULT_SAMPLE_FRACTION):
    """Download a single URL to output_path, applying post-processing and optional move.

    Post-processing order:
      1. extract (decompress .gz / .zip) or compress (gzip plain file)
      2. move_to_dest: if provided, move the final file to this resolved Path
      3. create_sample: if sample_root provided, write a reduced sample copy

    compress: None (no compression) or 'gzip' (gzip after download).
    move_to_dest: resolved Path to move the file into after post-processing, or None.
    output_dir/sample_root: if both provided, a line sample is created after download.
    Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs."""
    url, comment = parse_url_comment(url)

    # A directory at the output path means a previous run misidentified the
    # filename as a sub_key and called mkdir on it — remove it so we can write the file.
    if output_path.is_dir():
        logger.warning(f"Removing unexpected directory at {output_path} (will be replaced by file)")
        shutil.rmtree(output_path)

    # Check the post-processed path first: if a previous run already extracted,
    # compressed, or moved the file, the original filename won't exist on disk.
    post_processed_path = _post_processed_path(output_path, extract, compress, comment, move_to_dest)
    if post_processed_path.exists() and post_processed_path.stat().st_size > 0:
        logger.info(f"Skipped {url} (already exists as {post_processed_path.name})")
        return 0, 1, []

    if already_downloaded(output_path, url):
        logger.info(f"Skipped {url} (already exists)")
        return 0, 1, []

    if not download_with_retry(url, output_path):
        logger.error(f"Failed to download {url}")
        return 0, 0, [url]

    # --- post-processing: extract or compress ---
    # Extract takes priority; compress only applies if extraction didn't happen
    # (i.e. file was not compressed to begin with, or extract=False).
    if extract and not should_skip_extract(comment) and output_path.suffix in ('.gz', '.zip'):
        extract_compressed(output_path)
        if output_path.suffix == '.gz':
            output_path = output_path.with_suffix('')
    elif compress == 'gzip' and output_path.suffix != '.gz':
        compress_gzip(output_path)
        output_path = output_path.with_suffix(output_path.suffix + '.gz')

    # --- post-processing: move_to ---
    if move_to_dest:
        move_to_dest.mkdir(parents=True, exist_ok=True)
        dest_path = move_to_dest / output_path.name
        shutil.move(str(output_path), str(dest_path))
        output_path = dest_path
        logger.info(f"Moved {output_path.name} to {dest_path}")

    # --- post-processing: sample ---
    # output_path is already the final path after all transformations above
    if output_dir and sample_root:
        if output_path.exists():
            create_sample(output_path, output_dir, sample_root, sample_fraction)
        else:
            logger.warning(f"Post-processed file not found for sampling: {output_path}")

    logger.info(f"Downloaded {url}")
    return 1, 0, []


def resolve_move_to(dest_str, output_dir):
    """Resolve a move_to destination relative to output_dir.

    Example:
        output_dir = /input/hsa/
        dest_str   = ../rna_central
        result     = /input/rna_central/
    """
    return (output_dir / dest_str).resolve()


def process_urls(urls, output_dir, source_key, sub_key=None, extract=True, compress=None,
                 move_to=None, sample_root=None, sample_fraction=DEFAULT_SAMPLE_FRACTION):
    """Recursively process URLs from a YAML source entry.

    Supported url formats:
      - str:                  single URL, filename taken from URL path
      - list[str]:            multiple URLs, filenames taken from URL paths
      - dict{filename: url}:  explicit output filenames mapped to their URLs
      - dict{sub_key: urls}:  recurse into named subdirectories

    move_to: optional dict of {filename: dest_path_str} from the YAML move_to key.
             Filenames are matched against the downloaded file's final name.

    Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs.
    """
    downloaded = skipped = 0
    failed_urls = []
    sub_dir = output_dir / source_key / sub_key if sub_key else output_dir / source_key
    move_to = move_to or {}

    def get_move_to_dest(filename):
        """Return resolved destination Path for filename, or None."""
        dest_str = move_to.get(filename)
        return resolve_move_to(dest_str, output_dir) if dest_str else None

    def dl(url, path, filename):
        return download_one(url, path, extract, compress, get_move_to_dest(filename),
                            output_dir, sample_root, sample_fraction)

    if isinstance(urls, str):
        # Single URL — filename from URL path
        sub_dir.mkdir(parents=True, exist_ok=True)
        filename = Path(urlparse(urls.split('#')[0].strip()).path).name
        return dl(urls, sub_dir / filename, filename)

    if isinstance(urls, dict):
        if is_filename_url_dict(urls):
            # {filename: url} — use the dict key as the output filename
            sub_dir.mkdir(parents=True, exist_ok=True)
            for filename, url in urls.items():
                d, s, f = dl(url, sub_dir / filename, filename)
                downloaded += d; skipped += s; failed_urls += f
        else:
            # {sub_key: url_list} — recurse into named subdirectories
            for sub, sub_urls in urls.items():
                d, s, f = process_urls(sub_urls, output_dir, source_key, sub,
                                       extract=extract, compress=compress, move_to=move_to,
                                       sample_root=sample_root, sample_fraction=sample_fraction)
                downloaded += d; skipped += s; failed_urls += f
        return downloaded, skipped, failed_urls

    # List of URLs — filename from each URL path
    sub_dir.mkdir(parents=True, exist_ok=True)
    for url in urls:
        url_clean = url.split('#')[0].strip()
        filename = Path(urlparse(url_clean).path).name
        d, s, f = dl(url, sub_dir / filename, filename)
        downloaded += d; skipped += s; failed_urls += f

    return downloaded, skipped, failed_urls


# ---------------------------------------------------------------------------
# zip_extract processing
# ---------------------------------------------------------------------------

def process_zip_extract(zip_extract_list, output_dir, source_key, compress=None,
                        sample_root=None, sample_fraction=DEFAULT_SAMPLE_FRACTION):
    """Download a zip, extract only the specified files from it, then delete the zip.
    Optionally gzip each extracted file if compress='gzip'.

    zip_extract_list: list of dicts with keys:
        url:   URL of the zip file
        files: list of filenames to extract from the zip

    Returns (downloaded, skipped, failed_urls).
    """
    downloaded, skipped, failed_urls = 0, 0, []
    sub_dir = output_dir / source_key
    sub_dir.mkdir(parents=True, exist_ok=True)

    for entry in zip_extract_list:
        zip_url = entry.get('url')
        target_files = entry.get('files', [])

        if not zip_url:
            logger.warning(f"zip_extract entry missing 'url' — skipping")
            continue

        # Check if all target files already exist (post-processed)
        all_exist = all(
            (sub_dir / (f + '.gz' if compress == 'gzip' else f)).exists()
            for f in target_files
        )
        if all_exist:
            logger.info(f"Skipped {zip_url} (all target files already exist)")
            skipped += len(target_files)
            continue

        # Download the zip to a temp path
        zip_filename = Path(urlparse(zip_url).path).name
        zip_path = sub_dir / zip_filename

        if not download_with_retry(zip_url, zip_path):
            logger.error(f"Failed to download {zip_url}")
            failed_urls.append(zip_url)
            continue

        # Extract only the requested files
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                available = zf.namelist()
                for target in target_files:
                    # Support files nested inside zip subdirectories
                    match = next((n for n in available if n.endswith(target)), None)
                    if not match:
                        logger.warning(f"File '{target}' not found in {zip_filename}. Available: {available}")
                        failed_urls.append(f"{zip_url}#{target}")
                        continue
                    # Extract to sub_dir, flattening any subdirectory structure
                    extracted_path = sub_dir / target
                    with zf.open(match) as src, open(extracted_path, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                    logger.info(f"Extracted {target} from {zip_filename}")

                    if compress == 'gzip':
                        compress_gzip(extracted_path)
                        extracted_path = extracted_path.with_suffix(extracted_path.suffix + '.gz')

                    if sample_root:
                        create_sample(extracted_path, output_dir, sample_root, sample_fraction)

                    downloaded += 1
        except zipfile.BadZipFile as e:
            logger.error(f"Bad zip file {zip_filename}: {e}")
            failed_urls.append(zip_url)
        finally:
            zip_path.unlink(missing_ok=True)

    return downloaded, skipped, failed_urls


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@app.command()
def download_data(
    output_dir: str = typer.Option("./data", "--output-dir", help="Output directory for downloads"),
    sample_fraction: float = typer.Option(DEFAULT_SAMPLE_FRACTION, "--sample-fraction",
                                          help="Fraction of lines to include in sample files (0 to disable)"),
):
    """Download data from sources defined in config."""
    config_path = Path("config/hsa/hsa_data_source_config.yaml")
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        raise typer.Exit(1)

    logger.info(f"Downloading data to {output_dir}...")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    if not config:
        logger.error("Invalid config: empty or malformed")
        raise typer.Exit(1)

    output_dir = Path(output_dir)
    sample_root = output_dir / 'sample' if sample_fraction > 0 else None
    total_downloaded = total_skipped = 0
    all_failed_urls = []

    for source_key, source_data in config.items():
        if not isinstance(source_data, dict):
            logger.warning(f"Unexpected format for source '{source_key}' — skipping")
            continue

        extract = source_data.get('extract', True)
        compress = source_data.get('compress', None)
        move_to = source_data.get('move_to', {})
        has_handled = False

        # Handle 'url' key
        if 'url' in source_data:
            d, s, f = process_urls(source_data['url'], output_dir, source_key, extract=extract,
                                   compress=compress, move_to=move_to,
                                   sample_root=sample_root, sample_fraction=sample_fraction)
            total_downloaded += d; total_skipped += s; all_failed_urls += f
            has_handled = True

        # Handle 'directories' key — scrape each HTML directory listing
        if 'directories' in source_data:
            for dir_name, dir_url in source_data['directories'].items():
                dir_output = output_dir / source_key / dir_name
                logger.info(f"Scraping directory {dir_url} to {dir_output}...")
                d, s, f = scrape_directory(dir_url, dir_output)
                total_downloaded += d; total_skipped += s; all_failed_urls += f
            has_handled = True

        # Handle 'zip_extract' key — download zip and extract specific files
        if 'zip_extract' in source_data:
            d, s, f = process_zip_extract(source_data['zip_extract'], output_dir, source_key,
                                          compress=compress, sample_root=sample_root,
                                          sample_fraction=sample_fraction)
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


if __name__ == "__main__":
    app()



# import yaml
# import requests
# import typer
# import logging
# from pathlib import Path
# from tqdm import tqdm
# import time
# import gzip
# import zipfile
# import shutil
# from urllib.parse import urlparse, urljoin
# from html.parser import HTMLParser

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# app = typer.Typer()


# # ---------------------------------------------------------------------------
# # Download helpers
# # ---------------------------------------------------------------------------

# def download_with_retry(url, output_path, max_retries=5):
#     """Download a file with retry logic and a progress bar."""
#     for attempt in range(max_retries):
#         try:
#             response = requests.get(url, stream=True)
#             response.raise_for_status()
#             total_size = int(response.headers.get('content-length', 0))

#             with open(output_path, 'wb') as f, tqdm(
#                 desc=output_path.name,
#                 total=total_size,
#                 unit='B',
#                 unit_scale=True,
#                 unit_divisor=1024,
#             ) as bar:
#                 for chunk in response.iter_content(chunk_size=8192):
#                     f.write(chunk)
#                     bar.update(len(chunk))
#             return True
#         except requests.RequestException as e:
#             logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
#             if attempt < max_retries - 1:
#                 time.sleep(2 ** attempt)
#     return False


# def already_downloaded(output_path, url):
#     """Return True if a local file exists and appears to match the remote file.
#     Uses content-length for comparison when available; falls back to trusting
#     the local file if the server doesn't provide it or the HEAD request fails."""
#     if not (output_path.exists() and output_path.stat().st_size > 0):
#         return False
#     try:
#         response = requests.head(url, timeout=10)
#         remote_size = int(response.headers.get('content-length', 0))
#         # remote_size == 0 means the server didn't send content-length — trust local file
#         return remote_size == 0 or output_path.stat().st_size == remote_size
#     except requests.RequestException:
#         # HEAD request failed — trust the local file
#         return True


# def extract_compressed(file_path):
#     """Decompress a .gz or .zip file in place."""
#     if file_path.suffix == '.gz':
#         extracted_path = file_path.with_suffix('')
#         with gzip.open(file_path, 'rb') as f_in, open(extracted_path, 'wb') as f_out:
#             shutil.copyfileobj(f_in, f_out)
#         file_path.unlink()
#         logger.info(f"Extracted {file_path} to {extracted_path}")
#     elif file_path.suffix == '.zip':
#         extract_dir = file_path.parent
#         with zipfile.ZipFile(file_path, 'r') as zf:
#             zf.extractall(extract_dir)
#         file_path.unlink()
#         logger.info(f"Extracted {file_path} to {extract_dir}")


# def compress_gzip(file_path):
#     """Gzip a file in place, appending .gz to its name."""
#     compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
#     with open(file_path, 'rb') as f_in, gzip.open(compressed_path, 'wb') as f_out:
#         shutil.copyfileobj(f_in, f_out)
#     file_path.unlink()
#     logger.info(f"Compressed {file_path} to {compressed_path}")


# def parse_url_comment(url_str):
#     """Split 'url # comment' into (url, comment). Comment may be None."""
#     if '#' in url_str:
#         url, comment = url_str.split('#', 1)
#         return url.strip(), comment.strip()
#     return url_str.strip(), None


# def should_skip_extract(comment):
#     """Return True if the inline URL comment requests keeping the file compressed."""
#     return bool(comment and (
#         'no extract' in comment.lower() or 'keep gzipped' in comment.lower()
#     ))





# # ---------------------------------------------------------------------------
# # Directory scraping
# # ---------------------------------------------------------------------------

# class _LinkParser(HTMLParser):
#     """Minimal HTML parser that collects href values from anchor tags."""
#     def __init__(self):
#         super().__init__()
#         self.links = []

#     def handle_starttag(self, tag, attrs):
#         if tag == 'a':
#             for name, value in attrs:
#                 if name == 'href' and value:
#                     self.links.append(value)


# def scrape_directory(dir_url, output_dir, max_retries=3):
#     """Fetch an HTML directory listing and download every file linked from it.
#     Subdirectory links (trailing slash) are skipped.
#     Files are always kept as-is — no decompression.
#     Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs."""
#     for attempt in range(max_retries):
#         try:
#             response = requests.get(dir_url, timeout=30)
#             response.raise_for_status()
#             break
#         except requests.RequestException as e:
#             logger.warning(f"Attempt {attempt + 1} failed fetching directory {dir_url}: {e}")
#             if attempt < max_retries - 1:
#                 time.sleep(2 ** attempt)
#     else:
#         logger.error(f"Could not fetch directory listing: {dir_url}")
#         return 0, 0, [dir_url]

#     parser = _LinkParser()
#     parser.feed(response.text)

#     output_dir.mkdir(parents=True, exist_ok=True)
#     downloaded = skipped = 0
#     failed_urls = []

#     for href in parser.links:
#         # Skip parent dir links, anchors, absolute paths, and subdirectories
#         if not href or href.startswith(('?', '#', '/')) or href in ('..', './') or href.endswith('/'):
#             continue

#         file_url = urljoin(dir_url, href)
#         filename = Path(urlparse(file_url).path).name
#         if not filename:
#             continue

#         output_path = output_dir / filename

#         if already_downloaded(output_path, file_url):
#             logger.info(f"Skipped {file_url} (already exists)")
#             skipped += 1
#             continue

#         if download_with_retry(file_url, output_path):
#             logger.info(f"Downloaded {file_url}")
#             downloaded += 1
#         else:
#             logger.error(f"Failed to download {file_url}")
#             failed_urls.append(file_url)

#     return downloaded, skipped, failed_urls


# # ---------------------------------------------------------------------------
# # URL processing
# # ---------------------------------------------------------------------------

# def is_filename_url_dict(d):
#     """Return True if a dict uses {filename: url} pairs instead of {sub_key: url_list}."""
#     return isinstance(d, dict) and all(
#         isinstance(v, str) and v.startswith('http')
#         for v in d.values()
#     )


# def _post_processed_path(output_path, extract, compress, comment, move_to_dest):
#     """Return the path where the file will end up after all post-processing.
#     Used to skip re-downloading files that were already processed in a previous run."""
#     p = output_path
#     # After extraction: .gz/.zip → decompressed (only if file is actually compressed)
#     if extract and not should_skip_extract(comment) and p.suffix in ('.gz', '.zip'):
#         if p.suffix == '.gz':
#             p = p.with_suffix('')
#     # After compression: plain file → .gz appended
#     elif compress == 'gzip' and p.suffix != '.gz':
#         p = p.with_suffix(p.suffix + '.gz')
#     # After move_to: file lands in a different directory
#     if move_to_dest:
#         p = move_to_dest / p.name
#     return p


# def download_one(url, output_path, extract, compress=None, move_to_dest=None):
#     """Download a single URL to output_path, applying post-processing and optional move.

#     Post-processing order:
#       1. extract (decompress .gz / .zip) or compress (gzip plain file)
#       2. move_to_dest: if provided, move the final file to this resolved Path

#     compress: None (no compression) or 'gzip' (gzip after download).
#     move_to_dest: resolved Path to move the file into after post-processing, or None.
#     Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs."""
#     url, comment = parse_url_comment(url)

#     # A directory at the output path means a previous run misidentified the
#     # filename as a sub_key and called mkdir on it — remove it so we can write the file.
#     if output_path.is_dir():
#         logger.warning(f"Removing unexpected directory at {output_path} (will be replaced by file)")
#         shutil.rmtree(output_path)

#     # Check the post-processed path first: if a previous run already extracted,
#     # compressed, or moved the file, the original filename won't exist on disk.
#     post_processed_path = _post_processed_path(output_path, extract, compress, comment, move_to_dest)
#     if post_processed_path.exists() and post_processed_path.stat().st_size > 0:
#         logger.info(f"Skipped {url} (already exists as {post_processed_path.name})")
#         return 0, 1, []

#     if already_downloaded(output_path, url):
#         logger.info(f"Skipped {url} (already exists)")
#         return 0, 1, []

#     if not download_with_retry(url, output_path):
#         logger.error(f"Failed to download {url}")
#         return 0, 0, [url]

#     # --- post-processing: extract or compress ---
#     # Extract takes priority; compress only applies if extraction didn't happen
#     # (i.e. file was not compressed to begin with, or extract=False).
#     if extract and not should_skip_extract(comment) and output_path.suffix in ('.gz', '.zip'):
#         extract_compressed(output_path)
#         if output_path.suffix == '.gz':
#             output_path = output_path.with_suffix('')
#     elif compress == 'gzip' and output_path.suffix != '.gz':
#         compress_gzip(output_path)
#         output_path = output_path.with_suffix(output_path.suffix + '.gz')

#     # --- post-processing: move_to ---
#     if move_to_dest:
#         move_to_dest.mkdir(parents=True, exist_ok=True)
#         dest_path = move_to_dest / output_path.name
#         shutil.move(str(output_path), str(dest_path))
#         logger.info(f"Moved {output_path.name} to {dest_path}")

#     logger.info(f"Downloaded {url}")
#     return 1, 0, []


# def resolve_move_to(dest_str, output_dir):
#     """Resolve a move_to destination relative to output_dir.

#     Example:
#         output_dir = /input/hsa/
#         dest_str   = ../rna_central
#         result     = /input/rna_central/
#     """
#     return (output_dir / dest_str).resolve()


# def process_urls(urls, output_dir, source_key, sub_key=None, extract=True, compress=None, move_to=None):
#     """Recursively process URLs from a YAML source entry.

#     Supported url formats:
#       - str:                  single URL, filename taken from URL path
#       - list[str]:            multiple URLs, filenames taken from URL paths
#       - dict{filename: url}:  explicit output filenames mapped to their URLs
#       - dict{sub_key: urls}:  recurse into named subdirectories

#     move_to: optional dict of {filename: dest_path_str} from the YAML move_to key.
#              Filenames are matched against the downloaded file's final name.

#     Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs.
#     """
#     downloaded = skipped = 0
#     failed_urls = []
#     sub_dir = output_dir / source_key / sub_key if sub_key else output_dir / source_key
#     move_to = move_to or {}

#     def get_move_to_dest(filename):
#         """Return resolved destination Path for filename, or None."""
#         dest_str = move_to.get(filename)
#         return resolve_move_to(dest_str, output_dir) if dest_str else None

#     if isinstance(urls, str):
#         # Single URL — filename from URL path
#         sub_dir.mkdir(parents=True, exist_ok=True)
#         filename = Path(urlparse(urls.split('#')[0].strip()).path).name
#         return download_one(urls, sub_dir / filename, extract, compress, get_move_to_dest(filename))

#     if isinstance(urls, dict):
#         if is_filename_url_dict(urls):
#             # {filename: url} — use the dict key as the output filename
#             sub_dir.mkdir(parents=True, exist_ok=True)
#             for filename, url in urls.items():
#                 d, s, f = download_one(url, sub_dir / filename, extract, compress, get_move_to_dest(filename))
#                 downloaded += d; skipped += s; failed_urls += f
#         else:
#             # {sub_key: url_list} — recurse into named subdirectories
#             for sub, sub_urls in urls.items():
#                 d, s, f = process_urls(sub_urls, output_dir, source_key, sub, extract=extract, compress=compress, move_to=move_to)
#                 downloaded += d; skipped += s; failed_urls += f
#         return downloaded, skipped, failed_urls

#     # List of URLs — filename from each URL path
#     sub_dir.mkdir(parents=True, exist_ok=True)
#     for url in urls:
#         url_clean = url.split('#')[0].strip()
#         filename = Path(urlparse(url_clean).path).name
#         d, s, f = download_one(url, sub_dir / filename, extract, compress, get_move_to_dest(filename))
#         downloaded += d; skipped += s; failed_urls += f

#     return downloaded, skipped, failed_urls


# # ---------------------------------------------------------------------------
# # zip_extract processing
# # ---------------------------------------------------------------------------

# def process_zip_extract(zip_extract_list, output_dir, source_key, compress=None):
#     """Download a zip, extract only the specified files from it, then delete the zip.
#     Optionally gzip each extracted file if compress='gzip'.

#     zip_extract_list: list of dicts with keys:
#         url:   URL of the zip file
#         files: list of filenames to extract from the zip

#     Returns (downloaded, skipped, failed_urls).
#     """
#     downloaded = skipped = failed_urls = 0, 0, []
#     downloaded, skipped, failed_urls = 0, 0, []
#     sub_dir = output_dir / source_key
#     sub_dir.mkdir(parents=True, exist_ok=True)

#     for entry in zip_extract_list:
#         zip_url = entry.get('url')
#         target_files = entry.get('files', [])

#         if not zip_url:
#             logger.warning(f"zip_extract entry missing 'url' — skipping")
#             continue

#         # Check if all target files already exist (post-processed)
#         all_exist = all(
#             (sub_dir / (f + '.gz' if compress == 'gzip' else f)).exists()
#             for f in target_files
#         )
#         if all_exist:
#             logger.info(f"Skipped {zip_url} (all target files already exist)")
#             skipped += len(target_files)
#             continue

#         # Download the zip to a temp path
#         zip_filename = Path(urlparse(zip_url).path).name
#         zip_path = sub_dir / zip_filename

#         if not download_with_retry(zip_url, zip_path):
#             logger.error(f"Failed to download {zip_url}")
#             failed_urls.append(zip_url)
#             continue

#         # Extract only the requested files
#         try:
#             with zipfile.ZipFile(zip_path, 'r') as zf:
#                 available = zf.namelist()
#                 for target in target_files:
#                     # Support files nested inside zip subdirectories
#                     match = next((n for n in available if n.endswith(target)), None)
#                     if not match:
#                         logger.warning(f"File '{target}' not found in {zip_filename}. Available: {available}")
#                         failed_urls.append(f"{zip_url}#{target}")
#                         continue
#                     # Extract to sub_dir, flattening any subdirectory structure
#                     extracted_path = sub_dir / target
#                     with zf.open(match) as src, open(extracted_path, 'wb') as dst:
#                         shutil.copyfileobj(src, dst)
#                     logger.info(f"Extracted {target} from {zip_filename}")

#                     if compress == 'gzip':
#                         compress_gzip(extracted_path)

#                     downloaded += 1
#         except zipfile.BadZipFile as e:
#             logger.error(f"Bad zip file {zip_filename}: {e}")
#             failed_urls.append(zip_url)
#         finally:
#             zip_path.unlink(missing_ok=True)

#     return downloaded, skipped, failed_urls


# # ---------------------------------------------------------------------------
# # CLI entry point
# # ---------------------------------------------------------------------------

# @app.command()
# def download_data(output_dir: str = typer.Option("./data", "--output-dir", help="Output directory for downloads")):
#     """Download data from sources defined in config."""
#     config_path = Path("config/hsa/hsa_data_source_config.yaml")
#     if not config_path.exists():
#         logger.error(f"Config file not found: {config_path}")
#         raise typer.Exit(1)

#     logger.info(f"Downloading data to {output_dir}...")
#     with open(config_path, 'r') as f:
#         config = yaml.safe_load(f)

#     if not config:
#         logger.error("Invalid config: empty or malformed")
#         raise typer.Exit(1)

#     output_dir = Path(output_dir)
#     total_downloaded = total_skipped = 0
#     all_failed_urls = []

#     for source_key, source_data in config.items():
#         if not isinstance(source_data, dict):
#             logger.warning(f"Unexpected format for source '{source_key}' — skipping")
#             continue

#         extract = source_data.get('extract', True)
#         compress = source_data.get('compress', None)
#         move_to = source_data.get('move_to', {})
#         has_handled = False

#         # Handle 'url' key
#         if 'url' in source_data:
#             d, s, f = process_urls(source_data['url'], output_dir, source_key, extract=extract, compress=compress, move_to=move_to)
#             total_downloaded += d; total_skipped += s; all_failed_urls += f
#             has_handled = True

#         # Handle 'directories' key — scrape each HTML directory listing
#         if 'directories' in source_data:
#             for dir_name, dir_url in source_data['directories'].items():
#                 dir_output = output_dir / source_key / dir_name
#                 logger.info(f"Scraping directory {dir_url} to {dir_output}...")
#                 d, s, f = scrape_directory(dir_url, dir_output)
#                 total_downloaded += d; total_skipped += s; all_failed_urls += f
#             has_handled = True

#         # Handle 'zip_extract' key — download zip and extract specific files
#         if 'zip_extract' in source_data:
#             d, s, f = process_zip_extract(source_data['zip_extract'], output_dir, source_key, compress=compress)
#             total_downloaded += d; total_skipped += s; all_failed_urls += f
#             has_handled = True

#         if not has_handled:
#             logger.warning(f"No 'url' or 'directories' key found for source '{source_key}' — skipping")

#     logger.info(
#         f"Download complete: {total_downloaded} downloaded, "
#         f"{total_skipped} skipped, {len(all_failed_urls)} failed"
#     )
#     if all_failed_urls:
#         logger.warning("Failed URLs:")
#         for url in all_failed_urls:
#             logger.warning(f"  {url}")


# if __name__ == "__main__":
#     app()




# # import yaml
# # import requests
# # import typer
# # import logging
# # from pathlib import Path
# # from tqdm import tqdm
# # import time
# # import gzip
# # import zipfile
# # import shutil
# # from urllib.parse import urlparse, urljoin
# # from html.parser import HTMLParser

# # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# # logger = logging.getLogger(__name__)

# # app = typer.Typer()


# # # ---------------------------------------------------------------------------
# # # Download helpers
# # # ---------------------------------------------------------------------------

# # def download_with_retry(url, output_path, max_retries=5):
# #     """Download a file with retry logic and a progress bar."""
# #     for attempt in range(max_retries):
# #         try:
# #             response = requests.get(url, stream=True)
# #             response.raise_for_status()
# #             total_size = int(response.headers.get('content-length', 0))

# #             with open(output_path, 'wb') as f, tqdm(
# #                 desc=output_path.name,
# #                 total=total_size,
# #                 unit='B',
# #                 unit_scale=True,
# #                 unit_divisor=1024,
# #             ) as bar:
# #                 for chunk in response.iter_content(chunk_size=8192):
# #                     f.write(chunk)
# #                     bar.update(len(chunk))
# #             return True
# #         except requests.RequestException as e:
# #             logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
# #             if attempt < max_retries - 1:
# #                 time.sleep(2 ** attempt)
# #     return False


# # def already_downloaded(output_path, url):
# #     """Return True if a local file exists and appears to match the remote file.
# #     Uses content-length for comparison when available; falls back to trusting
# #     the local file if the server doesn't provide it or the HEAD request fails."""
# #     if not (output_path.exists() and output_path.stat().st_size > 0):
# #         return False
# #     try:
# #         response = requests.head(url, timeout=10)
# #         remote_size = int(response.headers.get('content-length', 0))
# #         # remote_size == 0 means the server didn't send content-length — trust local file
# #         return remote_size == 0 or output_path.stat().st_size == remote_size
# #     except requests.RequestException:
# #         # HEAD request failed — trust the local file
# #         return True


# # def extract_compressed(file_path):
# #     """Decompress a .gz or .zip file in place."""
# #     if file_path.suffix == '.gz':
# #         extracted_path = file_path.with_suffix('')
# #         with gzip.open(file_path, 'rb') as f_in, open(extracted_path, 'wb') as f_out:
# #             shutil.copyfileobj(f_in, f_out)
# #         file_path.unlink()
# #         logger.info(f"Extracted {file_path} to {extracted_path}")
# #     elif file_path.suffix == '.zip':
# #         extract_dir = file_path.parent
# #         with zipfile.ZipFile(file_path, 'r') as zf:
# #             zf.extractall(extract_dir)
# #         file_path.unlink()
# #         logger.info(f"Extracted {file_path} to {extract_dir}")


# # def compress_gzip(file_path):
# #     """Gzip a file in place, appending .gz to its name."""
# #     compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
# #     with open(file_path, 'rb') as f_in, gzip.open(compressed_path, 'wb') as f_out:
# #         shutil.copyfileobj(f_in, f_out)
# #     file_path.unlink()
# #     logger.info(f"Compressed {file_path} to {compressed_path}")


# # def parse_url_comment(url_str):
# #     """Split 'url # comment' into (url, comment). Comment may be None."""
# #     if '#' in url_str:
# #         url, comment = url_str.split('#', 1)
# #         return url.strip(), comment.strip()
# #     return url_str.strip(), None


# # def should_skip_extract(comment):
# #     """Return True if the inline URL comment requests keeping the file compressed."""
# #     return bool(comment and (
# #         'no extract' in comment.lower() or 'keep gzipped' in comment.lower()
# #     ))





# # # ---------------------------------------------------------------------------
# # # Directory scraping
# # # ---------------------------------------------------------------------------

# # class _LinkParser(HTMLParser):
# #     """Minimal HTML parser that collects href values from anchor tags."""
# #     def __init__(self):
# #         super().__init__()
# #         self.links = []

# #     def handle_starttag(self, tag, attrs):
# #         if tag == 'a':
# #             for name, value in attrs:
# #                 if name == 'href' and value:
# #                     self.links.append(value)


# # def scrape_directory(dir_url, output_dir, max_retries=3):
# #     """Fetch an HTML directory listing and download every file linked from it.
# #     Subdirectory links (trailing slash) are skipped.
# #     Files are always kept as-is — no decompression.
# #     Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs."""
# #     for attempt in range(max_retries):
# #         try:
# #             response = requests.get(dir_url, timeout=30)
# #             response.raise_for_status()
# #             break
# #         except requests.RequestException as e:
# #             logger.warning(f"Attempt {attempt + 1} failed fetching directory {dir_url}: {e}")
# #             if attempt < max_retries - 1:
# #                 time.sleep(2 ** attempt)
# #     else:
# #         logger.error(f"Could not fetch directory listing: {dir_url}")
# #         return 0, 0, [dir_url]

# #     parser = _LinkParser()
# #     parser.feed(response.text)

# #     output_dir.mkdir(parents=True, exist_ok=True)
# #     downloaded = skipped = 0
# #     failed_urls = []

# #     for href in parser.links:
# #         # Skip parent dir links, anchors, absolute paths, and subdirectories
# #         if not href or href.startswith(('?', '#', '/')) or href in ('..', './') or href.endswith('/'):
# #             continue

# #         file_url = urljoin(dir_url, href)
# #         filename = Path(urlparse(file_url).path).name
# #         if not filename:
# #             continue

# #         output_path = output_dir / filename

# #         if already_downloaded(output_path, file_url):
# #             logger.info(f"Skipped {file_url} (already exists)")
# #             skipped += 1
# #             continue

# #         if download_with_retry(file_url, output_path):
# #             logger.info(f"Downloaded {file_url}")
# #             downloaded += 1
# #         else:
# #             logger.error(f"Failed to download {file_url}")
# #             failed_urls.append(file_url)

# #     return downloaded, skipped, failed_urls


# # # ---------------------------------------------------------------------------
# # # URL processing
# # # ---------------------------------------------------------------------------

# # def is_filename_url_dict(d):
# #     """Return True if a dict uses {filename: url} pairs instead of {sub_key: url_list}."""
# #     return isinstance(d, dict) and all(
# #         isinstance(v, str) and v.startswith('http')
# #         for v in d.values()
# #     )


# # def _post_processed_path(output_path, extract, compress, comment, move_to_dest):
# #     """Return the path where the file will end up after all post-processing.
# #     Used to skip re-downloading files that were already processed in a previous run."""
# #     p = output_path
# #     # After extraction: .gz → no suffix, .zip → unpredictable, skip
# #     if extract and not should_skip_extract(comment):
# #         if p.suffix == '.gz':
# #             p = p.with_suffix('')
# #     # After compression: plain → .gz appended
# #     elif compress == 'gzip' and p.suffix != '.gz':
# #         p = p.with_suffix(p.suffix + '.gz')
# #     # After move_to: file lands in a different directory
# #     if move_to_dest:
# #         p = move_to_dest / p.name
# #     return p


# # def download_one(url, output_path, extract, compress=None, move_to_dest=None):
# #     """Download a single URL to output_path, applying post-processing and optional move.

# #     Post-processing order:
# #       1. extract (decompress .gz / .zip) or compress (gzip plain file)
# #       2. move_to_dest: if provided, move the final file to this resolved Path

# #     compress: None (no compression) or 'gzip' (gzip after download).
# #     move_to_dest: resolved Path to move the file into after post-processing, or None.
# #     Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs."""
# #     url, comment = parse_url_comment(url)

# #     # A directory at the output path means a previous run misidentified the
# #     # filename as a sub_key and called mkdir on it — remove it so we can write the file.
# #     if output_path.is_dir():
# #         logger.warning(f"Removing unexpected directory at {output_path} (will be replaced by file)")
# #         shutil.rmtree(output_path)

# #     # Check the post-processed path first: if a previous run already extracted,
# #     # compressed, or moved the file, the original filename won't exist on disk.
# #     post_processed_path = _post_processed_path(output_path, extract, compress, comment, move_to_dest)
# #     if post_processed_path.exists() and post_processed_path.stat().st_size > 0:
# #         logger.info(f"Skipped {url} (already exists as {post_processed_path.name})")
# #         return 0, 1, []

# #     if already_downloaded(output_path, url):
# #         logger.info(f"Skipped {url} (already exists)")
# #         return 0, 1, []

# #     if not download_with_retry(url, output_path):
# #         logger.error(f"Failed to download {url}")
# #         return 0, 0, [url]

# #     # --- post-processing: extract or compress ---
# #     if extract and not should_skip_extract(comment):
# #         extract_compressed(output_path)
# #         if output_path.suffix == '.gz':
# #             output_path = output_path.with_suffix('')
# #     elif compress == 'gzip':
# #         compress_gzip(output_path)
# #         if output_path.suffix != '.gz':
# #             output_path = output_path.with_suffix(output_path.suffix + '.gz')

# #     # --- post-processing: move_to ---
# #     if move_to_dest:
# #         move_to_dest.mkdir(parents=True, exist_ok=True)
# #         dest_path = move_to_dest / output_path.name
# #         shutil.move(str(output_path), str(dest_path))
# #         logger.info(f"Moved {output_path.name} to {dest_path}")

# #     logger.info(f"Downloaded {url}")
# #     return 1, 0, []


# # def resolve_move_to(dest_str, output_dir):
# #     """Resolve a move_to destination relative to output_dir.

# #     Example:
# #         output_dir = /input/hsa/
# #         dest_str   = ../rna_central
# #         result     = /input/rna_central/
# #     """
# #     return (output_dir / dest_str).resolve()


# # def process_urls(urls, output_dir, source_key, sub_key=None, extract=True, compress=None, move_to=None):
# #     """Recursively process URLs from a YAML source entry.

# #     Supported url formats:
# #       - str:                  single URL, filename taken from URL path
# #       - list[str]:            multiple URLs, filenames taken from URL paths
# #       - dict{filename: url}:  explicit output filenames mapped to their URLs
# #       - dict{sub_key: urls}:  recurse into named subdirectories

# #     move_to: optional dict of {filename: dest_path_str} from the YAML move_to key.
# #              Filenames are matched against the downloaded file's final name.

# #     Returns (downloaded, skipped, failed_urls) where failed_urls is a list of URLs.
# #     """
# #     downloaded = skipped = 0
# #     failed_urls = []
# #     sub_dir = output_dir / source_key / sub_key if sub_key else output_dir / source_key
# #     move_to = move_to or {}

# #     def get_move_to_dest(filename):
# #         """Return resolved destination Path for filename, or None."""
# #         dest_str = move_to.get(filename)
# #         return resolve_move_to(dest_str, output_dir) if dest_str else None

# #     if isinstance(urls, str):
# #         # Single URL — filename from URL path
# #         sub_dir.mkdir(parents=True, exist_ok=True)
# #         filename = Path(urlparse(urls.split('#')[0].strip()).path).name
# #         return download_one(urls, sub_dir / filename, extract, compress, get_move_to_dest(filename))

# #     if isinstance(urls, dict):
# #         if is_filename_url_dict(urls):
# #             # {filename: url} — use the dict key as the output filename
# #             sub_dir.mkdir(parents=True, exist_ok=True)
# #             for filename, url in urls.items():
# #                 d, s, f = download_one(url, sub_dir / filename, extract, compress, get_move_to_dest(filename))
# #                 downloaded += d; skipped += s; failed_urls += f
# #         else:
# #             # {sub_key: url_list} — recurse into named subdirectories
# #             for sub, sub_urls in urls.items():
# #                 d, s, f = process_urls(sub_urls, output_dir, source_key, sub, extract=extract, compress=compress, move_to=move_to)
# #                 downloaded += d; skipped += s; failed_urls += f
# #         return downloaded, skipped, failed_urls

# #     # List of URLs — filename from each URL path
# #     sub_dir.mkdir(parents=True, exist_ok=True)
# #     for url in urls:
# #         url_clean = url.split('#')[0].strip()
# #         filename = Path(urlparse(url_clean).path).name
# #         d, s, f = download_one(url, sub_dir / filename, extract, compress, get_move_to_dest(filename))
# #         downloaded += d; skipped += s; failed_urls += f

# #     return downloaded, skipped, failed_urls


# # # ---------------------------------------------------------------------------
# # # CLI entry point
# # # ---------------------------------------------------------------------------

# # @app.command()
# # def download_data(output_dir: str = typer.Option("./data", "--output-dir", help="Output directory for downloads")):
# #     """Download data from sources defined in config."""
# #     config_path = Path("config/hsa/hsa_data_source_config.yaml")
# #     if not config_path.exists():
# #         logger.error(f"Config file not found: {config_path}")
# #         raise typer.Exit(1)

# #     logger.info(f"Downloading data to {output_dir}...")
# #     with open(config_path, 'r') as f:
# #         config = yaml.safe_load(f)

# #     if not config:
# #         logger.error("Invalid config: empty or malformed")
# #         raise typer.Exit(1)

# #     output_dir = Path(output_dir)
# #     total_downloaded = total_skipped = 0
# #     all_failed_urls = []

# #     for source_key, source_data in config.items():
# #         if not isinstance(source_data, dict):
# #             logger.warning(f"Unexpected format for source '{source_key}' — skipping")
# #             continue

# #         extract = source_data.get('extract', True)
# #         compress = source_data.get('compress', None)
# #         move_to = source_data.get('move_to', {})
# #         has_handled = False

# #         # Handle 'url' key
# #         if 'url' in source_data:
# #             d, s, f = process_urls(source_data['url'], output_dir, source_key, extract=extract, compress=compress, move_to=move_to)
# #             total_downloaded += d; total_skipped += s; all_failed_urls += f
# #             has_handled = True

# #         # Handle 'directories' key — scrape each HTML directory listing
# #         if 'directories' in source_data:
# #             for dir_name, dir_url in source_data['directories'].items():
# #                 dir_output = output_dir / source_key / dir_name
# #                 logger.info(f"Scraping directory {dir_url} to {dir_output}...")
# #                 d, s, f = scrape_directory(dir_url, dir_output)
# #                 total_downloaded += d; total_skipped += s; all_failed_urls += f
# #             has_handled = True

# #         if not has_handled:
# #             logger.warning(f"No 'url' or 'directories' key found for source '{source_key}' — skipping")

# #     logger.info(
# #         f"Download complete: {total_downloaded} downloaded, "
# #         f"{total_skipped} skipped, {len(all_failed_urls)} failed"
# #     )
# #     if all_failed_urls:
# #         logger.warning("Failed URLs:")
# #         for url in all_failed_urls:
# #             logger.warning(f"  {url}")


# # if __name__ == "__main__":
# #     app()
