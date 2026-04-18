import yaml
import requests
import typer
import logging
from pathlib import Path
from tqdm import tqdm
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
# CLI entry point
# ---------------------------------------------------------------------------

@app.command()
def download_data(output_dir: str = typer.Option("./data", "--output-dir", help="Output directory for downloads")):
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
                logger.info(f"Scraping directory {dir_url} to {dir_output}...")
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


if __name__ == "__main__":
    app()
