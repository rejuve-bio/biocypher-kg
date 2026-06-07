import io
import math
import gzip
import re
import shutil
import tarfile
import time
import yaml
import zipfile
import logging
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, urljoin

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_SAMPLE_FRACTION = 0.01


# ---------------------------------------------------------------------------
# Module-level helpers (stateless, no session needed)
# ---------------------------------------------------------------------------

def _open_text(file_path: Path):
    """Open a file for text reading, transparently decompressing .gz."""
    if file_path.suffix == '.gz':
        return gzip.open(file_path, 'rt', errors='replace')
    return open(file_path, 'r', errors='replace')


def _parse_url_comment(url_str: str) -> tuple[str, str | None]:
    """Split 'url  # comment' into (url, comment). Comment may be None."""
    if '#' in url_str:
        url, comment = url_str.split('#', 1)
        return url.strip(), comment.strip()
    return url_str.strip(), None


def _should_skip_extract(comment: str | None) -> bool:
    """Return True if the inline URL comment requests keeping the file compressed."""
    return bool(comment and (
        'no extract' in comment.lower() or 'keep gzipped' in comment.lower()
    ))


def _is_filename_url_dict(d: dict) -> bool:
    """Return True if dict uses {filename: url} pairs rather than {sub_key: url_list}."""
    return isinstance(d, dict) and all(
        isinstance(v, str) and v.startswith('http') for v in d.values()
    )


def _post_processed_path(output_path: Path, extract: bool, compress: str | None,
                         comment: str | None, move_to_dest: Path | None) -> Path:
    """Return the path where a file will land after all post-processing.
    Used to detect files already handled in a previous run."""
    p = output_path
    if extract and not _should_skip_extract(comment) and p.suffix in ('.gz', '.zip'):
        if p.suffix == '.gz':
            p = p.with_suffix('')
    elif compress == 'gzip' and p.suffix != '.gz':
        p = p.with_suffix(p.suffix + '.gz')
    if move_to_dest:
        p = move_to_dest / p.name
    return p


class _LinkParser(HTMLParser):
    """Minimal HTML parser that collects href values from anchor tags."""
    def __init__(self):
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href' and value:
                    self.links.append(value)


# ---------------------------------------------------------------------------
# DownloadManager
# ---------------------------------------------------------------------------

class DownloadManager:
    def __init__(self, config_path: str, output_dir: Path,
                 sample_fraction: float = DEFAULT_SAMPLE_FRACTION):
        self.config = self._load_config(config_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sample_fraction = sample_fraction
        self.sample_root = self.output_dir / 'sample' if sample_fraction > 0 else None

        # Requests session with built-in retry for transient HTTP errors
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    def _load_config(self, config_path: str) -> dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    # ------------------------------------------------------------------
    # Core download primitive
    # ------------------------------------------------------------------

    def _download_file(self, url: str, filepath: Path,
                       verify: bool = True, max_retries: int = 5) -> bool:
        """Download url to filepath with progress bar, 0-byte validation, and retries.

        Returns True on success, False after all retries are exhausted.
        Treats a 0-byte result as a failure — this handles S3/FTP redirects
        that return HTTP 200 with an empty body on the first request.
        The incomplete file is deleted before each retry.
        """
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, stream=True, allow_redirects=True, verify=verify)
                r.raise_for_status()

                file_size = int(r.headers.get('Content-Length', 0))
                with tqdm(total=file_size, unit='iB', unit_scale=True,
                          desc=f"Downloading {filepath.name}") as pbar:
                    with filepath.open('wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))

                # Validate: 0-byte file = empty body (S3/FTP redirect or transient error)
                written = filepath.stat().st_size if filepath.exists() else 0
                if written == 0:
                    filepath.unlink(missing_ok=True)
                    raise ValueError("Downloaded file is 0 bytes — likely an S3/FTP redirect or transient error")

                return True

            except (requests.RequestException, ValueError) as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                if filepath.exists():
                    filepath.unlink(missing_ok=True)
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logger.info(f"Retrying in {wait}s...")
                    time.sleep(wait)

        logger.error(f"All {max_retries} attempts failed for {url}")
        return False

    # ------------------------------------------------------------------
    # Already-downloaded check
    # ------------------------------------------------------------------

    def _already_downloaded(self, output_path: Path, url: str) -> bool:
        """Return True if a local file exists, is non-empty, and matches the remote size.

        A 0-byte local file is NEVER considered complete — guards against S3/FTP
        endpoints that leave behind empty files on failed first attempts.
        """
        local_size = output_path.stat().st_size if output_path.exists() else 0
        if local_size == 0:
            if output_path.exists():
                logger.warning(f"Found 0-byte file {output_path.name} — will re-download")
                output_path.unlink(missing_ok=True)
            return False
        try:
            response = self.session.head(url, timeout=10)
            remote_size = int(response.headers.get('content-length', 0))
            if remote_size == 0:
                return True   # server didn't provide content-length — trust local file
            return local_size == remote_size
        except requests.RequestException:
            return True       # HEAD failed — trust the non-empty local file

    # ------------------------------------------------------------------
    # Post-processing: extract / compress / move
    # ------------------------------------------------------------------

    def _extract_compressed(self, file_path: Path) -> Path:
        """Decompress a .gz or .zip file in place. Returns the resulting path."""
        if file_path.suffix == '.gz':
            extracted_path = file_path.with_suffix('')
            with gzip.open(file_path, 'rb') as f_in, open(extracted_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            file_path.unlink()
            logger.info(f"Extracted {file_path.name} → {extracted_path.name}")
            return extracted_path
        elif file_path.suffix == '.zip':
            with zipfile.ZipFile(file_path, 'r') as zf:
                zf.extractall(file_path.parent)
            file_path.unlink()
            logger.info(f"Extracted {file_path.name} → {file_path.parent}")
            return file_path.parent
        return file_path

    def _compress_gzip(self, file_path: Path) -> Path:
        """Gzip a file in place, appending .gz. Returns the new path."""
        compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
        with open(file_path, 'rb') as f_in, gzip.open(compressed_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        file_path.unlink()
        logger.info(f"Compressed {file_path.name} → {compressed_path.name}")
        return compressed_path

    def _resolve_move_to(self, dest_str: str) -> Path:
        return (Path.cwd() / dest_str).resolve()

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    def _create_sample(self, file_path: Path) -> bool:
        """Write a 1%-line sample of file_path mirrored under sample_root.

        Returns True if a new sample was created.
        Preserves compression (.gz input → .gz sample).
        For very small files (≤ 1/fraction lines), copies the whole file.
        """
        if not self.sample_root:
            return False
        fraction = self.sample_fraction
        try:
            rel_path = file_path.relative_to(self.output_dir)
        except ValueError:
            logger.warning(f"Cannot compute relative path for sample: {file_path}")
            return False

        sample_path = self.sample_root / rel_path
        if sample_path.exists() and sample_path.stat().st_size > 0:
            return False

        sample_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            total_lines = sum(1 for _ in _open_text(file_path))
        except Exception as e:
            logger.warning(f"Skipping sample for {file_path.name}: cannot read as text ({e})")
            return False

        if total_lines == 0:
            return False

        min_lines = math.ceil(1 / fraction) if fraction > 0 else float('inf')
        if total_lines <= min_lines:
            shutil.copy2(str(file_path), str(sample_path))
            return True

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
            return True
        except Exception as e:
            logger.warning(f"Failed to create sample for {file_path.name}: {e}")
            return False

    def _ensure_samples(self, source_key: str) -> int:
        """Backfill samples for any file under output_dir/source_key/ that lacks one.
        Returns count of new samples created."""
        if not self.sample_root:
            return 0
        walk_root = self.output_dir / source_key
        if not walk_root.exists():
            return 0
        created = 0
        for file_path in sorted(walk_root.rglob('*')):
            if not file_path.is_file():
                continue
            try:
                file_path.relative_to(self.sample_root)
                continue   # inside sample/ tree — skip
            except ValueError:
                pass
            created += self._create_sample(file_path)
        return created

    # ------------------------------------------------------------------
    # Single-file download with full post-processing
    # ------------------------------------------------------------------

    def _download_one(self, url: str, output_path: Path, extract: bool,
                      compress: str | None = None,
                      move_to_dest: Path | None = None) -> tuple[int, int, list[str], int]:
        """Download one URL, apply post-processing, create sample.

        Returns (downloaded, skipped, failed_filenames, samples_created).
        """
        url, comment = _parse_url_comment(url)

        if output_path.is_dir():
            logger.warning(f"Removing unexpected directory at {output_path}")
            shutil.rmtree(output_path)

        # Check post-processed path first (handles files from previous runs)
        post_path = _post_processed_path(output_path, extract, compress, comment, move_to_dest)
        if self._already_downloaded(post_path, url):
            logger.info(f"Skipped {url} (already exists as {post_path.name})")
            return 0, 1, [], 0

        if self._already_downloaded(output_path, url):
            logger.info(f"Skipped {url} (already exists)")
            return 0, 1, [], 0

        filename = output_path.name
        if not self._download_file(url, output_path):
            return 0, 0, [filename], 0

        # Extract or compress
        if extract and not _should_skip_extract(comment) and output_path.suffix in ('.gz', '.zip'):
            output_path = self._extract_compressed(output_path)
        elif compress == 'gzip' and output_path.suffix != '.gz':
            output_path = self._compress_gzip(output_path)

        # Move
        if move_to_dest:
            move_to_dest.mkdir(parents=True, exist_ok=True)
            dest_path = move_to_dest / output_path.name
            source_dir = output_path.parent
            shutil.move(str(output_path), str(dest_path))
            output_path = dest_path
            logger.info(f"Moved {output_path.name} → {dest_path}")
            if source_dir.exists() and not any(source_dir.iterdir()):
                source_dir.rmdir()

        # Sample
        sc = self._create_sample(output_path) if output_path.exists() else 0

        logger.info(f"Downloaded {url}")
        return 1, 0, [], sc

    # ------------------------------------------------------------------
    # URL-list processor
    # ------------------------------------------------------------------

    def _process_urls(self, urls, source_key: str, sub_key: str | None = None,
                      extract: bool = True, compress: str | None = None,
                      move_to: dict | None = None) -> tuple[int, int, list[str], int]:
        """Recursively process a YAML url value (str / list / dict).

        Returns (downloaded, skipped, failed_filenames, samples_created).
        """
        downloaded = skipped = samples_created = 0
        failed_filenames: list[str] = []
        sub_dir = self.output_dir / source_key / sub_key if sub_key else self.output_dir / source_key
        move_to = move_to or {}

        def get_dest(filename):
            dest_str = move_to.get(filename)
            return self._resolve_move_to(dest_str) if dest_str else None

        def dl(url, path, filename):
            return self._download_one(url, path, extract, compress, get_dest(filename))

        if isinstance(urls, str):
            sub_dir.mkdir(parents=True, exist_ok=True)
            filename = Path(urlparse(urls.split('#')[0].strip()).path).name
            return dl(urls, sub_dir / filename, filename)

        if isinstance(urls, dict):
            if _is_filename_url_dict(urls):
                for filename, url in urls.items():
                    sub_dir.mkdir(parents=True, exist_ok=True)
                    d, s, f, sc = dl(url, sub_dir / filename, filename)
                    downloaded += d; skipped += s; failed_filenames += f; samples_created += sc
            else:
                for sub, sub_urls in urls.items():
                    d, s, f, sc = self._process_urls(sub_urls, source_key, sub,
                                                     extract=extract, compress=compress,
                                                     move_to=move_to)
                    downloaded += d; skipped += s; failed_filenames += f; samples_created += sc
            return downloaded, skipped, failed_filenames, samples_created

        # list of URLs
        for url in urls:
            sub_dir.mkdir(parents=True, exist_ok=True)
            url_clean = url.split('#')[0].strip()
            filename = Path(urlparse(url_clean).path).name
            d, s, f, sc = dl(url, sub_dir / filename, filename)
            downloaded += d; skipped += s; failed_filenames += f; samples_created += sc

        return downloaded, skipped, failed_filenames, samples_created

    # ------------------------------------------------------------------
    # Specialised handlers
    # ------------------------------------------------------------------

    def _handle_zip_extract(self, zip_extract_list: list, source_key: str,
                            compress: str | None = None) -> tuple[int, int, list[str], int]:
        """Download a zip and extract only the listed files from it.

        Returns (downloaded, skipped, failed_filenames, samples_created).
        """
        downloaded = skipped = samples_created = 0
        failed_filenames: list[str] = []
        sub_dir = self.output_dir / source_key
        sub_dir.mkdir(parents=True, exist_ok=True)

        for entry in zip_extract_list:
            zip_url = entry.get('url')
            target_files = entry.get('files', [])
            if not zip_url:
                logger.warning("zip_extract entry missing 'url' — skipping")
                continue

            all_exist = all(
                (sub_dir / (f + '.gz' if compress == 'gzip' else f)).exists()
                for f in target_files
            )
            if all_exist:
                logger.info(f"Skipped {zip_url} (all target files already exist)")
                skipped += len(target_files)
                continue

            zip_filename = Path(urlparse(zip_url).path).name
            zip_path = sub_dir / zip_filename
            if not self._download_file(zip_url, zip_path):
                failed_filenames.append(zip_filename)
                continue

            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    available = zf.namelist()
                    for target in target_files:
                        match = next((n for n in available if n.endswith(target)), None)
                        if not match:
                            logger.warning(f"'{target}' not found in {zip_filename}. Available: {available}")
                            failed_filenames.append(target)
                            continue
                        extracted_path = sub_dir / target
                        with zf.open(match) as src, open(extracted_path, 'wb') as dst:
                            shutil.copyfileobj(src, dst)
                        logger.info(f"Extracted {target} from {zip_filename}")
                        if compress == 'gzip':
                            extracted_path = self._compress_gzip(extracted_path)
                        samples_created += self._create_sample(extracted_path)
                        downloaded += 1
            except zipfile.BadZipFile as e:
                logger.error(f"Bad zip file {zip_filename}: {e}")
                failed_filenames.append(zip_filename)
            finally:
                zip_path.unlink(missing_ok=True)

        return downloaded, skipped, failed_filenames, samples_created

    def _handle_directories(self, directories: dict,
                            source_key: str) -> tuple[int, int, list[str]]:
        """Scrape HTML directory listings and download every linked file.

        Returns (downloaded, skipped, failed_filenames).
        """
        downloaded = skipped = 0
        failed_filenames: list[str] = []

        for dir_name, dir_url in directories.items():
            dir_output = self.output_dir / source_key / dir_name
            logger.info(f"Scraping directory {dir_url} → {dir_output}")
            d, s, f = self._scrape_directory(dir_url, dir_output)
            downloaded += d; skipped += s; failed_filenames += f

        return downloaded, skipped, failed_filenames

    def _scrape_directory(self, dir_url: str,
                          output_dir: Path) -> tuple[int, int, list[str]]:
        """Fetch an HTML directory listing and download all linked files.

        Returns (downloaded, skipped, failed_filenames).
        """
        try:
            response = self.session.get(dir_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Could not fetch directory listing {dir_url}: {e}")
            return 0, 0, [dir_url]

        parser = _LinkParser()
        parser.feed(response.text)
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded = skipped = 0
        failed_filenames: list[str] = []

        for href in parser.links:
            if not href or href.startswith(('?', '#', '/')) or href in ('..', './') or href.endswith('/'):
                continue
            file_url = urljoin(dir_url, href)
            filename = Path(urlparse(file_url).path).name
            if not filename:
                continue
            output_path = output_dir / filename
            if self._already_downloaded(output_path, file_url):
                logger.info(f"Skipped {file_url} (already exists)")
                skipped += 1
                continue
            if self._download_file(file_url, output_path):
                downloaded += 1
            else:
                failed_filenames.append(filename)

        return downloaded, skipped, failed_filenames

    def _handle_enhancer_atlas(self, source_config: dict,
                               source_key: str) -> tuple[int, int, list[str], int]:
        """Handle EnhancerAtlas records with bed_url / ep_base_url / tissues.

        Disk layout:
            <output_dir>/<source_key>/bed/   — BED files extracted from the tarball
            <output_dir>/<source_key>/ep/    — one EP file per tissue

        The tarball is always downloaded so member sizes can be compared against
        local files; only changed or missing members are extracted.

        Returns (downloaded, skipped, failed_filenames, samples_created).
        """
        downloaded = skipped = samples_created = 0
        failed_filenames: list[str] = []

        source_dir = self.output_dir / source_key
        bed_dir = source_dir / 'bed'
        ep_dir  = source_dir / 'ep'
        bed_dir.mkdir(parents=True, exist_ok=True)
        ep_dir.mkdir(parents=True, exist_ok=True)

        # ---- BED tarball ------------------------------------------------
        bed_url = source_config.get('bed_url')
        if bed_url:
            tar_filename = Path(urlparse(bed_url).path).name
            tar_path = source_dir / tar_filename

            if self._download_file(bed_url, tar_path):
                try:
                    with tarfile.open(tar_path, 'r:gz') as tf:
                        members = [
                            m for m in tf.getmembers()
                            if m.isfile() and not Path(m.name).name.startswith('.')
                        ]
                        # Extract only new or changed members (size comparison)
                        to_extract = []
                        for member in members:
                            flat_name = Path(member.name).name
                            local_path = bed_dir / flat_name
                            if local_path.exists() and local_path.stat().st_size == member.size:
                                skipped += 1
                            else:
                                to_extract.append(member)

                        if to_extract:
                            for member in to_extract:
                                member.name = Path(member.name).name  # flatten subdirs
                                tf.extract(member, path=bed_dir)
                            logger.info(
                                f"Extracted {len(to_extract)} BED files to {bed_dir} "
                                f"({len(members) - len(to_extract)} unchanged)"
                            )
                            downloaded += len(to_extract)
                            for member in to_extract:
                                fp = bed_dir / member.name
                                if fp.exists():
                                    samples_created += self._create_sample(fp)
                        else:
                            logger.info(f"Skipped all {len(members)} BED files (sizes unchanged)")

                except tarfile.TarError as e:
                    logger.error(f"Failed to extract {tar_filename}: {e}")
                    failed_filenames.append(tar_filename)
                finally:
                    tar_path.unlink(missing_ok=True)
            else:
                failed_filenames.append(tar_filename)

        # ---- EP files ---------------------------------------------------
        ep_base_url = source_config.get('ep_base_url', '').rstrip('/')
        tissues = source_config.get('tissues', [])

        if ep_base_url and tissues:
            for tissue in tissues:
                ep_filename = f"{tissue}_EP.bed"
                ep_url = f"{ep_base_url}/{tissue}/{ep_filename}"
                ep_path = ep_dir / ep_filename

                if self._already_downloaded(ep_path, ep_url):
                    logger.info(f"Skipped {ep_url} (already exists)")
                    skipped += 1
                    continue

                if self._download_file(ep_url, ep_path):
                    logger.info(f"Downloaded {ep_url}")
                    downloaded += 1
                    samples_created += self._create_sample(ep_path)
                else:
                    failed_filenames.append(ep_filename)

        return downloaded, skipped, failed_filenames, samples_created

    def _handle_gcp(self, source_config: dict, source_key: str) -> tuple[int, int, list[str], int]:
        """Download a file from Google Cloud Storage.

        Returns (downloaded, skipped, failed_filenames, samples_created).
        """
        from google.cloud import storage as gcs

        bucket_name = source_config['bucket']
        path = source_config['path']
        filename = path.split('/')[-1]
        save_dir = self.output_dir / source_key
        save_dir.mkdir(parents=True, exist_ok=True)
        filepath = save_dir / filename

        if filepath.exists() and filepath.stat().st_size > 0:
            logger.info(f"Skipped {filename} (already exists)")
            return 0, 1, [], 0

        try:
            storage_client = gcs.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(path)
            logger.info(f"Downloading from GCS: {bucket_name}/{path}")
            blob.download_to_filename(filepath)
            sc = self._create_sample(filepath)
            return 1, 0, [], sc
        except Exception as e:
            logger.error(f"GCS download failed for {bucket_name}/{path}: {e}")
            return 0, 0, [filename], 0

    def _handle_roadmap(self, source_config: dict, source_key: str) -> tuple[int, int, list[str], int]:
        """Download Roadmap Epigenomics imputed mark files E001–E129.

        Returns (downloaded, skipped, failed_filenames, samples_created).
        """
        SKIP_IDS = {60, 64}
        root_url = source_config['url'].rstrip('/')
        save_dir = self.output_dir / source_key
        save_dir.mkdir(parents=True, exist_ok=True)

        downloaded = skipped = samples_created = 0
        failed_filenames: list[str] = []

        for i in range(1, 130):
            if i in SKIP_IDS:
                continue
            filename = f"E{i:03d}_25_imputed12marks_mnemonics.bed.gz"
            url = f"{root_url}/{filename}"
            filepath = save_dir / filename

            if self._already_downloaded(filepath, url):
                logger.info(f"Skipped {filename} (already exists)")
                skipped += 1
                continue

            if self._download_file(url, filepath):
                downloaded += 1
                samples_created += self._create_sample(filepath)
            else:
                failed_filenames.append(filename)

        return downloaded, skipped, failed_filenames, samples_created

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def process_download(self, source_id: str,
                         source_config: dict) -> tuple[int, int, list[str], int]:
        """Dispatch a single source to the appropriate handler.

        Returns (downloaded, skipped, failed_filenames, samples_created).
        """
        logger.info(f"Downloading {source_id} ({source_config.get('name', '')}) ...")

        extract  = source_config.get('extract', True)
        compress = source_config.get('compress', None)
        # move_to can be either a flat dict {filename: dest} or a list of
        # single-key dicts [{filename: dest}, ...] (both are valid YAML styles).
        # Normalise to a flat dict so the rest of the code can always call .get().
        _raw_move_to = source_config.get('move_to', {})
        if isinstance(_raw_move_to, list):
            move_to = {k: v for entry in _raw_move_to for k, v in entry.items()}
        else:
            move_to = _raw_move_to or {}

        downloaded = skipped = samples_created = 0
        failed_filenames: list[str] = []
        has_handled = False

        if source_id == 'roadmap':
            d, s, f, sc = self._handle_roadmap(source_config, source_id)
            downloaded += d; skipped += s; failed_filenames += f; samples_created += sc
            has_handled = True

        elif 'bucket' in source_config:
            d, s, f, sc = self._handle_gcp(source_config, source_id)
            downloaded += d; skipped += s; failed_filenames += f; samples_created += sc
            has_handled = True

        else:
            if 'url' in source_config:
                d, s, f, sc = self._process_urls(source_config['url'], source_id,
                                                 extract=extract, compress=compress,
                                                 move_to=move_to)
                downloaded += d; skipped += s; failed_filenames += f; samples_created += sc
                has_handled = True

            if 'directories' in source_config:
                d, s, f = self._handle_directories(source_config['directories'], source_id)
                downloaded += d; skipped += s; failed_filenames += f
                has_handled = True

            if 'zip_extract' in source_config:
                d, s, f, sc = self._handle_zip_extract(source_config['zip_extract'], source_id,
                                                       compress=compress)
                downloaded += d; skipped += s; failed_filenames += f; samples_created += sc
                has_handled = True

            if 'bed_url' in source_config or 'ep_base_url' in source_config:
                d, s, f, sc = self._handle_enhancer_atlas(source_config, source_id)
                downloaded += d; skipped += s; failed_filenames += f; samples_created += sc
                has_handled = True

        if not has_handled:
            logger.warning(f"No recognised download key found for '{source_id}' — skipping")
            return 0, 0, [], 0

        # Backfill samples for files skipped (already downloaded) this run
        samples_created += self._ensure_samples(source_id)

        if samples_created:
            logger.info(f"Samples created for {source_id}")

        return downloaded, skipped, failed_filenames, samples_created

    def download_source(self, source_id: str):
        """Download a single source by key."""
        if source_id not in self.config:
            raise ValueError(f"Source '{source_id}' not found in config")
        self.process_download(source_id, self.config[source_id])

    def download_all(self):
        """Download all sources defined in the config, then print a grouped error report."""
        total_downloaded = total_skipped = 0
        failed_by_source: dict[str, list[str]] = {}

        for source_id, source_config in self.config.items():
            if source_id == 'name' or not isinstance(source_config, dict):
                continue
            d, s, f, _ = self.process_download(source_id, source_config)
            total_downloaded += d
            total_skipped    += s
            if f:
                failed_by_source[source_id] = f

        n_failed = sum(len(v) for v in failed_by_source.values())
        summary = (
            f"Download complete: {total_downloaded} downloaded, "
            f"{total_skipped} skipped, {n_failed} failed"
        )

        if failed_by_source:
            logger.error(summary)
            lines = ["\nError report:"]
            for source_id, filenames in failed_by_source.items():
                lines.append(f"{source_id}:")
                for name in filenames:
                    lines.append(f"        {name}")
            logger.error("\n".join(lines))
        else:
            logger.info(summary)
