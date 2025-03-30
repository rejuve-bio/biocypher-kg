import yaml
from pathlib import Path
import requests
import shutil
from tqdm import tqdm
from google.cloud import storage
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class DownloadManager:
    def __init__(self, config_path: str, output_dir: Path):
        self.config = self._load_config(config_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _load_config(self, config_path: str):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def download_file(self, url: str, filepath: Path, verify: bool = True, max_retries: int = 3):
        """Generic file download with progress bar and retries"""
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, stream=True, allow_redirects=True, verify=verify)
                r.raise_for_status()
                
                file_size = int(r.headers.get("Content-Length", 0))
                desc = f"Downloading {filepath.name}"
                
                with tqdm(total=file_size, unit='iB', unit_scale=True, desc=desc) as pbar:
                    with filepath.open("wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            size = f.write(chunk)
                            pbar.update(size)
                return True
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download {url} after {max_retries} attempts")
                    raise

    def process_download(self, source_id: str, source_config: dict):
        """Process download based on source configuration"""
        logger.info(f"Downloading from {source_config['name']} .....")
        save_dir = self.output_dir / source_id
        save_dir.mkdir(parents=True, exist_ok=True)

        try:
            if source_id == 'roadmap':
                self._handle_roadmap_download(source_config, save_dir)
            elif 'bucket' in source_config:
                self._handle_gcp_download(source_config, save_dir)
            elif isinstance(source_config.get('url'), (list, dict)):
                self._handle_multiple_urls(source_config, save_dir)
            else:
                self._handle_single_url(source_config, save_dir)
        except Exception as e:
            logger.error(f"Error downloading {source_id}: {str(e)}")
            raise
    def _handle_single_url(self, source_config: dict, save_dir: Path):
        """Handle single URL downloads"""
        url = source_config['url']
        filename = url.split('/')[-1]
        if not filename:  # Handle URLs without filename
            filename = 'download.txt'
        filepath = save_dir / filename
        verify = True  # Default to verify SSL
        if 'tadmap' in str(save_dir):  # Special case for tadmap
            verify = False
        self.download_file(url, filepath, verify=verify)

    def _handle_multiple_urls(self, source_config: dict, save_dir: Path):
        """Handle multiple URL downloads"""
        urls = source_config['url']
        if isinstance(urls, list):
            for url in urls:
                filename = url.split('/')[-1]
                filepath = save_dir / filename
                self.download_file(url, filepath)
        elif isinstance(urls, dict):
            for key, url in urls.items():
                filename = url.split('/')[-1]
                if not filename:
                    filename = f"{key}.txt"
                filepath = save_dir / filename
                self.download_file(url, filepath)

    def _handle_gcp_download(self, source_config: dict, save_dir: Path):
        """Handle Google Cloud Storage downloads"""
        bucket_name = source_config['bucket']
        path = source_config['path']
        filename = path.split('/')[-1]
        filepath = save_dir / filename
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(path)
        
        logger.info(f"Downloading from GCS: {bucket_name}/{path}")
        blob.download_to_filename(filepath)
    def _handle_roadmap_download(self, config: dict, save_dir: Path):
        """Special handler for roadmap downloads"""
        root_url = config['url']
        for i in range(1, 130):
            if i in [60, 64]:  # Skip missing files
                continue
            file_name = f"E{i:03d}_25_imputed12marks_mnemonics.bed.gz"
            url = f"{root_url}/{file_name}"
            filepath = save_dir / file_name
            try:
                self.download_file(url, filepath)
            except Exception as e:
                logger.warning(f"Failed to download {file_name}: {str(e)}")

    def download_source(self, source_id: str):
        """Download a specific source"""
        if source_id in self.config:
            self.process_download(source_id, self.config[source_id])
        else:
            raise ValueError(f"Source {source_id} not found in config")

    def download_all(self):
        """Download all sources"""
        failed_sources = []
        for source_id in self.config:
            if source_id != 'name':
                try:
                    self.download_source(source_id)
                except Exception as e:
                    logger.error(f"Failed to download {source_id}: {str(e)}")
                    failed_sources.append(source_id)
        
        if failed_sources:
            logger.error(f"Failed to download the following sources: {', '.join(failed_sources)}")