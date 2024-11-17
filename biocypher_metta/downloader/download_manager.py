import yaml
from pathlib import Path
from typing import Dict, Any
from .protocols.http import HTTPDownloader
import logging

logger = logging.getLogger(__name__)

class DownloadManager:
    def __init__(self, config_path: str, output_dir: Path):
        self.config = self._load_config(config_path)
        self.output_dir = output_dir
        self.downloaders = {
            'http': HTTPDownloader(),
            'https': HTTPDownloader()
        }
        logger.info(f"Initialized DownloadManager with config from {config_path}")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load the download configuration from a YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info(f"Successfully loaded config from {config_path}")
                return config
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {str(e)}")
            raise

    def download_source(self, source_id: str):
        """Download a specific source."""
        logger.info(f"Starting download for source: {source_id}")
        source_config = self.config['downloads'][source_id]
        
        for file_config in source_config['files']:
            try:
                method = file_config.get('method', 'http')
                url = file_config['url']
                dest = self.output_dir / file_config['dest'] / file_config['local_name']
                dest.parent.mkdir(parents=True, exist_ok=True)
                
                logger.info(f"Downloading {url} to {dest}")
                self.downloaders[method].download(
                    url=url,
                    dest=dest,
                    params=file_config.get('params')
                )
                logger.info(f"Successfully downloaded {url}")
            except Exception as e:
                logger.error(f"Error downloading {url}: {str(e)}")
                raise

    def download_all(self):
        """Download all sources."""
        logger.info("Starting download of all sources")
        for source_id in self.config['downloads']:
            try:
                self.download_source(source_id)
            except Exception as e:
                logger.error(f"Failed to download source {source_id}: {str(e)}")
                raise