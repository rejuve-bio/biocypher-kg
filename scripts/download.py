from abc import ABC, abstractmethod
import pathlib
import requests
from google.cloud import storage
import ftplib
import yaml
from typing import Dict, Any
import logging

class Downloader(ABC):
    @abstractmethod
    def download(self, url: str, dest_path: pathlib.Path) -> None:
        pass

class HTTPDownloader(Downloader):
    def download(self, url: str, dest_path: pathlib.Path) -> None:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

class FTPDownloader(Downloader):
    def download(self, url: str, dest_path: pathlib.Path) -> None:
        # FTP download implementation
        pass

class GCPDownloader(Downloader):
    def download(self, url: str, dest_path: pathlib.Path) -> None:
        # Google Cloud Storage download implementation
        pass

class DownloadManager:
    def __init__(self, config_path: str, output_dir: pathlib.Path):
        self.config = self._load_config(config_path)
        self.output_dir = output_dir
        self.downloaders = {
            'http': HTTPDownloader(),
            'https': HTTPDownloader(),
            'ftp': FTPDownloader(),
            'gcp': GCPDownloader()
        }

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        with open(config_path) as f:
            return yaml.safe_load(f)

    def download_all(self):
        for source_id, source_config in self.config['downloads'].items():
            self.download_source(source_id, source_config)

    def download_source(self, source_id: str, source_config: Dict[str, Any]):
        for file_config in source_config['files']:
            protocol = file_config['protocol']
            url = file_config['url']
            dest_path = self.output_dir / file_config['dest_path'] / file_config['local_name']
            
            # Handle parameterized downloads
            if 'params' in file_config:
                self._handle_parameterized_download(file_config, dest_path)
            else:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                self.downloaders[protocol].download(url, dest_path)

    def _handle_parameterized_download(self, file_config: Dict[str, Any], dest_path: pathlib.Path):
        # Handle downloads with parameters like ranges
        pass