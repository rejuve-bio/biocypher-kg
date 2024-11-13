import requests
from pathlib import Path
from .base import Downloader
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

class HTTPDownloader(Downloader):
    def download(self, url: str, dest: Path, params: dict = None) -> None:
        headers = params.get('headers', {}) if params else {}
        verify = params.get('verify', True) if params else True
        
        logger.info(f"Downloading {url}")
        response = requests.get(url, headers=headers, verify=verify, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 Kibibyte
        
        with open(dest, 'wb') as f:
            with tqdm(total=total_size, unit='iB', unit_scale=True, desc=dest.name) as pbar:
                for data in response.iter_content(block_size):
                    size = f.write(data)
                    pbar.update(size)