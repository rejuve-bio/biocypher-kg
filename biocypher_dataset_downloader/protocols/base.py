from abc import ABC, abstractmethod
from pathlib import Path

class Downloader(ABC):
    """Base class for all download protocols"""
    
    @abstractmethod
    def download(self, url: str, dest: Path, params: dict = None) -> None:
        """Download a file from url to dest"""
        pass