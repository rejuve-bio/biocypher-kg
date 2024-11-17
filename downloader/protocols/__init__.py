from .base import Downloader
from .http import HTTPDownloader
# from .ftp import FTPDownloader
# from .gcp import GCPDownloader
# from .local import LocalDownloader

__all__ = [
    'Downloader',
    'HTTPDownloader',
    # 'FTPDownloader',
    # 'GCPDownloader',
    'LocalDownloader'
]