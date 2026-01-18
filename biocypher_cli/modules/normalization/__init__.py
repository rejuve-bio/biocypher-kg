"""Normalization utilities.

This package centralizes file normalization used across sample preparation and
download/sampling scripts.
"""

from .file_normalization import detect_file_format, normalize_file_format
from .rnacentral import normalize_rnacentral_rfam

__all__ = [
    "detect_file_format",
    "normalize_file_format",
    "normalize_rnacentral_rfam",
]
