"""
Mapping processors for BioCypher MeTTa.

This package contains processors that maintain mappings between different
biological identifiers and data sources.

All processors inherit from BaseMappingProcessor and provide:
- Automatic update checking (time-based or dependency-based)
- Caching with pickle files
- Version tracking
- Graceful fallback to cached data
"""

from .base_mapping_processor import BaseMappingProcessor
from .hgnc_processor import HGNCProcessor
from .entrez_ensembl_processor import EntrezEnsemblProcessor
from .ensembl_uniprot_processor import EnsemblUniProtProcessor
from .go_subontology_processor import GOSubontologyProcessor
from .dbsnp_processor import DBSNPProcessor

__all__ = [
    'BaseMappingProcessor',
    'HGNCProcessor',
    'EntrezEnsemblProcessor',
    'EnsemblUniProtProcessor',
    'GOSubontologyProcessor',
    'DBSNPProcessor',
]
