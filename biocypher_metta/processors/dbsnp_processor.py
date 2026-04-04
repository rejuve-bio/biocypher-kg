"""
dbSNP Processor for rsID to Genomic Position Mappings.

LOAD-ONLY: This processor only loads pre-existing cache files.
Updates are handled by the separate update_dbsnp.py script.

Maintains bidirectional mappings between dbSNP rsIDs and genomic positions.
"""

import pickle
import gzip
from pathlib import Path
from typing import Dict, Any, Optional

from biocypher._logger import logger


class DBSNPProcessor:

    def __init__(self, cache_dir: str = 'aux_files/hsa/sample_dbsnp'):
        self.name = 'dbsnp'
        self.cache_dir = Path(cache_dir)
        self.mapping_file = self.cache_dir / 'dbsnp_mapping.pkl'
        self.version_file = self.cache_dir / 'dbsnp_version.json'
        self.mapping: Dict[str, Any] = {}

    def load_mapping(self) -> Dict[str, Any]:
        """Load mapping from cache file (compressed pickle)"""
        if not self.mapping_file.exists():
            raise FileNotFoundError(
                f"{self.name}: Cache file not found: {self.mapping_file}\n"
                f"Run 'python update_dbsnp.py' to create the cache."
            )

        try:
            # Try gzip-compressed first
            with gzip.open(self.mapping_file, 'rb') as f:
                self.mapping = pickle.load(f)
        except (OSError, gzip.BadGzipFile):
            # Fall back to uncompressed
            logger.info(f"{self.name}: Loading uncompressed pickle file...")
            with open(self.mapping_file, 'rb') as f:
                self.mapping = pickle.load(f)

        logger.info(f"{self.name}: Loaded mapping from {self.mapping_file}")

        # Show version info if available
        if self.version_file.exists():
            import json
            try:
                with open(self.version_file, 'r') as f:
                    version_info = json.load(f)
                if 'timestamp' in version_info:
                    from datetime import datetime
                    try:
                        timestamp = datetime.fromisoformat(version_info['timestamp'])
                        updated_at = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    except (ValueError, TypeError):
                        updated_at = version_info.get('timestamp', 'Unknown')
                    logger.info(f"{self.name}: Cache last updated: {updated_at}")
            except:
                pass

        return self.mapping

    def get_position(self, rsid: str) -> Optional[Dict[str, Any]]:
        """Get genomic position for an rsID"""
        if not self.mapping:
            if self.mapping_file.exists():
                self.load_mapping()
            else:
                return None

        if self._is_nested_format():
            rsid_to_pos = self.mapping.get('rsid_to_pos', {})
        else:
            rsid_to_pos = self.mapping
        return rsid_to_pos.get(rsid)

    def get_rsid(self, chrom: str, pos: int) -> Optional[str]:
        """Get rsID for a genomic position"""
        if not self.mapping:
            if self.mapping_file.exists():
                self.load_mapping()
            else:
                return None

        if self._is_nested_format():
            pos_to_rsid = self.mapping.get('pos_to_rsid', {})
        else:
            # Legacy format doesn't have pos_to_rsid
            return None

        # Try with provided chromosome format
        pos_key = f"{chrom}:{pos}"
        rsid = pos_to_rsid.get(pos_key)

        if rsid:
            return rsid

        # Try alternative format (with/without 'chr' prefix)
        if not chrom.startswith('chr'):
            pos_key = f"chr{chrom}:{pos}"
            return pos_to_rsid.get(pos_key)

        return None

    def _is_nested_format(self) -> bool:
        """Check if mapping uses nested format with rsid_to_pos/pos_to_rsid keys."""
        return 'rsid_to_pos' in self.mapping or 'pos_to_rsid' in self.mapping

    def get_dict_wrappers(self):
        if not self.mapping:
            raise RuntimeError(
                f"{self.name}: Mapping not loaded. "
                "Call load_mapping() first."
            )

        if self._is_nested_format():
            # New format: {"rsid_to_pos": {...}, "pos_to_rsid": {...}}
            return (
                self.mapping.get('rsid_to_pos', {}),
                self.mapping.get('pos_to_rsid', {})
            )
        else:
            # Legacy format: flat dict {rsid: {"chr": ..., "pos": ...}}
            # Treat the whole mapping as rsid_to_pos; pos_to_rsid not available
            logger.info(f"{self.name}: Detected legacy flat format, using as rsid_to_pos")
            return (self.mapping, {})
