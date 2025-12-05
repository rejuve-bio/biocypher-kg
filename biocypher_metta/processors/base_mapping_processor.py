"""
Base class for mapping processors that handle ID mappings between different data sources.

This module provides a common interface for processors that maintain mappings between
different biological identifiers (e.g., Ensembl to UniProt, Entrez to Ensembl, etc.).
Supports both time-based and dependency-based update strategies, with smart remote
version checking to avoid unnecessary downloads.
"""

import pickle
import os
import json
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path


class BaseMappingProcessor(ABC):

    def __init__(
        self,
        name: str,
        cache_dir: str = 'mapping_data',
        update_interval_hours: Optional[int] = None,
        dependency_file: Optional[str] = None
    ):
        self.name = name
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.mapping_file = self.cache_dir / f"{name}_mapping.pkl"
        self.version_file = self.cache_dir / f"{name}_version.json"

        self.update_interval = timedelta(hours=update_interval_hours) if update_interval_hours else None
        self.dependency_file = Path(dependency_file) if dependency_file else None

        self.mapping: Dict[str, Any] = {}
        self.last_update_check: Optional[datetime] = None
        self.last_check_result: Optional[bool] = None

    @abstractmethod
    def fetch_data(self) -> Any:
        pass

    @abstractmethod
    def process_data(self, raw_data: Any) -> Dict[str, Any]:
        pass

    def get_remote_urls(self) -> Optional[List[str]]:
        return None

    def check_remote_version(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            response = requests.head(url, timeout=10, allow_redirects=True)
            response.raise_for_status()

            metadata = {
                'url': url,
                'last_modified': response.headers.get('Last-Modified'),
                'etag': response.headers.get('ETag'),
                'content_length': response.headers.get('Content-Length'),
                'checked_at': datetime.now().isoformat()
            }

            return metadata
        except Exception as e:
            print(f"{self.name}: Could not check remote version for {url}: {e}")
            return None

    def has_remote_update(self) -> Optional[bool]:
        urls = self.get_remote_urls()
        if not urls:
            return None

        version_info = self._load_version_info()
        if not version_info or 'remote_metadata' not in version_info:
            print(f"{self.name}: No previous remote metadata, assuming update needed")
            return True

        previous_metadata = version_info.get('remote_metadata', {})

        has_valid_metadata = False
        for url in urls:
            current_metadata = self.check_remote_version(url)
            if not current_metadata:
                continue

            prev_meta = previous_metadata.get(url, {})

            if current_metadata.get('last_modified') and prev_meta.get('last_modified'):
                has_valid_metadata = True
                if current_metadata['last_modified'] != prev_meta['last_modified']:
                    print(f"{self.name}: Remote file updated (Last-Modified changed)")
                    return True

            if current_metadata.get('etag') and prev_meta.get('etag'):
                has_valid_metadata = True
                if current_metadata['etag'] != prev_meta['etag']:
                    print(f"{self.name}: Remote file updated (ETag changed)")
                    return True

            if current_metadata.get('content_length') and prev_meta.get('content_length'):
                has_valid_metadata = True
                if current_metadata['content_length'] != prev_meta['content_length']:
                    print(f"{self.name}: Remote file updated (size changed)")
                    return True

        if not has_valid_metadata:
            print(f"{self.name}: No valid remote metadata available for comparison")
            return None

        print(f"{self.name}: No remote updates detected")
        return False

    def check_update_needed(self) -> bool:
        current_time = datetime.now()

        if (self.last_update_check and
            (current_time - self.last_update_check) < timedelta(minutes=5)):
            return self.last_check_result

        self.last_update_check = current_time

        if not self.mapping_file.exists() or not self.version_file.exists():
            print(f"{self.name}: Mapping or version file not found. Update needed.")
            self.last_check_result = True
            return True

        version_info = self._load_version_info()
        if not version_info:
            print(f"{self.name}: Invalid version file. Update needed.")
            self.last_check_result = True
            return True

        remote_update = self.has_remote_update()
        if remote_update is True:
            self.last_check_result = True
            return True
        elif remote_update is False:
            print(f"{self.name}: Remote source unchanged. No update needed.")
            self.last_check_result = False
            return False

        if self.dependency_file and self.dependency_file.exists():
            dep_mtime = datetime.fromtimestamp(self.dependency_file.stat().st_mtime)
            mapping_time = datetime.fromisoformat(version_info['timestamp'])

            if dep_mtime > mapping_time:
                print(f"{self.name}: Dependency file is newer. Update needed.")
                self.last_check_result = True
                return True

        if self.update_interval:
            last_update = datetime.fromisoformat(version_info['timestamp'])
            time_since_update = current_time - last_update

            if time_since_update > self.update_interval:
                days = time_since_update.days
                hours = time_since_update.seconds // 3600
                print(f"{self.name}: Last updated {days} days, {hours} hours ago. Update needed (time-based fallback).")
                self.last_check_result = True
                return True
            else:
                print(f"{self.name}: Last updated recently. No update needed.")
                self.last_check_result = False
                return False

        self.last_check_result = False
        return False

    def update_mapping(self, force: bool = False) -> bool:
        if not force and not self.check_update_needed():
            if self.mapping_file.exists():
                print(f"{self.name}: Using existing mapping.")
                self.load_mapping()
                return True

        print(f"{self.name}: Updating mapping...")

        try:
            raw_data = self.fetch_data()
            self.mapping = self.process_data(raw_data)

            self.save_mapping()
            self.save_version_info()

            print(f"{self.name}: Successfully updated mapping with {len(self.mapping)} entries.")
            return True

        except Exception as e:
            print(f"{self.name}: Error during update: {e}")

            if self.mapping_file.exists():
                print(f"{self.name}: Falling back to cached mapping.")
                self.load_mapping()
                return True
            else:
                print(f"{self.name}: No cached mapping available. Cannot proceed.")
                return False

    def save_mapping(self):
        import gzip
        with gzip.open(self.mapping_file, 'wb') as f:
            pickle.dump(self.mapping, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"{self.name}: Saved compressed mapping to {self.mapping_file}")

    def load_mapping(self) -> Dict[str, Any]:
        import gzip
        try:
            with gzip.open(self.mapping_file, 'rb') as f:
                self.mapping = pickle.load(f)
        except (OSError, gzip.BadGzipFile):
            print(f"{self.name}: Loading uncompressed pickle file...")
            with open(self.mapping_file, 'rb') as f:
                self.mapping = pickle.load(f)
            print(f"{self.name}: Re-saving as compressed file...")
            self.save_mapping()

        print(f"{self.name}: Loaded mapping from {self.mapping_file} ({len(self.mapping)} entries)")

        version_info = self._load_version_info()
        if version_info and 'timestamp' in version_info:
            try:
                timestamp = datetime.fromisoformat(version_info['timestamp'])
                updated_at = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                updated_at = version_info.get('timestamp', 'Unknown')
            print(f"{self.name}: Cache last updated: {updated_at}")

        return self.mapping

    def save_version_info(self):
        total_entries = 0
        if isinstance(self.mapping, dict):
            all_values_are_dicts = all(isinstance(v, dict) for v in self.mapping.values())
            if all_values_are_dicts and len(self.mapping) > 0:
                total_entries = sum(len(v) for v in self.mapping.values())
            else:
                total_entries = len(self.mapping)
        else:
            total_entries = len(self.mapping) if hasattr(self.mapping, '__len__') else 0

        version_info = {
            'timestamp': datetime.now().isoformat(),
            'processor': self.name,
            'entries': total_entries
        }

        if self.dependency_file and self.dependency_file.exists():
            version_info['dependency_file'] = str(self.dependency_file)
            version_info['dependency_mtime'] = datetime.fromtimestamp(
                self.dependency_file.stat().st_mtime
            ).isoformat()

        urls = self.get_remote_urls()
        if urls:
            remote_metadata = {}
            for url in urls:
                metadata = self.check_remote_version(url)
                if metadata:
                    remote_metadata[url] = metadata
            if remote_metadata:
                version_info['remote_metadata'] = remote_metadata

        with open(self.version_file, 'w') as f:
            json.dump(version_info, f, indent=2)

        print(f"{self.name}: Saved version info to {self.version_file}")

    def _load_version_info(self) -> Optional[Dict[str, Any]]:
        try:
            with open(self.version_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"{self.name}: Error loading version file: {e}")
            return None

    def get_version_info(self, check_remote: bool = False) -> Dict[str, Any]:
        version_data = {
            'processor': self.name,
            'cached_version': None,
            'remote_version': None,
            'update_available': None
        }

        cached_info = self._load_version_info()
        if cached_info:
            version_data['cached_version'] = {
                'updated_at': cached_info.get('updated_at'),
                'entry_count': cached_info.get('entry_count'),
                'remote_metadata': cached_info.get('remote_metadata', {})
            }

        if check_remote:
            urls = self.get_remote_urls()
            if urls:
                remote_meta = {}
                for url in urls:
                    metadata = self.check_remote_version(url)
                    if metadata:
                        remote_meta[url] = metadata

                version_data['remote_version'] = remote_meta

                version_data['update_available'] = self.has_remote_update()

        return version_data

    def display_version_info(self, check_remote: bool = False):
        print(f"\n{'='*60}")
        print(f"📊 {self.name.upper()} Processor Version Info")
        print(f"{'='*60}")

        version_info = self.get_version_info(check_remote=check_remote)

        cached = version_info.get('cached_version')
        if cached:
            print(f"\n📦 Cached Data:")
            print(f"  Last Updated: {cached.get('updated_at', 'Unknown')}")
            print(f"  Entry Count: {cached.get('entry_count', 'Unknown'):,}" if cached.get('entry_count') else "  Entry Count: Unknown")

            remote_meta = cached.get('remote_metadata', {})
            if remote_meta:
                print(f"\n  Source File Metadata:")
                for url, meta in remote_meta.items():
                    print(f"    URL: {url}")
                    if meta.get('last_modified'):
                        print(f"    Last Modified: {meta['last_modified']}")
                    if meta.get('content_length'):
                        size_mb = int(meta['content_length']) / (1024 * 1024)
                        print(f"    Size: {size_mb:.1f} MB")
        else:
            print(f"\n📦 Cached Data: None (not yet downloaded)")

        if check_remote and version_info.get('remote_version'):
            print(f"\n🌐 Remote Source:")
            for url, meta in version_info['remote_version'].items():
                print(f"  URL: {url}")
                if meta.get('last_modified'):
                    print(f"  Current Last Modified: {meta['last_modified']}")
                if meta.get('content_length'):
                    size_mb = int(meta['content_length']) / (1024 * 1024)
                    print(f"  Current Size: {size_mb:.1f} MB")

            update_available = version_info.get('update_available')
            if update_available is not None:
                if update_available:
                    print(f"\n✅ Update Available: Yes")
                else:
                    print(f"\n✅ Update Available: No (cache is up-to-date)")
        elif check_remote:
            print(f"\n🌐 Remote Source: Could not check (no URLs configured)")

        print(f"\n{'='*60}\n")

    def get_mapping(self, key: str, default: Any = None) -> Any:
        return self.mapping.get(key, default)

    def load_or_update(self, force: bool = False) -> Dict[str, Any]:
        self.update_mapping(force=force)
        return self.mapping