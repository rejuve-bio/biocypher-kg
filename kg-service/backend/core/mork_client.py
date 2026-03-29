"""
MORK Client for API
Wraps MORK operations for API endpoints
"""

import json
from pathlib import Path
import sys

# Add MORK client to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "biocypher-kg" / "biocypher-mork"))
from client import MORK


class MORKClient:
    """Client for MORK database operations"""
    
    def __init__(self, mork_url: str = "http://localhost:8432"):
        self.mork_url = mork_url
        self.server = MORK(mork_url)
        self.metadata_file = Path("/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/mork/version_metadata.json")
    
    def get_latest_version(self):
        """Get latest version from metadata file"""
        if not self.metadata_file.exists():
            return None
        
        try:
            with open(self.metadata_file, 'r') as f:
                data = json.load(f)
            
            return {
                "version": data.get("atomspace_version"),
                "build_id": data.get("build_id"),
                "timestamp": data.get("build_timestamp"),
                "datasets": data.get("dataset_versions", {})
            }
        except Exception as e:
            return None
    
    def get_all_versions(self):
        """Get all versions from archives"""
        archive_dir = Path("/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/mork")
        
        if not archive_dir.exists():
            return []
        
        versions = set()
        for dataset_dir in archive_dir.iterdir():
            if dataset_dir.is_dir() and dataset_dir.name != "version_metadata.json":
                for version_dir in dataset_dir.iterdir():
                    if version_dir.is_dir():
                        versions.add(version_dir.name)
        
        # Sort versions (v1, v2, v3, ...)
        sorted_versions = sorted(versions, key=lambda x: int(x.replace('v', '')))
        return sorted_versions
    
    def get_dataset_count(self):
        """Get number of datasets in latest version"""
        latest = self.get_latest_version()
        if latest and latest.get("datasets"):
            return len(latest["datasets"])
        return 0
    
    def get_atom_count(self):
        """Get approximate atom count from MORK"""
        try:
            with self.server.work_at("annotation") as scope:
                data = scope.download_(max_results=1000)
                data.block()
                if data.data:
                    lines = [l for l in data.data.split('\n') if l.strip() and not l.strip().startswith(';')]
                    return len(lines)
            return 0
        except:
            return 0
    
    def compare_versions(self, version1: str, version2: str, dataset: str = None):
        """Compare two versions"""
        # This will use the version_diff.py logic
        # For now, return basic info
        return {
            "version1": version1,
            "version2": version2,
            "dataset": dataset,
            "message": "Comparison available via version_diff.py CLI"
        }
    
    def get_archives(self):
        """List all archived datasets"""
        archive_dir = Path("/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/mork")
        
        if not archive_dir.exists():
            return []
        
        archives = []
        for dataset_dir in archive_dir.iterdir():
            if dataset_dir.is_dir() and dataset_dir.name != "version_metadata.json":
                dataset_versions = []
                for version_dir in dataset_dir.iterdir():
                    if version_dir.is_dir():
                        # Count files
                        file_count = len(list(version_dir.rglob("*.metta")))
                        dataset_versions.append({
                            "version": version_dir.name,
                            "file_count": file_count,
                            "path": str(version_dir)
                        })
                
                # Sort by version
                dataset_versions.sort(key=lambda x: int(x['version'].replace('v', '')))
                
                archives.append({
                    "dataset": dataset_dir.name,
                    "versions": dataset_versions
                })
        
        return archives