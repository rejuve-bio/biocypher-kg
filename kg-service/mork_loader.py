#!/usr/bin/env python3
"""MORK loader with version management. Metadata stored in JSON."""
import os
import sys
import hashlib
import shutil
import json
from pathlib import Path
from datetime import datetime
import argparse

sys.path.insert(0, str(Path(__file__).parent / "biocypher-mork"))
from client import MORK


class MORKVersionManager:
    """Version management for MORK - stores metadata in JSON file"""
    
    def __init__(self, server, data_dir, archive_dir="/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives"):
        self.server = server
        self.data_dir = Path(data_dir)
        self.archive_dir = Path(archive_dir) / "mork"
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        
        # Metadata file location
        self.metadata_file = self.archive_dir / "version_metadata.json"
    
    def hash_all_datasets(self):
        """Calculate MD5 hash for each dataset folder"""
        hashes = {}
        
        for dataset_path in sorted(self.data_dir.iterdir()):
            if not dataset_path.is_dir():
                continue
            
            dataset_name = dataset_path.name
            all_files = sorted(dataset_path.rglob("*.metta"))
            
            if not all_files:
                continue
            
            hasher = hashlib.md5()
            for file_path in all_files:
                with open(file_path, 'rb') as f:
                    hasher.update(f.read())
            
            hashes[dataset_name] = hasher.hexdigest()
        
        return hashes
    
    def get_stored_metadata(self):
        """Retrieve stored metadata from JSON file"""
        if not self.metadata_file.exists():
            return None, {}, {}
        
        try:
            with open(self.metadata_file, 'r') as f:
                data = json.load(f)
            
            atomspace_version = data.get('atomspace_version')
            dataset_hashes = data.get('dataset_hashes', {})
            dataset_versions = data.get('dataset_versions', {})
            
            return atomspace_version, dataset_hashes, dataset_versions
            
        except Exception as e:
            print(f"  Warning: Error reading metadata file: {e}")
            return None, {}, {}
    
    def check_and_version(self):
        """Check for changes and determine new versions"""
        print("\n" + "="*60)
        print("VERSION CHECK - MORK Database")
        print("="*60)
        
        print("\nCalculating hashes...")
        current_hashes = self.hash_all_datasets()
        print(f"   Found {len(current_hashes)} datasets")
        
        print("\n📂 Retrieving stored metadata from file...")
        atomspace_version, stored_hashes, dataset_versions = self.get_stored_metadata()
        print(f"   Found {len(stored_hashes)} stored datasets")
        
        if atomspace_version:
            print(f"\n📌 Current AtomSpace version: {atomspace_version}")
        else:
            print(f"\n📌 No previous version found (fresh install)")
        
        print(f"\n🔍 Comparing hashes...")
        changed = []
        new = []
        unchanged = []
        
        for dataset, current_hash in current_hashes.items():
            stored_hash = stored_hashes.get(dataset)
            
            if stored_hash is None:
                new.append(dataset)
                print(f"  🆕 NEW: {dataset}")
            elif stored_hash != current_hash:
                changed.append(dataset)
                old_ver = dataset_versions.get(dataset, "None")
                new_ver = self._increment_version(old_ver)
                print(f"  🔄 CHANGED: {dataset}: {old_ver} → {new_ver}")
            else:
                unchanged.append(dataset)
                ver = dataset_versions.get(dataset, "v1")
                print(f"  ✅ UNCHANGED: {dataset} ({ver})")
        
        all_changed = changed + new
        
        if not all_changed:
            print(f"\n{'='*60}")
            print("✅ No changes detected - nothing to load!")
            print(f"{'='*60}\n")
            return None
        
        new_atomspace_version = self._increment_version(atomspace_version)
        new_dataset_versions = dataset_versions.copy()
        
        for dataset in all_changed:
            old_version = dataset_versions.get(dataset)
            new_version = self._increment_version(old_version)
            new_dataset_versions[dataset] = new_version
        
        print(f"\n{'='*60}")
        print(f"📦 Version Summary:")
        print(f"   AtomSpace version: {atomspace_version or 'None'} → {new_atomspace_version}")
        print(f"   Changed datasets: {len(all_changed)}")
        print(f"   Unchanged datasets: {len(unchanged)}")
        print(f"{'='*60}\n")
        
        return {
            'atomspace_version': new_atomspace_version,
            'dataset_versions': new_dataset_versions,
            'changed_datasets': all_changed,
            'unchanged_datasets': unchanged,
            'current_hashes': current_hashes
        }
    
    def archive_dataset(self, dataset_name, version):
        """Archive a dataset to archive directory"""
        source_path = self.data_dir / dataset_name
        dest_path = self.archive_dir / dataset_name / version
        
        if not source_path.exists():
            print(f"  ⚠️  Source not found: {source_path}")
            return
        
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        if dest_path.exists():
            shutil.rmtree(dest_path)
        
        shutil.copytree(source_path, dest_path)
        print(f"  ✅ Archived to: {dest_path}")
    
    def store_metadata(self, version_info, build_id):
        """Store version metadata to JSON file"""
        print(f"\n💾 Storing version metadata to file...")
        
        metadata = {
            'atomspace_version': version_info['atomspace_version'],
            'build_id': build_id,
            'build_timestamp': datetime.utcnow().isoformat() + 'Z',
            'dataset_hashes': version_info['current_hashes'],
            'dataset_versions': version_info['dataset_versions']
        }
        
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"  ✅ Stored metadata to: {self.metadata_file}")
        
        # add metadata atoms to MORK annotation namespace
        self._add_metadata_to_mork(version_info, build_id)
    
    def _add_metadata_to_mork(self, version_info, build_id):
        """Add metadata atoms to MORK annotation namespace"""
        print(f"  💾 Adding metadata atoms to MORK...")
        
        metadata_atoms = []
        atomspace_version = version_info['atomspace_version']
        
        for dataset in version_info['changed_datasets']:
            dataset_version = version_info['dataset_versions'][dataset]
            metadata_atoms.append(
                f'(data-metadata (dataset "{dataset}") '
                f'(atomspace-version "{atomspace_version}") '
                f'(dataset-version "{dataset_version}") '
                f'(build-id "{build_id}") '
                f'(timestamp "{datetime.utcnow().isoformat()}Z"))'
            )
        
        with self.server.work_at("annotation") as scope:
            metadata_text = "\n".join(metadata_atoms) + "\n"
            scope.upload_(metadata_text)
        
        print(f"  ✅ Added {len(metadata_atoms)} metadata atoms to MORK")
    
    def _increment_version(self, version):
        """Increment version string (v1 → v2 → v3)"""
        if version is None:
            return "v1"
        num = int(version.replace('v', ''))
        return f"v{num + 1}"


def connect_to_mork(host="localhost", port=None):
    """Connect to the running MORK instance."""
    if port is None:
        port = os.getenv("HOST_PORT", 8432)
    url = f"http://{host}:{port}"
    print(f"Connecting to MORK at {url} ...")
    server = MORK(url)
    
    try:
        with server.work_at("annotation") as scope:
            pass
        print("[SUCCESS] Successfully connected to MORK.")
    except Exception as e:
        raise ConnectionError(f"[FAILED] Connection failed: {e}")
    
    return server


def clear_annotation_namespace(server):
    """Clear the annotation namespace before loading"""
    print(f"\n🗑️  Clearing annotation namespace...")
    try:
        with server.work_at("annotation") as scope:
            scope.clear()
        print(f"  ✅ Namespace cleared")
    except Exception as e:
        print(f"  ⚠️  Error clearing namespace: {e}")


def load_metta_files(server, data_dir):
    """Load all .metta files from the given directory into MORK."""
    path = Path(data_dir)
    if not path.exists():
        raise ValueError(f"[FAILED] Data directory '{path}' not found.")
    
    files = list(path.rglob("*.metta"))
    if not files:
        raise ValueError(f"[WARNING] No .metta files found in '{path}'.")
    
    print(f"\n[FILE] Found {len(files)} .metta files. Starting import...")

    successful_files = 0
    failed_files = 0
    
    with server.work_at("annotation") as scope:
        for file_path in files:
            relative_path = file_path.relative_to(data_dir)
            container_file_path = Path("/app/data") / relative_path
            file_uri = f"file://{container_file_path}"
            
            folder_path = file_path.parent.name
            print(f"  ...Importing {folder_path}/{file_path.name}")
            try:
                cmd = scope.sexpr_import_(file_uri)
                cmd.block()
                print(f"     [SUCCESS] LOADED: {folder_path}/{file_path.name}")
                successful_files += 1
            except Exception as e:
                print(f"     [FAILED] FAILED: {folder_path}/{file_path.name}: {e}")
                failed_files += 1

    print("\n" + "="*50)
    print("...LOADING SUMMARY:")
    print(f"   [SUCCESS] Successfully loaded: {successful_files} files")
    print(f"   [FAILED] Failed to load: {failed_files} files")
    print(f"   Total processed: {len(files)} files")
    print("="*50)
    
    return successful_files, failed_files


def show_summary(server):
    """Show a detailed summary of data in MORK."""
    print(f"\n🔍 Verifying data in MORK...")
    
    with server.work_at("annotation") as scope:
        try:
            data = scope.download_(max_results=50)
            data.block()
            if data.data:
                print("\n  ... Sample atoms from annotation namespace (first 50):")
                print("  " + "="*60)
                lines = data.data.split('\n')[:50]
                for i, line in enumerate(lines, 1):
                    if line.strip():
                        print(f"  {i:3}. {line}")
                print("  " + "="*60)
                print(f"  ✅ Data verification successful")
            else:
                print("  ⚠️  No data found in annotation namespace.")
        except Exception as e:
            print(f"  ⚠️  Error fetching annotation data: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Load BioCypher MeTTa data into MORK with version management"
    )
    parser.add_argument("--data-dir", 
                        default=os.getenv("DATASET_PATH", "./output_hsa_metta"),
                        help="Directory with MeTTa files")
    parser.add_argument("--archive-dir",
                        default="/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives",
                        help="Archive base directory")
    parser.add_argument("--host",
                        default="localhost",
                        help="MORK host")
    parser.add_argument("--port",
                        default=8432,
                        type=int,
                        help="MORK port")
    parser.add_argument("--build-id",
                        default=f"build-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
                        help="Build ID")
    parser.add_argument("--verify",
                        action="store_true",
                        help="Verify data after loading")
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("MORK LOADER - BioCypher Knowledge Graph")
    print("="*60)
    
    # Step 1: Connect to MORK
    server = connect_to_mork(args.host, args.port)
    
    # Step 2: Initialize version manager
    version_manager = MORKVersionManager(server, args.data_dir, args.archive_dir)
    
    # Step 3: Check versions (compares hashes)
    version_info = version_manager.check_and_version()
    
    if version_info is None:
        # no changes, skip loading
        print("\n✅ Load complete (no changes)\n")
        return
    
    # Step 4: Archive ONLY changed datasets
    print(f"\n📦 Archiving changed datasets...")
    for dataset in version_info['changed_datasets']:
        version = version_info['dataset_versions'][dataset]
        print(f"  Archiving [{dataset}] to {version}...")
        version_manager.archive_dataset(dataset, version)
    
    # Step 5: Full reload (clear everything)
    clear_annotation_namespace(server)
    
    # Step 6: Load ALL data
    successful, failed = load_metta_files(server, args.data_dir)
    
    # Step 7: Store version metadata (JSON file + MORK atoms)
    version_manager.store_metadata(version_info, args.build_id)
    
    # Step 8: Verify (optional)
    if args.verify:
        show_summary(server)
    
    # Final summary
    print("\n" + "="*60)
    print(f"✅ MORK LOAD COMPLETE")
    print(f"   Version: {version_info['atomspace_version']}")
    print(f"   Build ID: {args.build_id}")
    print(f"   Changed datasets: {len(version_info['changed_datasets'])}")
    print(f"   Unchanged datasets: {len(version_info['unchanged_datasets'])}")
    print(f"   Files loaded: {successful}")
    if failed > 0:
        print(f"   ⚠️  Failed files: {failed}")
    print(f"\n   Metadata: {version_manager.metadata_file}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()