#!/usr/bin/env python3
"""
Version Manager for BioCypher KG
- Discovers source names from CSV files
- Archives changed datasets only
- Performs surgical updates
"""
import hashlib
import csv
import shutil
from pathlib import Path
from datetime import datetime
from neo4j import GraphDatabase
import logging
import argparse
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VersionManager:
    def __init__(self, archive_dir, neo4j_uri, username, password, output_dir=None, db_type="neo4j"):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(username, password))
        self.archive_dir = Path(archive_dir)
        self.output_dir = None
        if output_dir:
            self.output_dir = Path(output_dir)
        self.db_type = db_type
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def close(self):
        self.driver.close()

    # ===== HASHING =====

    def hash_file(self, filepath: Path) -> str:
        """Calculate MD5 hash of a single file"""
        md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def hash_dataset_folder(self, folder: Path) -> str:
        """Hash all CSV files in a dataset folder combined"""
        csv_files = sorted(folder.rglob("*.csv"))
        if not csv_files:
            return None

        combined = hashlib.md5()
        for csv_file in csv_files:
            file_hash = self.hash_file(csv_file)
            combined.update(file_hash.encode())

        return combined.hexdigest()

    def hash_all_datasets(self) -> dict:
        """Hash each dataset folder separately"""
        dataset_hashes = {}

        dataset_folders = [
            p for p in sorted(self.output_dir.iterdir())
            if p.is_dir()
        ]

        if not dataset_folders:
            hash_value = self.hash_dataset_folder(self.output_dir)
            if hash_value:
                dataset_hashes["root"] = hash_value
        else:
            for folder in dataset_folders:
                hash_value = self.hash_dataset_folder(folder)
                if hash_value:
                    dataset_hashes[folder.name] = hash_value
                    logger.info(f"Hashed [{folder.name}]: {hash_value[:8]}...")

        return dataset_hashes

    # ===== SOURCE DISCOVERY =====

    def get_source_from_csv(self, folder: Path) -> list:
        """
        Read source values from CSV files in folder.
        Fully dynamic - no hardcoding.
        """
        sources = set()
        csv_files = list(folder.rglob("nodes_*.csv"))

        for csv_file in csv_files:
            try:
                with open(csv_file, 'r') as f:
                    reader = csv.DictReader(f, delimiter='|')
                    for row in reader:
                        if 'source' in row and row['source']:
                            sources.add(row['source'])
                        break  # Only need first row
            except Exception as e:
                logger.warning(f"Could not read {csv_file}: {e}")

        return list(sources)

    def discover_all_sources(self) -> dict:
        """
        Discover folder → source mapping.
        Returns: {"gencode": ["GENCODE"], "dbsnp": ["dbSNP"], ...}
        """
        folder_sources = {}

        dataset_folders = [
            p for p in sorted(self.output_dir.iterdir())
            if p.is_dir()
        ]

        for folder in dataset_folders:
            sources = self.get_source_from_csv(folder)
            if sources:
                folder_sources[folder.name] = sources
                logger.info(f"Discovered [{folder.name}] → sources: {sources}")

        return folder_sources

    # ===== ARCHIVING =====

    def archive_dataset(self, output_dir, folder_name: str, version: str):
        """
        Archive a dataset folder to /archives/{folder}/{version}/
        Only archives CHANGED datasets to save space.
        """
        self.output_dir = Path(output_dir)

        source_folder = self.output_dir / folder_name
        archive_path = self.archive_dir / self.db_type / folder_name / version
        
        if not source_folder.exists():
            logger.error(f"Source folder not found: {source_folder}")
            return None
        
        archive_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Archiving [{folder_name}] to {archive_path}")

        for item in source_folder.rglob("*"):
            if item.is_file():
                relative = item.relative_to(source_folder)
                dest = archive_path / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, dest)
        
        logger.info(f"✅ Archived [{folder_name}] v{version}: {archive_path}")
        return str(archive_path)

    # ===== NEO4J OPERATIONS =====

    def get_stored_hashes(self) -> dict:
        """Get previously stored hashes from Neo4j"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (h:DatasetHash {db_type: $db_type})
                RETURN h.dataset as dataset, h.hash as hash
            """, db_type=self.db_type)
            hashes = {r["dataset"]: r["hash"] for r in result}
            logger.info(f"Found {len(hashes)} stored hashes in Neo4j")
            return hashes

    def get_current_versions(self) -> dict:
        """
        Get current version info from Neo4j.
        Returns: {
            "atomspace_version": "v4",
            "dataset_versions": {"gencode": "v1", "dbsnp": "v1"}
        }
        """
        with self.driver.session() as session:
            # Get latest KGVersion node for this database type
            kg_result = session.run("""
                MATCH (v:KGVersion {db_type: $db_type})
                RETURN v.version as atomspace_version,
                    v.dataset_versions_json as dataset_versions_json
                ORDER BY v.created_at DESC
                LIMIT 1
            """, db_type=self.db_type).single()
            
            if not kg_result:
                return {
                    "atomspace_version": None,
                    "dataset_versions": {}
                }
            
            # Parse JSON string back to dict
            dataset_versions = {}
            if kg_result["dataset_versions_json"]:
                dataset_versions = json.loads(kg_result["dataset_versions_json"])
            
            return {
                "atomspace_version": kg_result["atomspace_version"],
                "dataset_versions": dataset_versions
            }

    def increment_version(self, current: str) -> str:
        """v1 → v2 → v3"""
        if current is None:
            return "v1"
        number = int(current.replace("v", ""))
        return f"v{number + 1}"

    def store_folder_source_mapping(self, folder_sources: dict, version: str):
        """Store folder → source mapping in Neo4j"""
        with self.driver.session() as session:
            for folder, sources in folder_sources.items():
                for source in sources:
                    session.run("""
                        MERGE (m:DatasetMapping {folder: $folder, source: $source, db_type: $db_type})
                        SET m.version = $version,
                            m.updated_at = $updated_at
                    """, folder=folder,
                         source=source,
                         db_type=self.db_type,
                         version=version,
                         updated_at=datetime.utcnow().isoformat() + 'Z')

        logger.info(f"✅ Stored folder→source mappings")

    def get_stored_folder_source_mapping(self) -> dict:
        """Retrieve folder → source mapping from Neo4j"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (m:DatasetMapping {db_type: $db_type})
                RETURN m.folder as folder, 
                       collect(m.source) as sources
            """, db_type=self.db_type)
            mapping = {r["folder"]: r["sources"] for r in result}
            return mapping

    def store_hashes(self, dataset_hashes: dict, dataset_versions: dict):
        """Store new hashes in Neo4j"""
        with self.driver.session() as session:
            for dataset, hash_value in dataset_hashes.items():
                version = dataset_versions.get(dataset, "v1")
                session.run("""
                    MERGE (h:DatasetHash {dataset: $dataset, db_type: $db_type})
                    SET h.hash = $hash,
                        h.version = $version,
                        h.updated_at = $updated_at
                """, dataset=dataset,
                     db_type=self.db_type,
                     hash=hash_value,
                     version=version,
                     updated_at=datetime.utcnow().isoformat() + 'Z')

        logger.info(f"✅ Stored hashes for {len(dataset_hashes)} datasets")

    def create_dataset_version_nodes(self, dataset_info: dict):
        """Create DatasetVersion nodes for changed datasets"""
        with self.driver.session() as session:
            for dataset, info in dataset_info.items():
                session.run("""
                    CREATE (dv:DatasetVersion {
                        dataset: $dataset,
                        db_type: $db_type,
                        version: $version,
                        timestamp: $timestamp,
                        archive_path: $archive_path
                    })
                """, dataset=dataset,
                     db_type=self.db_type,
                     version=info['version'],
                     timestamp=info['timestamp'],
                     archive_path=info['archive_path'])
        
        logger.info(f"✅ Created DatasetVersion nodes")

    def create_version_node(self, version: str, build_id: str,
                            changed: list, unchanged: list,
                            dataset_versions: dict):
        """Create global KGVersion node"""
        
        with self.driver.session() as session:
            session.run("""
                CREATE (v:KGVersion {
                    version: $version,
                    db_type: $db_type,
                    build_id: $build_id,
                    created_at: $created_at,
                    changed_datasets: $changed,
                    unchanged_datasets: $unchanged,
                    dataset_versions_json: $dataset_versions_json
                })
            """, version=version,
                 db_type=self.db_type,
                 build_id=build_id,
                 created_at=datetime.utcnow().isoformat() + 'Z',
                 changed=changed,
                 unchanged=unchanged,
                 dataset_versions_json=json.dumps(dataset_versions))

        logger.info(f"✅ Created KGVersion node: {version}")

    # ===== MAIN LOGIC =====

    def check_and_version(self, output_dir):
        """
        Main method:
        1. Hash all datasets
        2. Compare with Neo4j
        3. Discover sources
        4. Archive changed datasets
        5. Return result (tuple format for neo4j_loader.py)
        """
        # Store output_dir
        self.output_dir = Path(output_dir)

        logger.info("="*60)
        logger.info("Starting version check...")
        logger.info("="*60)

        # Step 1: Hash current output
        current_hashes = self.hash_all_datasets()
        if not current_hashes:
            logger.error("No CSV files found!")
            return (None, None, None, None)

        # Step 2: Get stored hashes
        stored_hashes = self.get_stored_hashes()

        # Step 3: Get current versions
        version_info = self.get_current_versions()
        current_atomspace_version = version_info["atomspace_version"]
        current_dataset_versions = version_info["dataset_versions"]
        
        logger.info(f"Current AtomSpace version: {current_atomspace_version or 'None (first run)'}")

        # Step 4: Compare hashes
        changed_datasets = []
        unchanged_datasets = []

        for dataset, hash_value in current_hashes.items():
            if dataset not in stored_hashes:
                changed_datasets.append(dataset)
                logger.info(f"  NEW:       {dataset}")
            elif stored_hashes[dataset] != hash_value:
                changed_datasets.append(dataset)
                logger.info(f"  CHANGED:   {dataset}")
            else:
                unchanged_datasets.append(dataset)
                logger.info(f"  UNCHANGED: {dataset}")

        # Step 5: No changes?
        if not changed_datasets:
            logger.info("✅ No changes detected - nothing to load!")
            return (None, None, None, None)

        # Step 6: Discover sources
        logger.info("Discovering source names from CSV files...")
        folder_sources = self.discover_all_sources()

        # Step 7: Increment versions
        new_atomspace_version = self.increment_version(current_atomspace_version)
        new_dataset_versions = current_dataset_versions.copy()
        
        for dataset in changed_datasets:
            old_version = current_dataset_versions.get(dataset)
            new_version = self.increment_version(old_version)
            new_dataset_versions[dataset] = new_version
            logger.info(f"  {dataset}: {old_version or 'None'} → {new_version}")

        logger.info(f"AtomSpace version: {current_atomspace_version or 'None'} → {new_atomspace_version}")

        # Build per-file list for changed datasets (relative paths like "gaf/edges_foo.csv")
        # neo4j_loader uses these for per-relationship surgical deletes and file-level loading.
        changed_files = []
        for dataset in changed_datasets:
            folder = self.output_dir / dataset
            if dataset == "root":
                for csv_file in sorted(self.output_dir.glob("*.csv")):
                    changed_files.append(str(csv_file.relative_to(self.output_dir)))
            else:
                for csv_file in sorted(folder.rglob("*.csv")):
                    changed_files.append(str(csv_file.relative_to(self.output_dir)))

        # Return tuple format expected by neo4j_loader.py
        return (new_atomspace_version, new_dataset_versions, changed_datasets, changed_files)

    def finalize_version(self, output_dir, atomspace_version, dataset_versions, 
                         changed_datasets, build_id):
        """
        Called AFTER successful data loading:
        1. Store folder → source mapping
        2. Store hashes
        3. Create DatasetVersion nodes
        4. Create KGVersion node
        """
        # Store output_dir
        self.output_dir = Path(output_dir)
        
        # Build dataset_info
        dataset_info = {}
        for dataset in changed_datasets:
            version = dataset_versions[dataset]
            archive_path = self.archive_dir / self.db_type / dataset / version
            dataset_info[dataset] = {
                'version': version,
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'archive_path': str(archive_path)
            }
        
        # Get folder sources
        folder_sources = self.discover_all_sources()
        
        # Get current hashes
        current_hashes = self.hash_all_datasets()
        
        # Calculate unchanged
        all_datasets = set(current_hashes.keys())
        unchanged_datasets = list(all_datasets - set(changed_datasets))
        
        logger.info("="*60)
        logger.info("Finalizing version metadata...")
        logger.info("="*60)

        # Store mappings
        self.store_folder_source_mapping(folder_sources, atomspace_version)

        # Store hashes
        self.store_hashes(current_hashes, dataset_versions)

        # Create DatasetVersion nodes
        self.create_dataset_version_nodes(dataset_info)

        # Create KGVersion node
        self.create_version_node(
            version=atomspace_version,
            build_id=build_id,
            changed=changed_datasets,
            unchanged=unchanged_datasets,
            dataset_versions=dataset_versions
        )

        logger.info(f"✅ Version {atomspace_version} finalized!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BioCypher KG Version Manager"
    )
    parser.add_argument("--output-dir", required=True,
                        help="Path to BioCypher output directory")
    parser.add_argument("--archive-dir", default="/tmp/biocypher-archives",
                        help="Path to archive directory")
    parser.add_argument("--uri", default="bolt://localhost:27688",
                        help="Neo4j URI")
    parser.add_argument("--username", default="neo4j",
                        help="Neo4j username")
    parser.add_argument("--password", required=True,
                        help="Neo4j password")
    parser.add_argument("--build-id",
                        default=f"build-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
                        help="Build ID for this run")
    args = parser.parse_args()

    vm = VersionManager(
        archive_dir=args.archive_dir,
        neo4j_uri=args.uri,
        username=args.username,
        password=args.password,
        output_dir=args.output_dir,
        db_type="neo4j"
    )

    try:
        result = vm.check_and_version(args.output_dir)

        if result[0] is not None:
            new_version, dataset_versions, changed, _changed_files = result
            print("\n" + "="*60)
            print("VERSION CHECK RESULT:")
            print("="*60)
            print(f"New Version: {new_version}")
            print(f"Changed:     {changed}")
            print("="*60)
        else:
            print("\n✅ No changes detected")

    finally:
        vm.close()