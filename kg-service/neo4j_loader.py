#!/usr/bin/env python3
"""
Hybrid Neo4j Loader with Version Management
- Uses version_manager for versioning/archiving
- Uses .cypher files for reliable loading 
- Adds metadata after loading
- Multi-database architecture with db_type="neo4j"
"""
from neo4j import GraphDatabase
from version_manager import VersionManager
from pathlib import Path
from datetime import datetime
import logging
import argparse
import re
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Neo4jLoader:
    def __init__(self, uri, username, password, output_dir, archive_dir,
                 import_batch_size: int = 50000):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.output_dir = Path(output_dir)
        self.archive_dir = Path(archive_dir)
        self.import_batch_size = import_batch_size
        
        # Initialize version manager with db_type="neo4j"
        self.version_manager = VersionManager(
            archive_dir=archive_dir,      
            neo4j_uri=uri,                
            username=username,
            password=password,
            db_type="neo4j"                
        )

    def close(self):
        self.version_manager.close()
        self.driver.close()

    def verify_connection(self):
        """Verify Neo4j connection"""
        try:
            with self.driver.session() as session:
                session.run("RETURN 1").consume()
            logger.info("✅ Neo4j connection verified")
            return True
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False

    # ===== SURGICAL DELETE =====

    def delete_changed_datasets(self, changed_datasets: list):
        """Delete nodes from changed datasets only"""
        logger.info("="*60)
        logger.info("Starting surgical delete...")
        logger.info("="*60)

        with self.driver.session() as session:
            for dataset in changed_datasets:
                source = self.get_dataset_source_name(dataset)

                node_count_result = session.run("""
                    MATCH (n {source: $source})
                    RETURN count(n) as count
                """, source=source).single()
                node_count = node_count_result["count"] if node_count_result else 0

                edge_count_result = session.run("""
                    MATCH ()-[r {source: $source}]->()
                    RETURN count(r) as count
                """, source=source).single()
                edge_count = edge_count_result["count"] if edge_count_result else 0

                if node_count == 0 and edge_count == 0:
                    logger.info(f"  [{source}]: 0 nodes, 0 edges (skip)")
                    continue

                logger.info(f"  [{source}]: Deleting {node_count:,} nodes and {edge_count:,} edges...")

                # Delete edges first so edge-only datasets are also cleaned.
                session.run("""
                    CALL apoc.periodic.iterate(
                        'MATCH ()-[r {source: $source}]->() RETURN r',
                        'DELETE r',
                        {batchSize: $batch_size, parallel: false, params: {source: $source}}
                    )
                """, source=source, batch_size=self.import_batch_size).consume()

                # Delete nodes in batches to avoid single huge transaction cost.
                session.run("""
                    CALL apoc.periodic.iterate(
                        'MATCH (n {source: $source}) RETURN n',
                        'DETACH DELETE n',
                        {batchSize: $batch_size, parallel: false, params: {source: $source}}
                    )
                """, source=source, batch_size=self.import_batch_size).consume()

                logger.info(f"  ✅ [{source}]: Deleted {node_count:,} nodes and {edge_count:,} edges")

        logger.info("✅ Surgical delete complete!")

    def get_dataset_source_name(self, dataset_folder: str) -> str:
        """Map dataset folder name to source name (uppercase)"""
        return dataset_folder.upper()

    # ===== CYPHER FILE LOADING =====

    def find_cypher_files(self, folder_path: Path) -> dict:
        """Find all .cypher files in a folder (including subdirectories)"""
        node_files = sorted(folder_path.rglob("nodes_*.cypher"))
        edge_files = sorted(folder_path.rglob("edges_*.cypher"))
        
        return {
            'nodes': node_files,
            'edges': edge_files
        }

    def execute_cypher_file(self, cypher_file: Path):
        """Execute a single .cypher file, fixing paths automatically"""
        try:
            with open(cypher_file, 'r') as f:
                content = f.read()

            # # DEBUG: Show what we're looking for
            # import re
            # paths_before = re.findall(r"file://[^\s'\"]+\.csv", content)
            # if paths_before:
            #     logger.info(f"    🔍 BEFORE replacement: {paths_before[0]}")
            
            # # Show the pattern we're trying to match
            # pattern_to_match = f'file:////{str(self.output_dir)}/'
            # logger.info(f"    🔍 Looking for pattern: {pattern_to_match[:80]}...")
            # logger.info(f"    🔍 Will replace with: file:///import/")

            # FIX PATHS
            content = content.replace(
                f'file:///{str(self.output_dir)}/',
                'file:///'
            )

            # BOOST BATCH SIZE: dynamically replace whatever small batchSize was baked
            # into the .cypher file with the configured import_batch_size.
            # This lets us speed up already-generated files (e.g. coxpresdb) without
            # re-running the full KG generation pipeline.
            content = re.sub(
                r'\{batchSize\s*:\s*\d+',
                '{batchSize:' + str(self.import_batch_size),
                content
            )

            # # DEBUG: Show result
            # paths_after = re.findall(r"file://[^\s'\"]+\.csv", content)
            # if paths_after:
            #     logger.info(f"    🔍 AFTER replacement: {paths_after[0]}")

            queries = []
            current_query = []
            
            for line in content.split('\n'):
                current_query.append(line)
                if line.strip().endswith(';') and 'apoc.periodic.iterate' not in ''.join(current_query):
                    queries.append('\n'.join(current_query))
                    current_query = []
            
            if current_query:
                queries.append('\n'.join(current_query))

            with self.driver.session() as session:
                for query in queries:
                    query = query.strip()
                    if not query:
                        continue
                    
                    if "CREATE CONSTRAINT" in query:
                        session.run(query).consume()
                    elif "LOAD CSV" in query or "apoc.periodic.iterate" in query:
                        result = session.run(query)
                        records = list(result)
                        if records:
                            stats = records[0]
                            logger.info(f"    ✅ Loaded (batches: {stats.get('batches', '?')}, total: {stats.get('total', '?')})")
                        else:
                            logger.info(f"    ✅ Query executed")
            
            return True
            
        except Exception as e:
            logger.error(f"    ❌ Error executing {cypher_file.name}: {e}")
            return False

    def load_dataset(self, folder_name: str):
        """Load a dataset using its .cypher files"""
        folder_path = self.output_dir / folder_name
        
        if not folder_path.exists():
            logger.error(f"Folder not found: {folder_path}")
            return False

        logger.info(f"  Loading [{folder_name}]...")

        # Find all cypher files
        cypher_files = self.find_cypher_files(folder_path)
        
        # Load nodes first
        for node_file in cypher_files['nodes']:
            relative = node_file.relative_to(self.output_dir)
            logger.info(f"    Loading nodes: {relative}")
            self.execute_cypher_file(node_file)
        
        # Then load edges
        for edge_file in cypher_files['edges']:
            relative = edge_file.relative_to(self.output_dir)
            logger.info(f"    Loading edges: {relative}")
            self.execute_cypher_file(edge_file)

        logger.info(f"  ✅ Loaded [{folder_name}]")
        return True

    # ===== ADD METADATA =====

    def add_metadata_to_dataset(self, folder_name: str, atomspace_version: str, 
                   dataset_version: str, timestamp: str, build_id: str):
        """Add metadata to all nodes/edges from a dataset (batched for large datasets)"""
        
        logger.info(f"  Adding metadata to [{folder_name}]...")
        
        with self.driver.session() as session:
            # Get source name from DatasetMapping
            mapping_result = session.run("""
                MATCH (dm:DatasetMapping {folder: $folder, db_type: "neo4j"})
                RETURN dm.source as source
            """, folder=folder_name).single()
            
            if not mapping_result:
                logger.warning(f"    ⚠️  No DatasetMapping found for [{folder_name}]")
                return
            
            source = mapping_result["source"]
            logger.info(f"    Using source: [{source}]")
            
            # Update nodes in batches
            session.run("""
                CALL apoc.periodic.iterate(
                    "MATCH (n) 
                    WHERE n.source = $source
                        AND NOT n:DatasetHash AND NOT n:DatasetVersion 
                        AND NOT n:KGVersion AND NOT n:DatasetMapping
                    RETURN n",
                    "SET n.atomspace_version = $atomspace_version,
                        n.dataset_version = $dataset_version,
                        n.import_timestamp = $timestamp,
                        n.build_id = $build_id",
                    {batchSize: $batch_size, parallel: false, params: {
                        source: $source,
                        atomspace_version: $atomspace_version,
                        dataset_version: $dataset_version,
                        timestamp: $timestamp,
                        build_id: $build_id
                    }}
                )
            """, source=source, atomspace_version=atomspace_version,
                dataset_version=dataset_version, timestamp=timestamp, 
                build_id=build_id, batch_size=self.import_batch_size).consume()

            # Count results by label (preserve original reporting)
            node_stats = session.run("""
                MATCH (n)
                WHERE n.source = $source
                    AND NOT n:DatasetHash AND NOT n:DatasetVersion 
                    AND NOT n:KGVersion AND NOT n:DatasetMapping
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY count DESC
            """, source=source)

            total_nodes = 0
            for record in node_stats:
                label = record["label"]
                count = record["count"]
                total_nodes += count
                logger.info(f"    ✅ {count:,} [{label}] nodes")

            if total_nodes == 0:
                logger.info(f"    ⚠️  No nodes found")
            
            # Update edges in batches
            session.run("""
                CALL apoc.periodic.iterate(
                    "MATCH ()-[r]->()
                    WHERE r.source = $source
                    RETURN r",
                    "SET r.atomspace_version = $atomspace_version,
                        r.dataset_version = $dataset_version,
                        r.import_timestamp = $timestamp,
                        r.build_id = $build_id",
                    {batchSize: $batch_size, parallel: false, params: {
                        source: $source,
                        atomspace_version: $atomspace_version,
                        dataset_version: $dataset_version,
                        timestamp: $timestamp,
                        build_id: $build_id
                    }}
                )
            """, source=source, atomspace_version=atomspace_version,
                dataset_version=dataset_version, timestamp=timestamp,
                build_id=build_id, batch_size=self.import_batch_size).consume()

            # Count results by type (preserve original reporting)
            edge_stats = session.run("""
                MATCH ()-[r]->()
                WHERE r.source = $source
                RETURN type(r) as rel_type, count(r) as count
                ORDER BY count DESC
            """, source=source)

            total_edges = 0
            for record in edge_stats:
                rel_type = record["rel_type"]
                count = record["count"]
                total_edges += count
                logger.info(f"    ✅ {count:,} [{rel_type}] edges")

            if total_edges == 0:
                logger.info(f"    ⚠️  No edges found")
    # ===== MAIN WORKFLOW =====

    def load_all(self, build_id: str):
        """
        Main method - complete workflow:
        1. Check versions
        2. Archive changed datasets
        3. Delete changed datasets
        4. Load data using .cypher files
        5. Add metadata
        6. Finalize version
        """
        logger.info("="*60)
        logger.info("STARTING NEO4J LOAD WORKFLOW")
        logger.info("="*60)

        # Step 1: Check versions
        logger.info("\nSTEP 1: Checking versions...")
        result = self.version_manager.check_and_version(self.output_dir)

        if result[0] is None:
            # No changes detected
            logger.info("\n✅ No changes detected - nothing to load!")
            return True

        new_atomspace_version, new_dataset_versions, changed_datasets = result

        logger.info(f"\n📦 New AtomSpace version: {new_atomspace_version}")
        logger.info(f"📦 Changed datasets: {len(changed_datasets)}")

        # Step 2: Archive changed datasets
        logger.info("\nSTEP 2: Archiving changed datasets...")
        for dataset in changed_datasets:
            version = new_dataset_versions[dataset]
            logger.info(f"  Archiving [{dataset}] to {version}...")
            self.version_manager.archive_dataset(self.output_dir, dataset, version)

        # Step 3: Delete changed datasets (surgical)
        if changed_datasets:
            logger.info("\nSTEP 3: Surgical delete...")
            self.delete_changed_datasets(changed_datasets)

        # Step 4: Generate timestamp for this load
        timestamp = datetime.utcnow().isoformat() + 'Z'

        logger.info("\nSTEP 4: Loading data from .cypher files...")
        logger.info("="*60)

        # LOAD EACH DATASET (THIS WAS MISSING!)
        for dataset in changed_datasets:
            success = self.load_dataset(dataset)
            
            if not success:
                logger.error(f"Failed to load {dataset}")
                return False

        # Step 5: Finalize version (store hashes, create metadata nodes)
        logger.info("\nSTEP 5: Finalizing version metadata...")
        self.version_manager.finalize_version(
            self.output_dir,
            new_atomspace_version,
            new_dataset_versions,
            changed_datasets,
            build_id
        )
        
        # Step 6: Add metadata to all loaded datasets (AFTER finalize!)
        logger.info("\nSTEP 6: Adding metadata...")
        for dataset in changed_datasets:
            self.add_metadata_to_dataset(
                folder_name=dataset,
                atomspace_version=new_atomspace_version,
                dataset_version=new_dataset_versions[dataset],
                timestamp=timestamp,
                build_id=build_id
            )

        logger.info("\n" + "="*60)
        logger.info(f"✅ NEO4J LOAD COMPLETE - AtomSpace {new_atomspace_version}")
        logger.info(f"   Build ID: {build_id}")
        logger.info(f"   Changed datasets: {len(changed_datasets)}")
        logger.info("="*60)

        return True


def main():
    parser = argparse.ArgumentParser(
        description="Load BioCypher data to Neo4j with version management"
    )
    parser.add_argument("--output-dir", required=True,
                        help="BioCypher output directory")
    parser.add_argument("--archive-dir", 
                        default="/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives",
                        help="Archive directory")
    parser.add_argument("--uri", 
                        default="bolt://localhost:27688",
                        help="Neo4j URI")
    parser.add_argument("--username", 
                        default="neo4j",
                        help="Neo4j username")
    parser.add_argument("--password", 
                        required=True,
                        help="Neo4j password")
    parser.add_argument("--build-id",
                        default=f"build-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
                        help="Build ID")
    parser.add_argument("--import-batch-size",
                        type=int,
                        default=50000,
                        help="APOC batchSize for LOAD CSV and metadata operations (default: 50000). "
                             "Increase for faster loading on high-memory servers.")
    args = parser.parse_args()

    loader = Neo4jLoader(
        args.uri,
        args.username,
        args.password,
        args.output_dir,
        args.archive_dir,
        import_batch_size=args.import_batch_size
    )

    try:
        if not loader.verify_connection():
            sys.exit(1)

        success = loader.load_all(args.build_id)
        sys.exit(0 if success else 1)

    finally:
        loader.close()


if __name__ == "__main__":
    main()