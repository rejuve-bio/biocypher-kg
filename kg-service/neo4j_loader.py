#!/usr/bin/env python3
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
                 import_batch_size: int = 50000, import_dir: str = None):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.output_dir = Path(output_dir)
        self.archive_dir = Path(archive_dir)
        self.import_batch_size = import_batch_size
        # Optional: absolute path prefix injected into file:/// URLs.
        # Use when Neo4j's import dir is NOT configured to the output dir
        # (e.g. non-Docker Neo4j). Leave None for Docker (where /import is mounted).
        self.import_dir = import_dir.rstrip('/') if import_dir else None
        
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

    def extract_rel_type_from_cypher(self, cypher_file: Path) -> tuple:
        """Extract (rel_type, source_label, target_label) from an edge cypher file.
        Example: MATCH (source:protein {id:...}) MATCH (target:pathway {id:...}) MERGE (source)-[r:enables]->(target)
        Returns (enables, protein, pathway) — all three needed for precise surgical delete.
        """
        try:
            content = cypher_file.read_text()
            rel_match = re.search(r'\[(?:\w+)?:([\w]+)\]', content)
            rel_type = rel_match.group(1) if rel_match else None

            # Extract all MATCH (alias:label {id:...}) patterns — first is source, second is target
            label_matches = re.findall(r'MATCH\s*\(\w+:([\w]+)\s*\{', content)
            source_label = label_matches[0] if len(label_matches) > 0 else None
            target_label = label_matches[1] if len(label_matches) > 1 else None

            return (rel_type, source_label, target_label)
        except Exception as e:
            logger.warning(f"Could not extract rel type from {cypher_file}: {e}")
        return (None, None, None)

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

    def delete_changed_files(self, changed_files: list):
        """Per-file surgical delete.
        - Edge files: delete only the specific relationship type (not all edges in folder)
        - Node files: delete all nodes for that source (folder-level, once per source)
        """
        logger.info("="*60)
        logger.info("Starting surgical delete (per-file)...")
        logger.info("="*60)

        deleted_node_sources = set()  # avoid deleting the same source nodes twice

        with self.driver.session() as session:
            for file_path in changed_files:
                path = Path(file_path)
                file_name = path.name
                folder = path.parts[0]
                source = self.get_dataset_source_name(folder)

                if file_name.startswith("edges_"):
                    cypher_file = self.output_dir / path.with_suffix(".cypher")
                    rel_type, source_label, target_label = self.extract_rel_type_from_cypher(cypher_file)

                    if rel_type and target_label:
                        label = f"{source}:{rel_type}→{target_label}"
                        count_result = session.run("""
                            MATCH ()-[r]->(t)
                            WHERE type(r) = $rel_type AND r.source = $source AND $target_label IN labels(t)
                            RETURN count(r) as count
                        """, rel_type=rel_type, source=source, target_label=target_label).single()
                        count = count_result["count"] if count_result else 0

                        if count == 0:
                            logger.info(f"  [{label}]: 0 edges (skip)")
                            continue

                        logger.info(f"  [{label}]: Deleting {count:,} edges...")
                        session.run("""
                            CALL apoc.periodic.iterate(
                                'MATCH ()-[r]->(t) WHERE type(r) = $rel_type AND r.source = $source AND $target_label IN labels(t) RETURN r',
                                'DELETE r',
                                {batchSize: $batch_size, parallel: false, params: {rel_type: $rel_type, source: $source, target_label: $target_label}}
                            )
                        """, rel_type=rel_type, source=source, target_label=target_label, batch_size=self.import_batch_size).consume()
                        logger.info(f"  ✅ [{label}]: Deleted {count:,} edges")
                    elif rel_type:
                        label = f"{source}:{rel_type}"
                        count_result = session.run("""
                            MATCH ()-[r]->()
                            WHERE type(r) = $rel_type AND r.source = $source
                            RETURN count(r) as count
                        """, rel_type=rel_type, source=source).single()
                        count = count_result["count"] if count_result else 0

                        if count == 0:
                            logger.info(f"  [{label}]: 0 edges (skip)")
                            continue

                        logger.info(f"  [{label}]: Deleting {count:,} edges...")
                        session.run("""
                            CALL apoc.periodic.iterate(
                                'MATCH ()-[r]->() WHERE type(r) = $rel_type AND r.source = $source RETURN r',
                                'DELETE r',
                                {batchSize: $batch_size, parallel: false, params: {rel_type: $rel_type, source: $source}}
                            )
                        """, rel_type=rel_type, source=source, batch_size=self.import_batch_size).consume()
                        logger.info(f"  ✅ [{label}]: Deleted {count:,} edges")
                    else:
                        # fallback: delete all edges for this source
                        logger.warning(f"  Could not extract rel type from {file_path}, falling back to full source delete")
                        session.run("""
                            CALL apoc.periodic.iterate(
                                'MATCH ()-[r {source: $source}]->() RETURN r',
                                'DELETE r',
                                {batchSize: $batch_size, parallel: false, params: {source: $source}}
                            )
                        """, source=source, batch_size=self.import_batch_size).consume()

                elif file_name.startswith("nodes_") and source not in deleted_node_sources:
                    count_result = session.run("""
                        MATCH (n {source: $source}) RETURN count(n) as count
                    """, source=source).single()
                    count = count_result["count"] if count_result else 0

                    if count == 0:
                        logger.info(f"  [{source}]: 0 nodes (skip)")
                        deleted_node_sources.add(source)
                        continue

                    logger.info(f"  [{source}]: Deleting {count:,} nodes...")
                    session.run("""
                        CALL apoc.periodic.iterate(
                            'MATCH (n {source: $source}) RETURN n',
                            'DETACH DELETE n',
                            {batchSize: $batch_size, parallel: false, params: {source: $source}}
                        )
                    """, source=source, batch_size=self.import_batch_size).consume()
                    logger.info(f"  ✅ [{source}]: Deleted {count:,} nodes")
                    deleted_node_sources.add(source)

        logger.info("✅ Surgical delete complete!")

    def get_dataset_source_name(self, dataset_folder: str) -> str:
        """Resolve dataset source from DatasetMapping, fallback to uppercase folder name."""
        fallback_source = dataset_folder.upper()

        try:
            with self.driver.session() as session:
                result = session.run(
                    """
                    MATCH (dm:DatasetMapping {folder: $folder, db_type: "neo4j"})
                    RETURN dm.source as source
                    LIMIT 1
                    """,
                    folder=dataset_folder,
                ).single()

            if result and result.get("source"):
                return result["source"]
        except Exception as e:
            logger.warning(
                f"Could not resolve DatasetMapping source for '{dataset_folder}': {e}. "
                f"Falling back to '{fallback_source}'."
            )

        return fallback_source

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

            # BACKWARD COMPAT: old-style cypher files embed the absolute host path
            # (file:////absolute/path/to/output/subdir/file.csv). Strip it down to a
            # relative path so the same logic below applies to both old and new files.
            old_prefix = f'file:///{str(self.output_dir)}/'
            if old_prefix in content:
                content = content.replace(old_prefix, 'file:///')

            # INJECT IMPORT DIR: for non-Docker Neo4j where the import dir is not
            # configured to the output dir, prepend the absolute path so Neo4j can
            # resolve the file. Leave as-is for Docker (file:///relative → /import/relative).
            if self.import_dir:
                content = content.replace('file:///', f'file:///{self.import_dir}/')

            # BOOST BATCH SIZE: dynamically replace whatever small batchSize was baked
            # into the .cypher file with the configured import_batch_size.
            # For large files (>1GB CSV), cap at 5000 to avoid Java heap OOM.
            csv_file = cypher_file.with_suffix('.csv')
            if csv_file.exists() and csv_file.stat().st_size > 1_000_000_000:
                effective_batch_size = 5000
                logger.info(f"    Large file detected ({csv_file.stat().st_size // (1024**3)}GB) — using batch size {effective_batch_size}")
            else:
                effective_batch_size = self.import_batch_size
            content = re.sub(
                r'\{batchSize\s*:\s*\d+',
                '{batchSize:' + str(effective_batch_size),
                content
            )

            # USE CREATE INSTEAD OF MERGE FOR EDGE FILES
            # Edges are always surgically deleted before reloading, so MERGE's
            # expensive existence check is unnecessary and causes Java heap OOM
            # on large files (e.g. gtex eqtl: 67M rows). CREATE is correct here.
            if cypher_file.name.startswith("edges_"):
                content = re.sub(r'\bMERGE\s*\((\w+)\)-\[', r'CREATE (\1)-[', content)

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

    def load_single_file(self, relative_file_path: str) -> bool:
        """Load a single CSV file via its corresponding .cypher file.
        relative_file_path is like 'reactome/edges_ppi.csv'
        """
        path = Path(relative_file_path)
        cypher_file = self.output_dir / path.with_suffix(".cypher")

        if not cypher_file.exists():
            logger.error(f"Cypher file not found: {cypher_file}")
            return False

        logger.info(f"  Loading [{relative_file_path}]...")
        return self.execute_cypher_file(cypher_file)

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
        1. Check versions (per-file hash comparison)
        2. Archive changed datasets (folder-level)
        3. Surgical delete per changed file (edge-type-level for edges, folder-level for nodes)
        4. Load only changed files via .cypher
        5. Finalize version metadata in Neo4j
        6. Stamp metadata on loaded nodes/edges
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

        new_atomspace_version, new_dataset_versions, changed_datasets, changed_files = result

        logger.info(f"\n📦 New AtomSpace version: {new_atomspace_version}")
        logger.info(f"📦 Changed datasets: {len(changed_datasets)}")
        logger.info(f"📦 Changed files: {len(changed_files)}")

        # Step 2: Archive changed datasets (folder-level)
        logger.info("\nSTEP 2: Archiving changed datasets...")
        for dataset in changed_datasets:
            version = new_dataset_versions[dataset]
            logger.info(f"  Archiving [{dataset}] to {version}...")
            self.version_manager.archive_dataset(self.output_dir, dataset, version)

        # Step 3: Per-file surgical delete
        if changed_files:
            logger.info("\nSTEP 3: Surgical delete (per-file)...")
            self.delete_changed_files(changed_files)

        # Step 4: Generate timestamp for this load
        timestamp = datetime.utcnow().isoformat() + 'Z'

        logger.info("\nSTEP 4: Loading changed files from .cypher files...")
        logger.info("="*60)

        # Nodes must load before edges — edges use MATCH on nodes that must already exist
        ordered_files = sorted(changed_files, key=lambda p: (0 if Path(p).name.startswith("nodes_") else 1))

        failed_files = {}   # file_path -> error (populated by load_single_file logging)
        loaded_datasets = set()

        for file_path in ordered_files:
            success = self.load_single_file(file_path)
            dataset = Path(file_path).parts[0]  # e.g. "gtex/eqtl/edges_..." → "gtex"

            if not success:
                failed_files[file_path] = "see error above"
            else:
                loaded_datasets.add(dataset)

        # Datasets where EVERY file failed — skip steps 5 & 6 for these
        failed_datasets = set()
        for file_path in failed_files:
            dataset = Path(file_path).parts[0]
            if dataset not in loaded_datasets:
                failed_datasets.add(dataset)

        # Datasets that had at least one file succeed
        ok_datasets = set(changed_datasets) - failed_datasets

        # Step 5: Finalize version (store hashes, create metadata nodes)
        logger.info("\nSTEP 5: Finalizing version metadata...")
        self.version_manager.finalize_version(
            self.output_dir,
            new_atomspace_version,
            new_dataset_versions,
            list(ok_datasets),
            build_id
        )

        # Step 6: Add metadata to successfully loaded datasets only
        logger.info("\nSTEP 6: Adding metadata...")
        for dataset in ok_datasets:
            self.add_metadata_to_dataset(
                folder_name=dataset,
                atomspace_version=new_atomspace_version,
                dataset_version=new_dataset_versions[dataset],
                timestamp=timestamp,
                build_id=build_id
            )

        logger.info("\n" + "="*60)
        if failed_files:
            logger.info(f"⚠️  NEO4J LOAD PARTIAL - AtomSpace {new_atomspace_version}")
            logger.info(f"   Build ID: {build_id}")
            logger.info(f"   Loaded datasets:  {len(ok_datasets)}")
            logger.info(f"   Failed datasets:  {len(failed_datasets)}")
            logger.info(f"\n   ❌ Failed files ({len(failed_files)}):")
            for fp in sorted(failed_files):
                logger.info(f"      - {fp}")
            logger.info("\n   Re-run with --only-files to retry failed files:")
            logger.info(f"      --only-files {' '.join(sorted(failed_files))}")
        else:
            logger.info(f"✅ NEO4J LOAD COMPLETE - AtomSpace {new_atomspace_version}")
            logger.info(f"   Build ID: {build_id}")
            logger.info(f"   Changed datasets: {len(changed_datasets)}")
        logger.info("="*60)

        return len(failed_files) == 0


def _load_env_file(path: str) -> dict:
    """Parse a simple KEY=VALUE env file, ignoring comments and blanks."""
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, value = line.partition('=')
            env[key.strip()] = value.strip()
    return env


def main():
    parser = argparse.ArgumentParser(
        description="Load BioCypher data to Neo4j with version management"
    )
    parser.add_argument("--env-file",
                        default=None,
                        help="Path to a neo4j.env file. Values are used as defaults "
                             "and can be overridden by explicit CLI flags.")
    parser.add_argument("--output-dir",
                        help="BioCypher output directory (env: NEO4J_OUTPUT_DIR)")
    parser.add_argument("--archive-dir",
                        help="Archive directory for version management (env: NEO4J_ARCHIVE_DIR)")
    parser.add_argument("--uri",
                        help="Neo4j bolt URI, e.g. bolt://localhost:7887 (env: NEO4J_URI)")
    parser.add_argument("--username",
                        help="Neo4j username (env: NEO4J_USERNAME, default: neo4j)")
    parser.add_argument("--password",
                        help="Neo4j password (env: NEO4J_PASSWORD)")
    parser.add_argument("--build-id",
                        default=f"build-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
                        help="Build ID")
    parser.add_argument("--import-batch-size",
                        type=int,
                        help="APOC batchSize for LOAD CSV and metadata operations "
                             "(env: NEO4J_IMPORT_BATCH_SIZE, default: 50000). "
                             "Increase for faster loading on high-memory servers.")
    parser.add_argument("--import-dir",
                        default=None,
                        help="Absolute host path to inject into file:/// URLs. "
                             "Use for non-Docker Neo4j where the import dir is not "
                             "configured to the output dir. Omit when running via Docker.")
    args = parser.parse_args()

    # Merge env-file values: CLI flags take precedence over env-file
    env = {}
    if args.env_file:
        env = _load_env_file(args.env_file)
        logger.info(f"Loaded config from {args.env_file}")

    def resolve(cli_val, env_key, default=None, required=False):
        val = cli_val or env.get(env_key) or default
        if required and not val:
            parser.error(f"--{env_key.lower().replace('_', '-')} is required "
                         f"(or set {env_key} in --env-file)")
        return val

    output_dir  = resolve(args.output_dir,  "NEO4J_OUTPUT_DIR",  required=True)
    archive_dir = resolve(args.archive_dir, "NEO4J_ARCHIVE_DIR", required=True)
    uri         = resolve(args.uri,         "NEO4J_URI",         required=True)
    username    = resolve(args.username,    "NEO4J_USERNAME",    default="neo4j")
    password    = resolve(args.password,    "NEO4J_PASSWORD",    required=True)
    batch_size  = int(resolve(args.import_batch_size, "NEO4J_IMPORT_BATCH_SIZE", default=50000))

    loader = Neo4jLoader(
        uri,
        username,
        password,
        output_dir,
        archive_dir,
        import_batch_size=batch_size,
        import_dir=args.import_dir,
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