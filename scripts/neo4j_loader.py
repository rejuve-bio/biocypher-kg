from neo4j import GraphDatabase
from pathlib import Path
import logging
import getpass
import argparse
import sys
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _fmt_elapsed(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {seconds:.1f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.1f}s"

class Neo4jLoader:
    def __init__(self, uri, username, password, csv_base_path=None):
        self.driver = None
        self.session = None
        self.uri = uri
        self.username = username
        self.password = password
        # When set, rewrites file:/// URIs in cypher files to use this absolute path
        # as the root, allowing Neo4j to read CSVs from outside its default import dir.
        # Requires apoc.import.file.use_neo4j_config=false in apoc.conf.
        self.csv_base_path = str(csv_base_path).rstrip('/') if csv_base_path else None

    def connect(self):
        """Establish connection and verify credentials"""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
            # Verify connection with a simple query
            with self.driver.session() as test_session:
                test_session.run("RETURN 1").consume()
            logger.info("Successfully connected to Neo4j database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            return False

    def close(self):
        if self.session:
            self.session.close()
        if self.driver:
            self.driver.close()

    def start_session(self):
        """Start a new Neo4j session"""
        if not self.session:
            self.session = self.driver.session()
        return self.session

    def execute_constraint_query(self, query):
        try:
            result = self.session.run(query)
            result.consume()
            logger.info("Constraint created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating constraint: {str(e)}")
            return False

    def execute_load_query(self, query):
        try:
            result = self.session.run(query)
            records = list(result)
            if records and len(records) > 0:
                stats = records[0]
                logger.info(f"Data loaded successfully. Batches: {stats['batches']}, Total operations: {stats['total']}")
            return True
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return False

    def process_cypher_file(self, file_path):
        with open(file_path, 'r') as f:
            content = f.read()

        if self.csv_base_path:
            content = content.replace("file:///", f"file:///{self.csv_base_path.lstrip('/')}/")


        queries = []
        current_query = []
        
        for line in content.split('\n'):
            current_query.append(line)
            if line.strip().endswith(';') and not any(apoc_keyword in ''.join(current_query) 
                                                    for apoc_keyword in ['apoc.periodic.iterate', 'YIELD batches']):
                queries.append('\n'.join(current_query))
                current_query = []
        
        if current_query:
            queries.append('\n'.join(current_query))

        for query in queries:
            query = query.strip()
            if not query:
                continue
                
            if "CREATE CONSTRAINT" in query:
                if not self.execute_constraint_query(query):
                    logger.error(f"Failed to create constraint from {file_path}")
            elif "LOAD CSV" in query:
                if not self.execute_load_query(query):
                    logger.error(f"Failed to load data from {file_path}")

    def process_all_files(self, file_paths):
        for file_path in file_paths:
            logger.info(f"Processing file: {file_path}")
            self.process_cypher_file(file_path)

    def load_queries_from_directory(self, directory_path):
        directory = Path(directory_path)
        if not directory.exists():
            logger.error(f"Directory not found: {directory}")
            return

        return {
            'nodes': sorted(directory.glob("nodes_*.cypher")),
            'edges': sorted(directory.glob("edges_*.cypher"))
        }

def _load_env_file(path):
    try:
        env = {}
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, _, v = line.partition('=')
                env[k.strip()] = v.strip()
        return env
    except OSError as e:
        raise SystemExit(f"error: cannot read env file '{path}': {e}")


def get_neo4j_credentials():
    parser = argparse.ArgumentParser(description='Load data into Neo4j database')
    parser.add_argument('--output-dir', help='Path to the output directory')
    parser.add_argument('--uri', default="bolt://localhost:7687", help='Neo4j URI')
    parser.add_argument('--username', default="neo4j", help='Neo4j username')
    parser.add_argument('--password', default=None, help='Neo4j password (falls back to interactive prompt)')
    parser.add_argument('--env-file', default=None, help='Path to neo4j.env to load connection settings from')
    parser.add_argument('--csv-base-path', default=None,
                         help='Absolute host path to prepend to file:/// URIs in the .cypher files. '
                              'Use when Neo4j\'s import dir is not configured to the output dir '
                              '(requires apoc.import.file.use_neo4j_config=false). '
                              'Omit for the default Docker setup where /import is bind-mounted to output-dir.')

    # Pre-parse to detect --env-file, then set argparse defaults from it
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument('--env-file', default=None)
    pre_args, _ = pre.parse_known_args()
    if pre_args.env_file:
        env = _load_env_file(pre_args.env_file)
        parser.set_defaults(
            uri=env.get('NEO4J_URI', 'bolt://localhost:7687'),
            username=env.get('NEO4J_USERNAME', 'neo4j'),
            password=env.get('NEO4J_PASSWORD'),
            output_dir=env.get('NEO4J_OUTPUT_DIR'),
            csv_base_path=env.get('CSV_BASE_PATH'),
        )

    args = parser.parse_args()

    if not args.output_dir:
        parser.error('--output-dir is required (or set NEO4J_OUTPUT_DIR in --env-file)')

    password = args.password or getpass.getpass('Enter Neo4j password: ')

    loader = Neo4jLoader(args.uri, args.username, password)
    if not loader.connect():
        logger.error("Failed to connect to Neo4j.")
        sys.exit(1)
    loader.close()

    return args.uri, args.username, password, args.output_dir, args.csv_base_path

def process_output_directory(output_dir):
    output_path = Path(output_dir)
    query_dirs = []

    has_top_level = list(output_path.glob("nodes_*.cypher")) or list(output_path.glob("edges_*.cypher"))
    if has_top_level:
        query_dirs.append(output_path)

    for path in output_path.rglob("*"):
        if path.is_dir() and (list(path.glob("nodes_*.cypher")) or list(path.glob("edges_*.cypher"))):
            query_dirs.append(path)

    return query_dirs

def _dir_label(dir_path, output_path):
    rel = dir_path.relative_to(output_path)
    return str(rel) if str(rel) != "." else "(root)"


def main():
    neo4j_uri, username, password, output_dir, csv_base_path = get_neo4j_credentials()
    output_path = Path(output_dir)
    all_start = time.time()

    try:
        loader = Neo4jLoader(neo4j_uri, username, password, csv_base_path=csv_base_path)
        if not loader.connect():
            return

        loader.start_session()
        query_dirs = process_output_directory(output_dir)

        if not query_dirs:
            logger.error(f"No directories containing Cypher query files found in {output_dir}")
            return

        logger.info(f"Found {len(query_dirs)} directories containing Cypher queries")

        dir_files = [(dir_path, loader.load_queries_from_directory(dir_path)) for dir_path in query_dirs]
        species_time = {_dir_label(dir_path, output_path): 0.0 for dir_path, _ in dir_files}

        all_node_files = [f for _, files in dir_files for f in files['nodes']]
        logger.info(f"Processing {len(all_node_files)} node files across all directories")
        for dir_path, files in dir_files:
            label = _dir_label(dir_path, output_path)
            start = time.time()
            loader.process_all_files(files['nodes'])
            species_time[label] += time.time() - start

        all_edge_files = [f for _, files in dir_files for f in files['edges']]
        logger.info(f"Processing {len(all_edge_files)} edge files across all directories")
        for dir_path, files in dir_files:
            label = _dir_label(dir_path, output_path)
            start = time.time()
            loader.process_all_files(files['edges'])
            species_time[label] += time.time() - start

        all_elapsed = time.time() - all_start
        logger.info("\n" + "=" * 60)
        logger.info("  SPECIES TIMING SUMMARY")
        logger.info("=" * 60)
        name_width = max(len(name) for name in species_time)
        for label, elapsed in species_time.items():
            logger.info(f"  {label:<{name_width}} : {_fmt_elapsed(elapsed):>10}")
        logger.info("-" * 60)
        logger.info(f"  Total time (all directories): {_fmt_elapsed(all_elapsed)}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

    finally:
        loader.close()

if __name__ == "__main__":
    main()