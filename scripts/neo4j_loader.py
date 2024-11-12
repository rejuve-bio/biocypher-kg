from neo4j import GraphDatabase
from pathlib import Path
import logging
import getpass
import argparse
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jLoader:
    def __init__(self, uri, username, password):
        self.driver = None
        self.session = None
        self.uri = uri
        self.username = username
        self.password = password

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

def get_neo4j_credentials():
    parser = argparse.ArgumentParser(description='Load data into Neo4j database')
    parser.add_argument('--output-dir', required=True, help='Path to the output directory')
    parser.add_argument('--uri', default="bolt://localhost:7687", help='Neo4j URI (default: bolt://localhost:7687)')
    parser.add_argument('--username', default="neo4j", help='Neo4j username (default: neo4j)')
    
    args = parser.parse_args()
    
    max_attempts = 3
    for attempt in range(max_attempts):
        password = getpass.getpass('Enter Neo4j password: ')
        loader = Neo4jLoader(args.uri, args.username, password)
        
        if loader.connect():
            loader.close()  # Close the test connection
            return args.uri, args.username, password, args.output_dir
        
        if attempt < max_attempts - 1:
            logger.error(f"Authentication failed. {max_attempts - attempt - 1} attempts remaining.")
    
    logger.error("Maximum authentication attempts reached. Exiting.")
    sys.exit(1)

def process_output_directory(output_dir):
    query_dirs = []
    for path in Path(output_dir).rglob("*"):
        if path.is_dir() and (
            list(path.glob("nodes_*.cypher")) or 
            list(path.glob("edges_*.cypher"))
        ):
            query_dirs.append(path)
    return query_dirs

def main():
    neo4j_uri, username, password, output_dir = get_neo4j_credentials()
    
    try:
        loader = Neo4jLoader(neo4j_uri, username, password)
        if not loader.connect():
            return
        
        loader.start_session()
        query_dirs = process_output_directory(output_dir)
        
        if not query_dirs:
            logger.error(f"No directories containing Cypher query files found in {output_dir}")
            return
        
        logger.info(f"Found {len(query_dirs)} directories containing Cypher queries")
        
        all_node_files = []
        all_edge_files = []
        
        for dir_path in query_dirs:
            files = loader.load_queries_from_directory(dir_path)
            all_node_files.extend(files['nodes'])
            all_edge_files.extend(files['edges'])
        
        logger.info(f"Processing {len(all_node_files)} node files across all directories")
        loader.process_all_files(all_node_files)
        
        logger.info(f"Processing {len(all_edge_files)} edge files across all directories")
        loader.process_all_files(all_edge_files)
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    
    finally:
        loader.close()

if __name__ == "__main__":
    main()