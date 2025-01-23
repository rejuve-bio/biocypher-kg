from neo4j import GraphDatabase
from pathlib import Path
import logging
import time
import argparse
import urllib.parse
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jLoader:
    def __init__(self, uri, username, password):
        self.driver = None
        self.session = None
        self.uri = uri
        self.username = username
        self.password = password
        self.batch_size = 100  # Set to a smaller batch size to avoid hitting limits

    def connect(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
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
        if not self.session:
            self.session = self.driver.session()
        return self.session

    def execute_query(self, query, parameters=None):
        try:
            if parameters is None:
                parameters = {}
            result = self.session.run(query, parameters)
            result.consume()
            return True
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return False

    def process_cypher_file(self, file_path):
        with open(file_path, 'r') as f:
            content = f.read()

        # Split the content into individual queries
        queries = [q.strip() for q in content.split(';') if q.strip()]

        for query in queries:
            if not query:
                continue

            # Handle constraint creation
            if "CREATE CONSTRAINT" in query:
                logger.info(f"Creating constraint from {file_path}")
                success = self.execute_query(f"{query};")
                if not success:
                    logger.error(f"Failed to create constraint from {query}")
                continue

            # Handle LOAD CSV queries
            if "LOAD CSV" in query:
                file_path_start = query.find("file://")
                file_path_end = query.find("'", file_path_start)
                if file_path_start == -1 or file_path_end == -1:
                    logger.error("LOAD CSV query does not contain a valid file path.")
                    continue

                csv_file_path = query[file_path_start + 7:file_path_end]
                try:
                    self.load_data_in_batches(csv_file_path, query)
                except Exception as e:
                    logger.error(f"Error processing CSV file {csv_file_path}: {str(e)}")
                continue

            # Execute other queries directly
            success = self.execute_query(query)
            if not success:
                logger.error(f"Failed to execute query: {query}")

    def load_data_in_batches(self, csv_file_path, original_query):
        try:
            # Read CSV file with pipe delimiter
            with open(csv_file_path, 'r') as f:
                csv_reader = csv.DictReader(f, delimiter='|')
                headers = csv_reader.fieldnames

                # Determine labels and relationship types dynamically from headers
                node_label = headers[0].split('_')[0]  # Example: id might be 'Node_id'
                relationship_label = headers[1].split('_')[0] if len(headers) > 1 else None  # Example: sourceId might be 'Relationship_sourceId'

                batch = []

                for row in csv_reader:
                    # Ensure 'id' is present and not empty
                    if 'id' in row and row['id']:
                        batch.append(row)
                    else:
                        logger.error(f"Row is missing 'id' or 'id' is empty: {row}")

                    if len(batch) >= self.batch_size:
                        self.process_batch(batch, node_label, relationship_label)
                        batch = []

                # Process remaining records
                if batch:
                    self.process_batch(batch, node_label, relationship_label)

        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")

    def process_batch(self, batch, node_label, relationship_label):
        # Convert LOAD CSV query to direct MERGE statement for nodes
        node_query = self.convert_load_csv_to_create(node_label)

        # Execute the node creation query with the provided batch parameter
        parameters = {'batch': batch}
        self.execute_query(node_query, parameters)

        # Handle relationship creation if applicable
        if relationship_label:
            relationship_query = self.convert_load_csv_to_relationship(node_label, relationship_label)
            self.execute_query(relationship_query, parameters)

    def convert_load_csv_to_create(self, node_label):
        # Create a batch query for nodes
        batch_query = f"""
        UNWIND $batch AS row
        MERGE (n:{node_label} {{id: row.id}})
        SET n += apoc.map.removeKeys(row, ['id'])
        """
        return batch_query

    def convert_load_csv_to_relationship(self, node_label, relationship_label):
        # This is a simplified conversion for relationships
        relationship_query = f"""
        UNWIND $batch AS row
        MATCH (a:{node_label} {{id: row.sourceId}})
        OPTIONAL MATCH (b:{node_label} {{id: row.targetId}})
        WITH a, b, row
        WHERE b IS NOT NULL
        MERGE (a)-[:{relationship_label}]->(b)
        """
        return relationship_query

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
    # Updated with your credentials
    default_uri = "neo4j+s://fba89187.databases.neo4j.io"
    default_username = "neo4j"
    default_password = "LPbMcc_B6P33HUwlKkB1LxLdJXHSWc75B9lF3a0fJD8"

    logger.info("Using provided credentials. Waiting 60 seconds before connecting...")
    time.sleep(60)

    return default_uri, default_username, default_password

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
    neo4j_uri, username, password = get_neo4j_credentials()

    parser = argparse.ArgumentParser(description='Load data into Neo4j database')
    parser.add_argument('--output-dir', required=True, help='Path to the output directory')
    args = parser.parse_args()

    try:
        loader = Neo4jLoader(neo4j_uri, username, password)
        if not loader.connect():
            return

        loader.start_session()

        output_dir = urllib.parse.unquote(args.output_dir)
        query_dirs = process_output_directory(output_dir)

        if not query_dirs:
            logger.error(f"No directories containing Cypher query files found in {args.output_dir}")
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
        if 'loader' in locals():
            loader.close()

if __name__ == "__main__":
    main()
