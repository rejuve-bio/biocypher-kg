import csv
import gzip
import logging
import os
import psutil
import sqlite3
import time
from multiprocessing import Pool
from prometheus_client import Counter, Gauge, start_http_server

# Configure logging
LOG_FILE = "pipeline.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# Prometheus metrics
rows_processed = Counter("rows_processed", "Number of rows processed successfully")
errors_encountered = Counter("errors_encountered", "Number of errors encountered in the pipeline")
current_chunk_size = Gauge("current_chunk_size", "Current chunk size being processed")
retry_attempts = Counter("retry_attempts", "Number of retry attempts made")

# Retry decorator
def retry(attempts=3, delay=2):
    """Retry decorator for handling transient errors."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retry_attempts.inc()
                    logging.warning(f"Retry {attempt}/{attempts} for {func.__name__} due to error: {e}")
                    time.sleep(delay)
            raise Exception(f"All {attempts} retries failed for {func.__name__}")
        return wrapper
    return decorator

@retry(attempts=3, delay=5)
def get_optimal_chunk_size(estimated_row_size=1024, max_memory_fraction=0.1):
    """Calculate an optimal chunk size based on available memory."""
    available_memory = psutil.virtual_memory().available
    chunk_size = max(1, int((available_memory * max_memory_fraction) / estimated_row_size))
    logging.info(f"Calculated optimal chunk size: {chunk_size}")
    return chunk_size

@retry(attempts=3, delay=5)
def stream_data(file_path, chunk_size):
    """Stream data from a CSV file in chunks."""
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip headers
        buffer = []
        for i, row in enumerate(reader, start=1):
            buffer.append(row)
            if len(buffer) == chunk_size:
                rows_processed.inc(len(buffer))
                yield buffer
                buffer = []
        if buffer:
            rows_processed.inc(len(buffer))
            yield buffer

def process_chunk(chunk):
    """Process a chunk of data."""
    try:
        current_chunk_size.set(len(chunk))
        return [(row[0], row[1].upper()) for row in chunk]
    except Exception as e:
        errors_encountered.inc()
        logging.error(f"Error processing chunk: {e}")
        raise

@retry(attempts=3, delay=5)
def write_to_compressed_csv(file_path, headers, data_iterable):
    """Write processed data to a compressed CSV file."""
    with gzip.open(f"{file_path}.gz", 'wt', newline='', encoding='utf-8') as gzfile:
        writer = csv.writer(gzfile)
        writer.writerow(headers)
        for chunk in data_iterable:
            writer.writerows(chunk)
    logging.info(f"Compressed output written to {file_path}.gz")

@retry(attempts=3, delay=5)
def group_and_count(data, table_name):
    """Group and count data using SQLite."""
    conn = sqlite3.connect(':memory:')  # Use in-memory database
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE {table_name} (key TEXT, value TEXT)")
    conn.commit()
    cursor.executemany(f"INSERT INTO {table_name} VALUES (?, ?)", data)
    conn.commit()
    cursor.execute(f"SELECT key, COUNT(*) FROM {table_name} GROUP BY key")
    result = cursor.fetchall()
    conn.close()
    return result

def parallel_process_and_write(input_file, output_file, estimated_row_size=1024, max_memory_fraction=0.1):
    """Integrated pipeline for processing and writing large datasets."""
    try:
        optimal_chunk_size = get_optimal_chunk_size(estimated_row_size, max_memory_fraction)
        logging.info(f"Starting pipeline with chunk size: {optimal_chunk_size}")
        
        headers = ["id", "name"]  # Replace with actual headers
        
        with Pool() as pool:
            processed_data = pool.map(
                process_chunk,
                stream_data(input_file, optimal_chunk_size)
            )
        
        write_to_compressed_csv(output_file, headers, processed_data)

        grouped_data = group_and_count(
            [(row[0], row[1]) for chunk in processed_data for row in chunk],
            "example_table"
        )
        logging.info(f"Grouped data: {grouped_data}")
    except Exception as e:
        errors_encountered.inc()
        logging.critical(f"Pipeline encountered an error: {e}")
        raise

# Example usage
if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(8000)  # Accessible at http://localhost:8000/metrics
    
    input_file = "large_dataset.csv"  # Input file path
    output_file = "processed_output"  # Output file path
    try:
        parallel_process_and_write(input_file, output_file)
    except Exception as e:
        logging.critical(f"Critical failure in the pipeline: {e}")
