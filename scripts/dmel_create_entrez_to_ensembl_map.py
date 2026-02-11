import csv
import gzip
import pickle
import argparse
import sys
import os
import requests
import tempfile
from urllib.parse import urljoin

'''
    This script downloads gene_info files from NCBI, extracts Ensembl cross-references,
    creates a pickled dictionary mapping Entrez IDs to Ensembl IDs, and cleans up
    the downloaded files automatically.
'''

# Base URL for NCBI gene_info files
BASE_NCBI_URL = "https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/"

# Mapping of organism codes to their NCBI category and gene_info filename
ORGANISM_CONFIG = {
    'dmel': {
        'category': 'Invertebrates',
        'filename': 'Drosophila_melanogaster.gene_info.gz',
        'full_name': 'Drosophila melanogaster',
        'dbxref_prefix': 'FLYBASE'
    },
    'hsa': {
        'category': 'Mammalia', 
        'filename': 'Homo_sapiens.gene_info.gz',
        'full_name': 'Homo sapiens',
        'dbxref_prefix': 'Ensembl'
    },
    'mmu': {
        'category': 'Mammalia',
        'filename': 'Mus_musculus.gene_info.gz',
        'full_name': 'Mus musculus',
        'dbxref_prefix': ''
    },
    'cel': {
        'category': 'Invertebrates',
        'filename': 'Caenorhabditis_elegans.gene_info.gz',
        'full_name': 'Caenorhabditis elegans',
        'dbxref_prefix': ''
    },
    # Add more organisms as needed
}

def download_gene_info_file(organism_code, temp_dir):
    """
    Downloads the gene_info.gz file for the specified organism to a temporary directory.
    
    Args:
        organism_code (str): The organism code (e.g., 'dmel', 'hsa')
        temp_dir (str): Path to the temporary directory for download
        
    Returns:
        str: Path to the downloaded file, or None if download failed
    """
    if organism_code not in ORGANISM_CONFIG:
        print(f"Error: Organism code '{organism_code}' is not supported.", file=sys.stderr)
        print(f"Supported organisms: {', '.join(ORGANISM_CONFIG.keys())}", file=sys.stderr)
        return None
    
    config = ORGANISM_CONFIG[organism_code]
    category = config['category']
    filename = config['filename']
    full_name = config['full_name']
    
    # Construct the full URL
    file_url = urljoin(BASE_NCBI_URL, f"{category}/{filename}")
    local_filepath = os.path.join(temp_dir, filename)
    
    print(f"Downloading gene_info for {full_name} ({organism_code})...")
    print(f"URL: {file_url}")
    
    try:
        response = requests.get(file_url, stream=True)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        # Download with progress indication
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        
        with open(local_filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    
                    # Simple progress indicator
                    if total_size > 0:
                        progress = (downloaded_size / total_size) * 100
                        print(f"\rProgress: {progress:.1f}%", end='', flush=True)
        
        print(f"\nDownload completed: {local_filepath}")
        return local_filepath
        
    except requests.exceptions.RequestException as e:
        print(f"\nError downloading file: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"\nUnexpected error during download: {e}", file=sys.stderr)
        return None

def extract_ensembl_mappings(gz_tsv_filepath, organism_code):
    """
    Extracts Ensembl cross-references from a gzipped TSV file.
    
    Args:
        gz_tsv_filepath (str): Path to the gzipped TSV input file
        organism_code (str): The organism code (e.g., 'dmel', 'hsa')
        
    Returns:
        dict: Dictionary mapping Entrez Gene IDs to Ensembl IDs
    """
    gene_ensembl_map = {}
    extracted_count = 0
    total_genes_processed = 0
    
    print(f"Extracting Ensembl mappings from {gz_tsv_filepath}...")
    
    try:
        with gzip.open(gz_tsv_filepath, 'rt', encoding='utf-8') as tsv_file:
            # Use DictReader to easily access columns by header name
            reader = csv.DictReader(tsv_file, delimiter='\t')
            
            # Check if required headers exist
            if 'GeneID' not in reader.fieldnames or 'dbXrefs' not in reader.fieldnames:
                print(f"Error: The input TSV file must contain 'GeneID' and 'dbXrefs' columns.", file=sys.stderr)
                return None
            
            for row in reader:
                total_genes_processed += 1
                gene_id = row['GeneID']
                dbxrefs_str = row['dbXrefs']
                
                # Skip entries with no cross-references
                if dbxrefs_str == '-':
                    continue
                
                # Split the dbXrefs string into individual cross-references
                dbxrefs_list = dbxrefs_str.split('|')
                
                ensembl_id = None
                for xref in dbxrefs_list:
                    # Check if the xref starts with 'Ensembl:'
                    if xref.startswith(f'{ORGANISM_CONFIG[organism_code]['dbxref_prefix']}:'):
                        # Extract the ID part after 'Ensembl:'
                        ensembl_id = xref.split(':', 1)[1]
                        break  # Take the first Ensembl ID found
                
                if ensembl_id:
                    gene_ensembl_map[gene_id] = ensembl_id
                    extracted_count += 1
        
        print(f"Successfully processed {total_genes_processed} genes.")
        print(f"Found and extracted {extracted_count} Ensembl IDs.")
        return gene_ensembl_map
        
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        return None

def save_pickle_mapping(gene_mapping, organism_code):
    """
    Saves the gene mapping dictionary to a pickle file in the specified directory structure.
    
    Args:
        gene_mapping (dict): Dictionary mapping Entrez IDs to Ensembl IDs
        organism_code (str): The organism code (e.g., 'dmel', 'hsa')
        
    Returns:
        str: Path to the saved pickle file, or None if saving failed
    """
    if gene_mapping is None or len(gene_mapping) == 0:
        print(f"No mappings to save for {organism_code}.", file=sys.stderr)
        return None
    
    # Create the directory structure: aux_files/{organism_code}/
    output_dir = os.path.join('aux_files', organism_code)
    os.makedirs(output_dir, exist_ok=True)
    
    # Create the pickle filename
    pickle_filename = f"{organism_code}_entrez_to_ensembl.pkl"
    pickle_filepath = os.path.join(output_dir, pickle_filename)
    
    print(f"Saving mappings to {pickle_filepath}...")
    
    try:
        with open(pickle_filepath, 'wb') as pickle_file:
            pickle.dump(gene_mapping, pickle_file)
        
        print(f"Successfully saved {len(gene_mapping)} mappings to {pickle_filepath}")
        return pickle_filepath
        
    except Exception as e:
        print(f"Error saving pickle file: {e}", file=sys.stderr)
        return None

def process_organism(organism_code):
    """
    Complete workflow: download, extract, save, and cleanup for one organism.
    
    Args:
        organism_code (str): The organism code (e.g., 'dmel', 'hsa')
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n{'='*60}")
    print(f"Processing organism: {organism_code}")
    print(f"{'='*60}")
    
    # Create a temporary directory for download
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")
        
        # Step 1: Download the gene_info file
        downloaded_file = download_gene_info_file(organism_code, temp_dir)
        if downloaded_file is None:
            print(f"Failed to download gene_info file for {organism_code}")
            return False
        
        # Step 2: Extract Ensembl mappings
        gene_mapping = extract_ensembl_mappings(downloaded_file, organism_code)
        if gene_mapping is None:
            print(f"Failed to extract mappings for {organism_code}")
            return False
        
        # Step 3: Save the pickle file
        pickle_path = save_pickle_mapping(gene_mapping, organism_code)
        if pickle_path is None:
            print(f"Failed to save mappings for {organism_code}")
            return False
        
        print(f"Successfully completed processing for {organism_code}")
        print(f"Temporary files will be automatically cleaned up.")
        return True
    
    # The temporary directory and its contents are automatically deleted here

if __name__ == "__main__":
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Downloads NCBI gene_info files, extracts Ensembl cross-references, "
                    "and creates pickled dictionaries mapping Entrez IDs to Ensembl IDs. "
                    "Downloaded files are automatically cleaned up after processing.",
        epilog=f"Supported organisms: {', '.join(ORGANISM_CONFIG.keys())}"
    )
    parser.add_argument(
        "organism_code",
        help="The organism code to process (e.g., 'dmel' for Drosophila melanogaster, "
             "'hsa' for Homo sapiens). "
             f"Supported: {', '.join(ORGANISM_CONFIG.keys())}"
    )
    
    # Parse command-line arguments
    args = parser.parse_args()
    
    # Validate organism code
    if args.organism_code not in ORGANISM_CONFIG:
        print(f"Error: Organism code '{args.organism_code}' is not supported.", file=sys.stderr)
        print(f"Supported organisms: {', '.join(ORGANISM_CONFIG.keys())}", file=sys.stderr)
        sys.exit(1)
    
    # Process the organism
    success = process_organism(args.organism_code)
    
    if success:
        print(f"\n🎉 Successfully completed processing for {args.organism_code}!")
        pickle_path = os.path.join('aux_files', args.organism_code, f"{args.organism_code}_entrez_to_ensembl.pkl")
        print(f"📁 Pickle file saved at: {pickle_path}")
    else:
        print(f"\n❌ Failed to process {args.organism_code}")
        sys.exit(1)