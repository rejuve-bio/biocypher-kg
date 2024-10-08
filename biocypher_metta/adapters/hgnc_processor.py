import requests
import csv
import pickle
import os
from typing import Dict, Optional, Any

class HGNCSymbolProcessor:
    def __init__(self, pickle_file_path: str = 'hgnc_gene_data/hgnc_data.pkl', version_file_path: str = 'hgnc_gene_data/hgnc_version.txt'):
        self.pickle_file_path = pickle_file_path
        self.version_file_path = version_file_path
        self.current_symbols: Dict[str, str] = {}  # Current official symbols
        self.symbol_aliases: Dict[str, str] = {}   # Maps old symbols to current ones
        self.ensembl_to_symbol: Dict[str, str] = {}  # Maps Ensembl IDs to HGNC symbols
        self.current_version = self.get_current_version()

    def get_current_version(self) -> str:
        """Get the current version from HGNC server"""
        url = "https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/json/hgnc_complete_set.json"
        response = requests.head(url)
        return response.headers.get('Last-Modified', '')

    def check_version(self) -> bool:
        """Check if we need to update the data"""
        if not os.path.exists(self.version_file_path):
            print(f"Version file not found. Current version: {self.current_version}")
            return True
        with open(self.version_file_path, 'r') as f:
            stored_version = f.read().strip()
        print(f"Stored version: {stored_version}")
        print(f"Current version: {self.current_version}")
        return self.current_version != stored_version

    def save_version(self):
        """Save the current version to file"""
        os.makedirs(os.path.dirname(self.version_file_path), exist_ok=True)
        with open(self.version_file_path, 'w') as f:
            f.write(self.current_version)

    def update_hgnc_data(self):
        """Update HGNC data if needed"""
        if not self.check_version() and os.path.exists(self.pickle_file_path):
            print("Using existing HGNC data.")
            self.load_data()
            return

        print("Updating HGNC data...")
        url = "https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv/hgnc_complete_set.txt"
        # response = requests.get(url)
        # response.raise_for_status()
        
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                print("Server returned 404 error. Using local database instead.")
                if os.path.exists(self.pickle_file_path):
                    self.load_data()
                    return
                else:
                    print("Local database not found. Cannot proceed without data.")
                    return
            else:
                print(f"HTTP error occurred: {e}")
                return
        except Exception as e:
            print(f"An error occurred: {e}")
            return
        
        reader = csv.DictReader(response.text.splitlines(), delimiter='\t')
        for row in reader:
            symbol = row['symbol']
            ensembl_id = row['ensembl_gene_id']
            
            # Store current symbols
            self.current_symbols[symbol] = symbol
            
            # Store Ensembl ID to symbol mapping
            if ensembl_id:
                self.ensembl_to_symbol[ensembl_id] = symbol
                # Also store the base Ensembl ID without version
                base_ensembl = ensembl_id.split('.')[0]
                self.ensembl_to_symbol[base_ensembl] = symbol
            
            # Process aliases
            aliases = row['alias_symbol'].split('|') if row['alias_symbol'] else []
            for alias in aliases:
                self.symbol_aliases[alias] = symbol

        self.save_data()
        self.save_version()

    def save_data(self):
        """Save processed data to pickle file"""
        os.makedirs(os.path.dirname(self.pickle_file_path), exist_ok=True)
        data = {
            'current_symbols': self.current_symbols,
            'symbol_aliases': self.symbol_aliases,
            'ensembl_to_symbol': self.ensembl_to_symbol
        }
        with open(self.pickle_file_path, 'wb') as f:
            pickle.dump(data, f)

    def load_data(self):
        """Load processed data from pickle file"""
        with open(self.pickle_file_path, 'rb') as f:
            data = pickle.load(f)
        self.current_symbols = data['current_symbols']
        self.symbol_aliases = data['symbol_aliases']
        self.ensembl_to_symbol = data['ensembl_to_symbol']

    def process_identifier(self, identifier: str) -> Dict[str, Any]:
        """
        Process a gene identifier (symbol or Ensembl ID)
        Returns a dictionary with status and symbol information`
        """
        # Remove version number from Ensembl ID if present
        base_identifier = identifier.split('.')[0] if identifier.startswith('ENSG') else identifier
        
        # Check if it's a current symbol
        if base_identifier in self.current_symbols:
            return {
                'status': 'current',
                'original': identifier,
                'current': base_identifier
            }
        
        # Check if it's an outdated symbol
        if base_identifier in self.symbol_aliases:
            current_symbol = self.symbol_aliases[base_identifier]
            return {
                'status': 'updated',
                'original': identifier,
                'current': current_symbol
            }
        
        # Check if it's an Ensembl ID with a known symbol
        if base_identifier in self.ensembl_to_symbol:
            return {
                'status': 'ensembl_with_symbol',
                'original': identifier,
                'current': self.ensembl_to_symbol[base_identifier],
                'ensembl_id': base_identifier
            }
        
        # If it's an Ensembl ID without a known symbol, keep it as is
        if base_identifier.startswith('ENSG'):
            return {
                'status': 'ensembl_only',
                'original': identifier,
                'current': base_identifier,
                'ensembl_id': base_identifier
            }
        
        # If we can't find it anywhere
        return {
            'status': 'unknown',
            'original': identifier,
            'current': identifier
        }

    def get_current_symbol(self, identifier: str) -> str:
        """
        Simple helper function that just returns the current symbol or original identifier
        """
        result = self.process_identifier(identifier)
        return result['current']
