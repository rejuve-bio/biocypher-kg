import requests
import csv
import pickle
import os
from typing import Dict, Any
from io import StringIO
from datetime import datetime, timedelta

class HGNCSymbolProcessor:
    def __init__(self, pickle_file_path: str = 'hgnc_gene_data/hgnc_data.pkl', version_file_path: str = 'hgnc_gene_data/hgnc_version.txt'):
        self.pickle_file_path = pickle_file_path
        self.version_file_path = version_file_path
        self.current_symbols: Dict[str, str] = {}
        self.symbol_aliases: Dict[str, str] = {}
        self.ensembl_to_symbol: Dict[str, str] = {}
        self.update_interval = timedelta(days=7)  # Update weekly
        self.last_update_check = None
        self.last_check_result = None

    def check_update_needed(self) -> bool:
        """Check if we need to update the data based on the last update time"""
        current_time = datetime.now()
        
        # Check if we have a cached result from the last 30 minutes
        if self.last_update_check and (current_time - self.last_update_check) < timedelta(minutes=30):
            return self.last_check_result

        self.last_update_check = current_time

        if not os.path.exists(self.version_file_path):
            print("HGNC data: Version file not found. Update needed.")
            self.last_check_result = True
            return True
    
        with open(self.version_file_path, 'r') as f:
            last_update_str = f.read().strip()
    
        try:
            last_update = datetime.fromisoformat(last_update_str)
            time_since_update = current_time - last_update
            update_needed = time_since_update > self.update_interval
        
            if update_needed:
                print(f"HGNC data: Last updated {time_since_update.days} days ago. Update needed.")
            else:
                print(f"HGNC data: Last updated {time_since_update.days} days ago. No update needed.")
            
            self.last_check_result = update_needed
            return update_needed
        except ValueError:
            print("HGNC data: Invalid date format in version file. Forcing update.")
            self.last_check_result = True
            return True

    def save_update_time(self):
        """Save the current time as the last update time"""
        os.makedirs(os.path.dirname(self.version_file_path), exist_ok=True)
        current_time = datetime.now().isoformat()
        with open(self.version_file_path, 'w') as f:
            f.write(current_time)
        print(f"HGNC data: Saved update time: {current_time}")

    def update_hgnc_data(self):
        """Update HGNC data if needed"""
        if not self.check_update_needed() and os.path.exists(self.pickle_file_path):
            print("HGNC data: Using existing data.")
            self.load_data()
            return

        print("HGNC data: Updating...")
        url = "https://www.genenames.org/cgi-bin/download/custom?col=gd_app_sym&col=gd_prev_sym&col=gd_aliases&col=gd_pub_ensembl_id&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"HGNC data: Error occurred while fetching data: {e}")
            if os.path.exists(self.pickle_file_path):
                print("HGNC data: Using local database instead.")
                self.load_data()
            else:
                print("HGNC data: Local database not found. Cannot proceed without data.")
            return

        reader = csv.DictReader(StringIO(response.text), delimiter='\t')
        
        print(f"HGNC data: Available columns: {reader.fieldnames}")

        column_mapping = {
            'symbol': ['Approved symbol', 'Symbol', 'HGNC ID'],
            'ensembl_id': ['Ensembl gene ID', 'Ensembl ID(supplied by Ensembl)', 'Ensembl ID'],
            'prev_symbol': ['Previous symbols', 'Previous symbol'],
            'alias_symbol': ['Alias symbols', 'Alias symbol']
        }

        actual_columns = {}
        for key, alternatives in column_mapping.items():
            found = next((col for col in alternatives if col in reader.fieldnames), None)
            if found:
                actual_columns[key] = found
                print(f"HGNC data: Found column for {key}: {found}")
            else:
                print(f"HGNC data: Could not find column for {key}")

        for row in reader:
            symbol = row[actual_columns['symbol']]
            ensembl_id = row.get(actual_columns.get('ensembl_id', ''), '')
            
            self.current_symbols[symbol] = symbol
            
            if ensembl_id:
                self.ensembl_to_symbol[ensembl_id] = symbol
                base_ensembl = ensembl_id.split('.')[0]
                self.ensembl_to_symbol[base_ensembl] = symbol
            
            aliases = row.get(actual_columns.get('alias_symbol', ''), '').split('|')
            prev_symbols = row.get(actual_columns.get('prev_symbol', ''), '').split('|')
            for alias in aliases + prev_symbols:
                if alias:
                    self.symbol_aliases[alias] = symbol

        self.save_data()
        self.save_update_time()

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
        print(f"HGNC data: Saved data to {self.pickle_file_path}")

    def load_data(self):
        """Load processed data from pickle file"""
        with open(self.pickle_file_path, 'rb') as f:
            data = pickle.load(f)
        self.current_symbols = data['current_symbols']
        self.symbol_aliases = data['symbol_aliases']
        self.ensembl_to_symbol = data['ensembl_to_symbol']
        print(f"HGNC data: Loaded data from {self.pickle_file_path}")

    def process_identifier(self, identifier: str) -> Dict[str, Any]:
        """
        Process a gene identifier (symbol or Ensembl ID)
        Returns a dictionary with status and symbol information
        """
        base_identifier = identifier.split('.')[0] if identifier.startswith('ENSG') else identifier
        
        if base_identifier in self.current_symbols:
            return {
                'status': 'current',
                'original': identifier,
                'current': base_identifier
            }
        
        if base_identifier in self.symbol_aliases:
            current_symbol = self.symbol_aliases[base_identifier]
            return {
                'status': 'updated',
                'original': identifier,
                'current': current_symbol
            }
        
        if base_identifier in self.ensembl_to_symbol:
            return {
                'status': 'ensembl_with_symbol',
                'original': identifier,
                'current': self.ensembl_to_symbol[base_identifier],
                'ensembl_id': base_identifier
            }
        
        if base_identifier.startswith('ENSG'):
            return {
                'status': 'ensembl_only',
                'original': identifier,
                'current': base_identifier,
                'ensembl_id': base_identifier
            }
        
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