"""
HGNC Gene Symbol Processor.

Maintains mappings between:
- Current HGNC gene symbols
- Previous/alias symbols → current symbols
- Ensembl gene IDs → HGNC symbols

Data source: HGNC (HUGO Gene Nomenclature Committee)
Update strategy: Time-based (every 48 hours)
"""

import requests
import csv
from io import StringIO
from typing import Dict, Any, Optional
from .base_mapping_processor import BaseMappingProcessor


class HGNCProcessor(BaseMappingProcessor):
    HGNC_API_URL = (
        "https://www.genenames.org/cgi-bin/download/custom?"
        "col=gd_app_sym&col=gd_prev_sym&col=gd_aliases&col=gd_pub_ensembl_id"
        "&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit"
    )

    def __init__(
        self,
        cache_dir: str = 'aux_files/hgnc',
        update_interval_hours: Optional[int] = None
    ):
        super().__init__(
            name='hgnc',
            cache_dir=cache_dir,
            update_interval_hours=update_interval_hours
        )

    def get_remote_urls(self):
        return [self.HGNC_API_URL]

    def fetch_data(self) -> str:
        print(f"{self.name}: Fetching data from HGNC API...")
        response = requests.get(self.HGNC_API_URL, timeout=30)
        response.raise_for_status()
        return response.text

    def process_data(self, raw_data: str) -> Dict[str, Dict[str, Any]]:
        reader = csv.DictReader(StringIO(raw_data), delimiter='\t')

        print(f"{self.name}: Available columns: {reader.fieldnames}")

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
                print(f"{self.name}: Found column for {key}: {found}")
            else:
                print(f"{self.name}: Could not find column for {key}")

        current_symbols = {}
        symbol_aliases = {}
        ensembl_to_symbol = {}

        for row in reader:
            symbol = row[actual_columns['symbol']]
            ensembl_id = row.get(actual_columns.get('ensembl_id', ''), '')

            current_symbols[symbol] = symbol

            if ensembl_id:
                ensembl_to_symbol[ensembl_id] = symbol
                base_ensembl = ensembl_id.split('.')[0]
                ensembl_to_symbol[base_ensembl] = symbol

            aliases = row.get(actual_columns.get('alias_symbol', ''), '').split('|')
            prev_symbols = row.get(actual_columns.get('prev_symbol', ''), '').split('|')

            for alias in aliases + prev_symbols:
                if alias:
                    symbol_aliases[alias] = symbol

        return {
            'current_symbols': current_symbols,
            'symbol_aliases': symbol_aliases,
            'ensembl_to_symbol': ensembl_to_symbol
        }

    def process_identifier(self, identifier: str) -> Dict[str, Any]:
        if not self.mapping:
            self.load_or_update()

        current_symbols = self.mapping.get('current_symbols', {})
        symbol_aliases = self.mapping.get('symbol_aliases', {})
        ensembl_to_symbol = self.mapping.get('ensembl_to_symbol', {})

        base_identifier = identifier.split('.')[0] if identifier.startswith('ENSG') else identifier

        if base_identifier in current_symbols:
            return {
                'status': 'current',
                'original': identifier,
                'current': base_identifier
            }

        if base_identifier in symbol_aliases:
            current_symbol = symbol_aliases[base_identifier]
            return {
                'status': 'updated',
                'original': identifier,
                'current': current_symbol
            }

        if base_identifier in ensembl_to_symbol:
            return {
                'status': 'ensembl_with_symbol',
                'original': identifier,
                'current': ensembl_to_symbol[base_identifier],
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
        result = self.process_identifier(identifier)
        return result['current']

    def get_ensembl_id(self, symbol: str) -> str:
        if not self.mapping:
            self.load_or_update()

        ensembl_to_symbol = self.mapping.get('ensembl_to_symbol', {})

        for ensembl_id, gene_symbol in ensembl_to_symbol.items():
            if gene_symbol == symbol and '.' not in ensembl_id:
                return ensembl_id

        return None
