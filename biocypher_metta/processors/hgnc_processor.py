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
        "col=gd_hgnc_id&col=gd_app_sym&col=gd_prev_sym&col=gd_aliases&col=gd_pub_ensembl_id"
        "&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit"
    )

    def __init__(
        self,
        cache_dir: str = 'aux_files/hgnc',
        update_interval_hours: Optional[int] = 48  # HGNC needs time-based (no remote metadata)
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
            'symbol': ['Approved symbol', 'Symbol'],
            'hgnc_id': ['HGNC ID'],
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
        symbol_to_ensembl = {}
        hgnc_id_to_symbol = {}
        hgnc_id_to_ensembl = {}

        for row in reader:
            symbol = row[actual_columns['symbol']]

            # Safely get ensembl_id and hgnc_id only if columns were found
            ensembl_id = ''
            if 'ensembl_id' in actual_columns and actual_columns['ensembl_id']:
                ensembl_id = row.get(actual_columns['ensembl_id'], '').strip()

            hgnc_id = ''
            if 'hgnc_id' in actual_columns and actual_columns['hgnc_id']:
                hgnc_id = row.get(actual_columns['hgnc_id'], '').strip()

            current_symbols[symbol] = symbol

            # Store HGNC ID to symbol mapping (always, even without Ensembl ID)
            if hgnc_id:
                hgnc_id_to_symbol[hgnc_id] = symbol

            if ensembl_id:
                base_ensembl = ensembl_id.split('.')[0]

                # Bidirectional Ensembl ↔ Symbol mappings
                ensembl_to_symbol[ensembl_id] = symbol
                ensembl_to_symbol[base_ensembl] = symbol
                symbol_to_ensembl[symbol] = base_ensembl

                # Store direct HGNC ID to Ensembl mapping when both exist
                if hgnc_id:
                    hgnc_id_to_ensembl[hgnc_id] = base_ensembl

            # Safely get alias and prev symbols
            aliases = []
            if 'alias_symbol' in actual_columns and actual_columns['alias_symbol']:
                aliases = row.get(actual_columns['alias_symbol'], '').split('|')

            prev_symbols = []
            if 'prev_symbol' in actual_columns and actual_columns['prev_symbol']:
                prev_symbols = row.get(actual_columns['prev_symbol'], '').split('|')

            for alias in aliases + prev_symbols:
                if alias and alias.strip():
                    symbol_aliases[alias.strip()] = symbol

        return {
            'current_symbols': current_symbols,
            'symbol_aliases': symbol_aliases,
            'ensembl_to_symbol': ensembl_to_symbol,
            'symbol_to_ensembl': symbol_to_ensembl,
            'hgnc_id_to_symbol': hgnc_id_to_symbol,
            'hgnc_id_to_ensembl': hgnc_id_to_ensembl
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

    def get_symbol_from_hgnc_id(self, hgnc_id: str) -> str:
        """
        Get gene symbol from HGNC ID.

        Args:
            hgnc_id: HGNC ID (e.g., "HGNC:11998")

        Returns:
            Gene symbol (e.g., "TP53") or None if not found
        """
        if not self.mapping:
            self.load_or_update()

        hgnc_id_to_symbol = self.mapping.get('hgnc_id_to_symbol', {})
        return hgnc_id_to_symbol.get(hgnc_id)

    def get_ensembl_id(self, identifier: str) -> str:
        """
        Get Ensembl ID for a gene symbol or HGNC ID.

        Args:
            identifier: Gene symbol (e.g., "TP53") or HGNC ID (e.g., "HGNC:11998")

        Returns:
            Ensembl gene ID (e.g., "ENSG00000141510") or None if not found
        """
        if not self.mapping:
            self.load_or_update()

        # Check if it's an HGNC ID (format: HGNC:12345)
        if identifier.startswith('HGNC:'):
            # Try direct HGNC ID to Ensembl mapping first
            hgnc_id_to_ensembl = self.mapping.get('hgnc_id_to_ensembl', {})
            if identifier in hgnc_id_to_ensembl:
                return hgnc_id_to_ensembl[identifier]

            # Fallback: HGNC ID -> symbol -> Ensembl ID
            hgnc_id_to_symbol = self.mapping.get('hgnc_id_to_symbol', {})
            symbol = hgnc_id_to_symbol.get(identifier)
            if symbol:
                symbol_to_ensembl = self.mapping.get('symbol_to_ensembl', {})
                return symbol_to_ensembl.get(symbol)

            return None

        # Otherwise treat as gene symbol - use direct mapping
        symbol_to_ensembl = self.mapping.get('symbol_to_ensembl', {})
        return symbol_to_ensembl.get(identifier)
