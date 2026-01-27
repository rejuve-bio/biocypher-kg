"""
Build a mapping pickle for FlyBase symbol -> FBgn mappings from synonyms table.

This script reads fb_synonym_fb_*.tsv.gz and creates a pickle
containing the mapping dictionary used by EPDAdapter for fly.

Usage:
    python build_flybase_synonym_mapping.py /path/to/fb_synonym_fb_2025_05.tsv.gz /path/to/output.pkl
"""

import pickle
import sys
from pathlib import Path
from biocypher_metta.adapters.dmel.flybase_tsv_reader import FlybasePrecomputedTable

def build_flybase_synonym_mapping(synonym_file_path):
    """Build mapping from gene symbols to FBgn IDs from FlyBase synonyms table.

    Returns:
    - symbol_to_fbgn: dict of symbol -> FBgn
    """
    synonym_file_path = Path(synonym_file_path)
    if not synonym_file_path.exists():
        raise FileNotFoundError(f"Synonym file not found: {synonym_file_path}")

    table = FlybasePrecomputedTable(str(synonym_file_path))
    df = table.to_pandas_dataframe()

    # Filter for FBgn (FlyBase gene IDs)
    fbgn_rows = df[df['primary_FBid'].str.startswith('FBgn', na=False)]

    symbol_to_fbgn = {}

    for _, row in fbgn_rows.iterrows():
        fbgn = row['primary_FBid']
        current_symbol = row.get('current_symbol', '').strip()
        synonyms = row.get('symbol_synonym(s)', '').strip()

        if current_symbol:
            symbol_to_fbgn[current_symbol] = fbgn

        if synonyms:
            for synonym in synonyms.split('|'):
                synonym = synonym.strip()
                if synonym:
                    symbol_to_fbgn[synonym] = fbgn

    return symbol_to_fbgn

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python build_flybase_synonym_mapping.py <synonym_file> <output_pickle>")
        sys.exit(1)

    synonym_file = sys.argv[1]
    output_pickle = sys.argv[2]

    print(f"Building mapping from {synonym_file}...")
    mapping = build_flybase_synonym_mapping(synonym_file)

    print(f"Saving mapping to {output_pickle}...")
    with open(output_pickle, 'wb') as f:
        pickle.dump(mapping, f)

    print(f"Mapping saved. Total symbols: {len(mapping)}")