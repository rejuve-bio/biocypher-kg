#!/usr/bin/env python3
import sys
import json
from pathlib import Path

from regenerate_metadata import compute_schema_hash, load_config_file


def check_schema_change(previous_build_path: str, schema_config_path: str, biocypher_config_path: str) -> bool:
    previous_graph_info = Path(previous_build_path) / "graph_info.json"

    if not previous_graph_info.exists():
        print("No previous graph_info.json found - assuming schema changed")
        return True

    try:
        with open(previous_graph_info, 'r') as f:
            graph_info = json.load(f)

        old_schema_hash = graph_info.get('metadata', {}).get('schema_version', {}).get('hash')

        if not old_schema_hash:
            print("No schema hash in previous build - assuming schema changed")
            return True

        new_schema_hash = compute_schema_hash(schema_config_path, biocypher_config_path)

        if old_schema_hash != new_schema_hash:
            print(f"Schema CHANGED:")
            print(f"  Old hash: {old_schema_hash}")
            print(f"  New hash: {new_schema_hash}")
            print(f"  Result: FULL REBUILD REQUIRED")
            return True
        else:
            print(f"Schema UNCHANGED:")
            print(f"  Hash: {new_schema_hash}")
            print(f"  Result: Incremental build OK")
            return False

    except Exception as e:
        print(f"Error checking schema: {e}", file=sys.stderr)
        return True  

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: check_schema_version.py <previous_build_path> <schema_config.yaml> <biocypher_config.yaml>")
        sys.exit(2)

    previous_build_path = sys.argv[1]
    schema_config_path = sys.argv[2]
    biocypher_config_path = sys.argv[3]

    schema_changed = check_schema_change(previous_build_path, schema_config_path, biocypher_config_path)

    sys.exit(1 if schema_changed else 0)
