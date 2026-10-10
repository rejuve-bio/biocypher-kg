#!/usr/bin/env python3
"""
Script that receives a YAML file with BioCypher schemas and generates a '_sorted.yaml' file
with schemas organized as: nodes first (sorted), then edges (sorted).
Nodes and edges are sorted alphabetically (case-insensitive, digits first).
"""

import sys
import re
import yaml
import os

def find_duplicate_top_level_keys(file_path):
    """
    yaml.safe_load silently keeps only the last occurrence of a repeated top-level
    key. Scan the raw text so a duplicate (dead, shadowed) block doesn't go
    unnoticed while we sort around whatever the parser kept.
    """
    top_level_re = re.compile(r'^([^\s#][^:]*):(\s|$)')
    seen = {}
    dups = {}
    with open(file_path) as f:
        for lineno, line in enumerate(f, start=1):
            m = top_level_re.match(line)
            if not m:
                continue
            name = m.group(1).strip()
            if name in seen:
                dups.setdefault(name, [seen[name]]).append(lineno)
            else:
                seen[name] = lineno
    return dups

def sort_key(schema):
    """Sorting: digits first, then letters (case-insensitive)"""
    if schema[0].isdigit():
        return ('0', schema.lower())
    else:
        return ('1', schema.lower())

def load_yaml(file_path):
    """Load a YAML file with error handling"""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Invalid YAML in '{file_path}': {e}")
        sys.exit(1)

def categorize_schemas(data):
    """Separate schemas into nodes/edges/other based on 'represented_as' field.
    Nothing is dropped: a schema whose represented_as is missing or isn't
    'node'/'edge' goes to 'other' instead of silently disappearing from the
    output (that used to happen here with no warning)."""
    nodes = {}
    edges = {}
    other = {}

    for schema_name, schema_content in data.items():
        if schema_name == 'Title':
            continue

        represented_as = isinstance(schema_content, dict) and schema_content.get('represented_as')
        if represented_as == 'node':
            nodes[schema_name] = schema_content
        elif represented_as == 'edge':
            edges[schema_name] = schema_content
        else:
            other[schema_name] = schema_content

    return nodes, edges, other

def write_output_file(file_path, title, data_dict, sorted_nodes, sorted_edges, sorted_other=None):
    """Write output file with nodes first, then edges, then anything uncategorized"""
    sorted_other = sorted_other or []
    try:
        with open(file_path, 'w') as f:
            # Write title
            if title:
                yaml.dump({'Title': title}, f, default_flow_style=False, sort_keys=False)
                f.write('\n')  # Blank line after title

            # Write nodes (sorted)
            f.write('\n\n##################             NODES   SECTION             ####################\n\n')
            for i, schema_name in enumerate(sorted_nodes):
                yaml.dump(
                    {schema_name: data_dict[schema_name]},
                    f,
                    default_flow_style=False,
                    sort_keys=False
                )
                f.write('\n')  # Blank line between schemas

            # Write edges (sorted)
            f.write('\n\n##################             EDGES   SECTION             ####################\n\n')
            for i, schema_name in enumerate(sorted_edges):
                yaml.dump(
                    {schema_name: data_dict[schema_name]},
                    f,
                    default_flow_style=False,
                    sort_keys=False
                )
                if i < len(sorted_edges) - 1 or sorted_other:
                    f.write('\n')  # Blank line between schemas (except last)

            # Write anything that didn't classify as node or edge, rather than dropping it.
            if sorted_other:
                f.write('\n\n##################   UNCATEGORIZED (no represented_as: node/edge)   ##########\n\n')
                for i, schema_name in enumerate(sorted_other):
                    yaml.dump(
                        {schema_name: data_dict[schema_name]},
                        f,
                        default_flow_style=False,
                        sort_keys=False
                    )
                    if i < len(sorted_other) - 1:
                        f.write('\n')
    except Exception as e:
        print(f"Error: Failed to write to '{file_path}': {e}")
        sys.exit(1)

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python sort_schemas.py <input.yaml>")
        sys.exit(1)
    
    input_file = sys.argv[1]

    dups = find_duplicate_top_level_keys(input_file)
    if dups:
        print(f"WARNING: '{input_file}' has {len(dups)} duplicate top-level key(s) — only the LAST "
              f"occurrence of each survives YAML parsing, the earlier one(s) are dead/shadowed:")
        for name_, linenos in sorted(dups.items()):
            print(f"  - '{name_}' at lines {linenos}")
        print("  Fix these before trusting this file's sorted output.\n")

    # Load YAML
    data = load_yaml(input_file)

    if not isinstance(data, dict):
        print("Error: YAML file must contain a dictionary.")
        sys.exit(1)

    # Extract title
    title = data.get('Title')

    # Categorize schemas into nodes, edges, and anything uncategorized
    nodes, edges, other = categorize_schemas(data)

    # Sort schemas
    sorted_nodes = sorted(nodes.keys(), key=sort_key)
    sorted_edges = sorted(edges.keys(), key=sort_key)
    sorted_other = sorted(other.keys(), key=sort_key)

    print(f"Number of schemas in '{input_file}': {len(nodes) + len(edges) + len(other)}")
    print(f"Number of nodes: {len(nodes)}")
    print(f"Number of edges: {len(edges)}")
    if other:
        print(f"WARNING: {len(other)} schema(s) have no represented_as: node/edge — kept in an "
              f"UNCATEGORIZED section instead of being dropped: {sorted_other}")

    print("\nNodes (sorted):")
    for schema in sorted_nodes:
        print(f"  {schema}")

    print("\nEdges (sorted):")
    for schema in sorted_edges:
        print(f"  {schema}")

    # Generate output file name
    base_name = os.path.basename(input_file)
    name, ext = os.path.splitext(base_name)
    output_file = os.path.join(os.path.dirname(input_file), f"{name}_sorted{ext}")

    # Write output file
    write_output_file(output_file, title, {**nodes, **edges, **other}, sorted_nodes, sorted_edges, sorted_other)


if __name__ == "__main__":
    main()
