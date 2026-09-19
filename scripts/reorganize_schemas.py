"""
Split two BioCypher schema YAML files into:
  - a "common" file: schemas with the same name AND identical content in both inputs
  - "unique_<input1>": everything only input1 needs (names unique to it, plus its own
    version of any name that exists in both but with DIFFERENT content)
  - "unique_<input2>": same for input2

A schema name shared by both inputs is only ever merged into the common file when
its content is byte-for-byte identical (as parsed YAML). If the same name carries
different content in each file, both versions are preserved as-is in their
respective "unique" output — never silently collapsed to one of them — and the
divergence is printed so a human can review it (it usually means one side made a
legitimate species-specific customization, e.g. added properties, or that the two
copies have drifted and one of them is stale).
"""

import yaml
import sys
import os


def sort_key(schema):
    """Sort key: digits first, then alphabetically (case-insensitive)"""
    if schema[0].isdigit():
        return ('0', schema.lower())
    else:
        return ('1', schema.lower())


def load_yaml(file_path):
    """Load YAML file with error handling"""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Failed to parse YAML in '{file_path}': {e}")
        sys.exit(1)


def find_duplicate_top_level_keys(file_path):
    """
    yaml.safe_load silently keeps only the last occurrence of a repeated top-level
    key. Scan the raw text so a duplicate (dead, shadowed) block doesn't go
    unnoticed while we reorganize around whatever the parser kept.
    """
    import re
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


def write_output_file(file_path, title, data_dict, sorted_nodes, sorted_edges, sorted_other=None):
    """Write output file with nodes first, then edges, then anything uncategorized"""
    sorted_other = sorted_other or []
    try:
        with open(file_path, 'w') as f:
            if title:
                yaml.dump({'Title': title}, f, default_flow_style=False, sort_keys=False)
                f.write('\n')

            f.write('\n\n##################             NODES   SECTION             ####################\n\n')
            for schema_name in sorted_nodes:
                yaml.dump({schema_name: data_dict[schema_name]}, f, default_flow_style=False, sort_keys=False)
                f.write('\n')

            f.write('\n\n##################             EDGES   SECTION             ####################\n\n')
            for i, schema_name in enumerate(sorted_edges):
                yaml.dump({schema_name: data_dict[schema_name]}, f, default_flow_style=False, sort_keys=False)
                if i < len(sorted_edges) - 1 or sorted_other:
                    f.write('\n')

            if sorted_other:
                f.write('\n\n##################   UNCATEGORIZED (no represented_as: node/edge)   ##########\n\n')
                for i, schema_name in enumerate(sorted_other):
                    yaml.dump({schema_name: data_dict[schema_name]}, f, default_flow_style=False, sort_keys=False)
                    if i < len(sorted_other) - 1:
                        f.write('\n')
    except Exception as e:
        print(f"Error: Failed to write to '{file_path}': {e}")
        sys.exit(1)


def categorize_schemas(schemas_dict):
    """Separate schemas into nodes/edges/other based on 'represented_as'. Nothing
    is dropped: anything that isn't explicitly 'node' or 'edge' goes to 'other'
    instead of vanishing, so a missing/typo'd represented_as is visible in the
    output rather than silently losing the type."""
    nodes, edges, other = {}, {}, {}
    for schema_name, schema_content in schemas_dict.items():
        represented_as = isinstance(schema_content, dict) and schema_content.get('represented_as')
        if represented_as == 'node':
            nodes[schema_name] = schema_content
        elif represented_as == 'edge':
            edges[schema_name] = schema_content
        else:
            other[schema_name] = schema_content
    return nodes, edges, other


def write_report(label, nodes, edges, other):
    print(f"  Nodes: {len(nodes)}  Edges: {len(edges)}  Total: {len(nodes) + len(edges) + len(other)}")
    if other:
        print(f"  WARNING: {len(other)} schema(s) have no represented_as: node/edge and were "
              f"kept in an UNCATEGORIZED section instead of being dropped: {sorted(other)}")


def main():
    if len(sys.argv) != 4:
        print("Usage: python reorganize_schemas.py <input1.yaml> <input2.yaml> <output.yaml>")
        sys.exit(1)

    input1_path, input2_path, output_path = sys.argv[1:4]

    for path in (input1_path, input2_path):
        dups = find_duplicate_top_level_keys(path)
        if dups:
            print(f"WARNING: '{path}' has {len(dups)} duplicate top-level key(s) — only the LAST "
                  f"occurrence of each survives YAML parsing, the earlier one(s) are dead/shadowed:")
            for name, linenos in sorted(dups.items()):
                print(f"  - '{name}' at lines {linenos}")
            print("  Fix these before trusting this reorganization's output.\n")

    data1 = load_yaml(input1_path)
    data2 = load_yaml(input2_path)

    if not isinstance(data1, dict) or not isinstance(data2, dict):
        print("Error: YAML files must contain dictionaries.")
        sys.exit(1)

    title1 = data1.get('Title', 'BioCypher schema')
    title2 = data2.get('Title', 'BioCypher schema')

    schemas1 = {k: v for k, v in data1.items() if k != 'Title'}
    schemas2 = {k: v for k, v in data2.items() if k != 'Title'}

    print(f"Number of schemas in {input1_path}: {len(schemas1)}")
    print(f"Number of schemas in {input2_path}: {len(schemas2)}")

    shared_names = set(schemas1) & set(schemas2)
    identical_names = {k for k in shared_names if schemas1[k] == schemas2[k]}
    diverged_names = shared_names - identical_names
    unique1_names = set(schemas1) - shared_names
    unique2_names = set(schemas2) - shared_names

    print(f"Shared name, identical content (-> common file): {len(identical_names)}")
    print(f"Shared name, DIFFERENT content (kept in both unique files, not merged): {len(diverged_names)}")
    if diverged_names:
        for k in sorted(diverged_names, key=sort_key):
            print(f"    - {k!r}")

    # Common (merged) output: only truly identical schemas.
    common_schemas = {k: schemas1[k] for k in identical_names}
    nodes_c, edges_c, other_c = categorize_schemas(common_schemas)
    write_output_file(output_path, title1, common_schemas,
                       sorted(nodes_c, key=sort_key), sorted(edges_c, key=sort_key), sorted(other_c, key=sort_key))
    print(f"\n✅ Common schemas written to '{output_path}'")
    write_report(output_path, nodes_c, edges_c, other_c)

    # input1's own file: names unique to it, plus its own version of any diverged name.
    unique1_schemas = {k: schemas1[k] for k in unique1_names | diverged_names}
    nodes_u1, edges_u1, other_u1 = categorize_schemas(unique1_schemas)
    unique1_file = os.path.join(os.path.dirname(input1_path), 'unique_' + os.path.basename(input1_path))
    write_output_file(unique1_file, title1, unique1_schemas,
                       sorted(nodes_u1, key=sort_key), sorted(edges_u1, key=sort_key), sorted(other_u1, key=sort_key))
    print(f"\n✅ Unique schemas for {input1_path} written to '{unique1_file}'")
    write_report(unique1_file, nodes_u1, edges_u1, other_u1)

    # input2's own file: same idea.
    unique2_schemas = {k: schemas2[k] for k in unique2_names | diverged_names}
    nodes_u2, edges_u2, other_u2 = categorize_schemas(unique2_schemas)
    unique2_file = os.path.join(os.path.dirname(input2_path), 'unique_' + os.path.basename(input2_path))
    write_output_file(unique2_file, title2, unique2_schemas,
                       sorted(nodes_u2, key=sort_key), sorted(edges_u2, key=sort_key), sorted(other_u2, key=sort_key))
    print(f"\n✅ Unique schemas for {input2_path} written to '{unique2_file}'")
    write_report(unique2_file, nodes_u2, edges_u2, other_u2)

    total_out = len(common_schemas) + len(unique1_schemas) + len(unique2_schemas)
    total_in = len(set(schemas1) | set(schemas2))
    if total_out - len(diverged_names) != total_in:
        print(f"\nWARNING: input had {total_in} distinct names, output accounts for "
              f"{total_out - len(diverged_names)} (after de-duplicating the deliberately-duplicated "
              f"diverged names) — investigate before trusting these files.")


if __name__ == "__main__":
    main()
