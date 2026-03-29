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

def write_output_file(file_path, title, data_dict, sorted_nodes, sorted_edges):
    """Write output file with nodes first, then edges"""
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
                if i < len(sorted_edges) - 1:
                    f.write('\n')  # Blank line between schemas (except last)
    except Exception as e:
        print(f"Error: Failed to write to '{file_path}': {e}")
        sys.exit(1)

def categorize_schemas(schemas_dict):
    """Separate schemas into nodes and edges based on 'represented_as' field"""
    nodes = {}
    edges = {}
    
    for schema_name, schema_content in schemas_dict.items():
        if isinstance(schema_content, dict) and schema_content.get('represented_as') == 'node':
            nodes[schema_name] = schema_content
        elif isinstance(schema_content, dict) and schema_content.get('represented_as') == 'edge':
            edges[schema_name] = schema_content
    
    return nodes, edges

def main():
    if len(sys.argv) != 4:
        print("Usage: python script.py <input1.yaml> <input2.yaml> <output.yaml>")
        sys.exit(1)
    
    input1_path = sys.argv[1]
    input2_path = sys.argv[2]
    output_path = sys.argv[3]
    
    # Load YAML files
    data1 = load_yaml(input1_path)
    data2 = load_yaml(input2_path)
    
    if not isinstance(data1, dict) or not isinstance(data2, dict):
        print("Error: YAML files must contain dictionaries.")
        sys.exit(1)
    
    # Extract title and schemas
    title1 = data1.get('Title', 'BioCypher schema')
    title2 = data2.get('Title', 'BioCypher schema')
    
    schemas1 = {k: v for k, v in data1.items() if k != 'Title'}
    schemas2 = {k: v for k, v in data2.items() if k != 'Title'}
    
    print(f"Number of schemas in {input1_path}: {len(schemas1)}")
    print(f"Number of schemas in {input2_path}: {len(schemas2)}")
    
    # Find common and unique schemas
    common_keys = set(schemas1.keys()) & set(schemas2.keys())
    unique1_keys = set(schemas1.keys()) - common_keys
    unique2_keys = set(schemas2.keys()) - common_keys
    
    print(f"Number of common schemas: {len(common_keys)}")
    
    # 
    # Write prime (common) schemas
    # 
    common_schemas = {k: schemas1[k] for k in common_keys}
    nodes_common, edges_common = categorize_schemas(common_schemas)
    sorted_nodes_common = sorted(nodes_common.keys(), key=sort_key)
    sorted_edges_common = sorted(edges_common.keys(), key=sort_key)
    
    write_output_file(output_path, title1, common_schemas, sorted_nodes_common, sorted_edges_common)
    
    print(f"\n✅ Common schemas written to '{output_path}'")
    print(f"  Number of nodes: {len(nodes_common)}")
    print(f"  Number of edges: {len(edges_common)}")
    print(f"  Total schemas: {len(common_schemas)}")
    
    # 
    # Write unique schemas for input1
    # 
    unique1_schemas = {k: schemas1[k] for k in unique1_keys}
    nodes_unique1, edges_unique1 = categorize_schemas(unique1_schemas)
    sorted_nodes_unique1 = sorted(nodes_unique1.keys(), key=sort_key)
    sorted_edges_unique1 = sorted(edges_unique1.keys(), key=sort_key)
    
    unique1_file = os.path.join(os.path.dirname(input1_path), 'unique_' + os.path.basename(input1_path))
    write_output_file(unique1_file, title1, unique1_schemas, sorted_nodes_unique1, sorted_edges_unique1)
    
    print(f"\n✅ Unique schemas for {input1_path} written to '{unique1_file}'")
    print(f"  Number of nodes: {len(nodes_unique1)}")
    print(f"  Number of edges: {len(edges_unique1)}")
    print(f"  Total schemas: {len(unique1_schemas)}")
    
    # 
    # Write unique schemas for input2
    # 
    unique2_schemas = {k: schemas2[k] for k in unique2_keys}
    nodes_unique2, edges_unique2 = categorize_schemas(unique2_schemas)
    sorted_nodes_unique2 = sorted(nodes_unique2.keys(), key=sort_key)
    sorted_edges_unique2 = sorted(edges_unique2.keys(), key=sort_key)
    
    unique2_file = os.path.join(os.path.dirname(input2_path), 'unique_' + os.path.basename(input2_path))
    write_output_file(unique2_file, title2, unique2_schemas, sorted_nodes_unique2, sorted_edges_unique2)
    
    print(f"\n✅ Unique schemas for {input2_path} written to '{unique2_file}'")
    print(f"  Number of nodes: {len(nodes_unique2)}")
    print(f"  Number of edges: {len(edges_unique2)}")
    print(f"  Total schemas: {len(unique2_schemas)}")

if __name__ == "__main__":
    main()

# import sys
# import yaml
# import os

# # Configure YAML to add blank lines between top-level keys
# class CustomDumper(yaml.SafeDumper):
#     pass

# def represent_none(self, _):
#     return self.represent_scalar('tag:yaml.org,2002:null', '')

# CustomDumper.add_representer(type(None), represent_none)

# def load_yaml(file_path):
#     try:
#         with open(file_path, 'r') as f:
#             return yaml.safe_load(f)
#     except FileNotFoundError:
#         print(f"Error: File '{file_path}' not found.")
#         sys.exit(1)
#     except yaml.YAMLError as e:
#         print(f"Error: Invalid YAML in '{file_path}': {e}")
#         sys.exit(1)

# def main():
#     if len(sys.argv) != 4:
#         print("Usage: python reorganize_schema.py <input1.yaml> <input2.yaml> <output.yaml>")
#         sys.exit(1)
    
#     input1 = sys.argv[1]
#     input2 = sys.argv[2]
#     output = sys.argv[3]
#     # output = 'config/primer_schema_config.yaml'
    
#     data1 = load_yaml(input1)
#     data2 = load_yaml(input2)
    
#     if not isinstance(data1, dict) or not isinstance(data2, dict):
#         print("Error: YAML files must contain dictionaries.")
#         sys.exit(1)
    
#     # Filter out 'Title' key - it's metadata, not a schema
#     schemas1 = [k for k in data1.keys() if k != 'Title']
#     schemas2 = [k for k in data2.keys() if k != 'Title']
    
#     print(f"Number of schemas in {input1}: {len(schemas1)}")
#     print(f"Number of schemas in {input2}: {len(schemas2)}")
    
#     sorted_schemas1 = sorted(schemas1)
#     sorted_schemas2 = sorted(schemas2)
    
#     common_schemas = set(schemas1) & set(schemas2)
#     print(f"Number of common schemas: {len(common_schemas)}")
    
#     def sort_key(schema):
#         if schema[0].isdigit():
#             return ('0', schema.lower())
#         else:
#             return ('1', schema.lower())
    
#     sorted_common = sorted(common_schemas, key=sort_key)
    
#     # print("\nCommon schemas definitions:")
#     # for schema in sorted_common:
#     #     print(f"{schema}: {data1[schema]}")
#     #     print()  # Blank line
    
#     try:
#         with open(output, 'w') as f:
#             # Write Title first if it exists in data1
#             if 'Title' in data1:
#                 yaml.dump(
#                     {'Title': 'Title: BioCypher graph schema configuration file (Biolink-adapted for all species)'},  #data1['Title']},
#                     f,
#                     default_flow_style=False,
#                     sort_keys=False
#                 )
#                 f.write('\n')  # Blank line after Title
            
#             # Write each schema separately with blank line separation
#             for i, schema in enumerate(sorted_common):
#                 yaml.dump(
#                     {schema: data1[schema]},
#                     f,
#                     default_flow_style=False,
#                     sort_keys=False
#                 )
#                 # Add blank line after each schema (except the last one)
#                 if i < len(sorted_common) - 1:
#                     f.write('\n')
        
#         print(f"Common schemas written to {output}")
#         print(f"Number of schemas written to '{output}': {len(sorted_common)}")
#     except Exception as e:
#         print(f"Error writing to {output}: {e}")
#         sys.exit(1)
    
#     # Compute unique schemas
#     unique_schemas1 = set(schemas1) - set(schemas2)
#     unique_schemas2 = set(schemas2) - set(schemas1)
    
#     sorted_unique1 = sorted(unique_schemas1, key=sort_key)
#     sorted_unique2 = sorted(unique_schemas2, key=sort_key)
    
#     # Generate unique file for input1
#     unique_file1 = os.path.join(os.path.dirname(input1), 'unique_' + os.path.basename(input1))
#     # unique_file1 = os.path.join(os.path.dirname(input1), os.path.basename(input1))
#     try:
#         with open(unique_file1, 'w') as f:
#             # Write Title
#             if 'Title' in data1:
#                 yaml.dump(
#                     {'Title': data1['Title']},
#                     f,
#                     default_flow_style=False,
#                     sort_keys=False
#                 )
#                 f.write('\n')
            
#             # # Include common file
#             # yaml.dump(
#             #     {'$include': output},
#             #     f,
#             #     default_flow_style=False,
#             #     sort_keys=False
#             # )
#             # f.write('\n')
            
#             # Write unique schemas
#             for i, schema in enumerate(sorted_unique1):
#                 yaml.dump(
#                     {schema: data1[schema]},
#                     f,
#                     default_flow_style=False,
#                     sort_keys=False
#                 )
#                 if i < len(sorted_unique1) - 1:
#                     f.write('\n')
        
#         print(f"Unique schemas for {input1} written to {unique_file1}")
#         print(f"Number of schemas written to '{unique_file1}': {len(sorted_unique1)}")
#     except Exception as e:
#         print(f"Error writing to {unique_file1}: {e}")
#         sys.exit(1)
    
#     # Generate unique file for input2
#     unique_file2 = os.path.join(os.path.dirname(input2), 'unique_' + os.path.basename(input2))
#     # unique_file2 = os.path.join(os.path.dirname(input2), os.path.basename(input2))
#     try:
#         with open(unique_file2, 'w') as f:
#             # Write Title
#             if 'Title' in data2:
#                 yaml.dump(
#                     {'Title': data2['Title']},
#                     f,
#                     default_flow_style=False,
#                     sort_keys=False
#                 )
#                 f.write('\n')
            
#             # # Include common file
#             # yaml.dump(
#             #     {'$include': output},
#             #     f,
#             #     default_flow_style=False,
#             #     sort_keys=False
#             # )
#             # f.write('\n')
            
#             # Write unique schemas
#             for i, schema in enumerate(sorted_unique2):
#                 yaml.dump(
#                     {schema: data2[schema]},
#                     f,
#                     default_flow_style=False,
#                     sort_keys=False
#                 )
#                 if i < len(sorted_unique2) - 1:
#                     f.write('\n')
        
#         print(f"Unique schemas for {input2} written to {unique_file2}")
#         print(f"Number of schemas written to '{unique_file2}': {len(sorted_unique2)}")
#     except Exception as e:
#         print(f"Error writing to {unique_file2}: {e}")
#         sys.exit(1)
        
# if __name__ == "__main__":
#     main()