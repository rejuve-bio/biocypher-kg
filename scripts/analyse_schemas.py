
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
#         print("Usage: python script.py <input1.yaml> <input2.yaml> <output.yaml>")
#         sys.exit(1)
    
#     input1 = sys.argv[1]
#     input2 = sys.argv[2]
#     output = sys.argv[3]
    
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
    
#     print("\nCommon schemas definitions:")
#     for schema in sorted_common:
#         print(f"{schema}: {data1[schema]}")
#         print()  # Blank line
    
#     try:
#         with open(output, 'w') as f:
#             # Write Title first if it exists in data1
#             if 'Title' in data1:
#                 yaml.dump(
#                     {'Title': data1['Title']},
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
            
#             # Include common file
#             yaml.dump(
#                 {'$include': output},
#                 f,
#                 default_flow_style=False,
#                 sort_keys=False
#             )
#             f.write('\n')
            
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
            
#             # Include common file
#             yaml.dump(
#                 {'$include': output},
#                 f,
#                 default_flow_style=False,
#                 sort_keys=False
#             )
#             f.write('\n')
            
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