#!/usr/bin/env python3
"""
Script that receives a YAML file with BioCypher schemas and generates a '_sorted.yaml' file
with the schemas sorted alphabetically (case-insensitive, digits first).
"""

import sys
import yaml
import os

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

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python script.py <input.yaml>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    # Load YAML
    data = load_yaml(input_file)
    
    if not isinstance(data, dict):
        print("Error: YAML file must contain a dictionary.")
        sys.exit(1)
    
    # Extract schemas (excluding 'Title')
    schemas = [k for k in data.keys() if k != 'Title']
    
    print(f"Number of schemas in '{input_file}': {len(schemas)}")
    
    # Sort schemas
    sorted_schemas = sorted(schemas, key=sort_key)
    
    print("\nSchemas (sorted):")
    for schema in sorted_schemas:
        print(f"  {schema}")
    
    # Generate output file name
    base_name = os.path.basename(input_file)
    name, ext = os.path.splitext(base_name)
    output_file = os.path.join(os.path.dirname(input_file), f"{name}_sorted{ext}")
    
    # Write output file
    try:
        with open(output_file, 'w') as f:
            # Write Title if it exists
            if 'Title' in data:
                yaml.dump(
                    {'Title': data['Title']},
                    f,
                    default_flow_style=False,
                    sort_keys=False
                )
                f.write('\n')
            
            # Write each schema with blank line separation
            for i, schema in enumerate(sorted_schemas):
                yaml.dump(
                    {schema: data[schema]},
                    f,
                    default_flow_style=False,
                    sort_keys=False
                )
                if i < len(sorted_schemas) - 1:
                    f.write('\n')
        
        print(f"\n✅ Sorted schemas written to '{output_file}'")
        print(f"Number of schemas written: {len(sorted_schemas)}")
    
    except Exception as e:
        print(f"Error writing to '{output_file}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()