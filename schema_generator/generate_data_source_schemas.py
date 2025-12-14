#!/usr/bin/env python3
"""
Automated Data Source Schema Generator v4

Generates data source schema YAML files by analyzing:
1. Adapter implementations (to extract properties)
2. Schema configuration (to get descriptions and validate properties)
3. Adapter configuration (to map adapters to data sources)

Key features:
- Groups by actual adapter provenance (self.source)
- Uses specific source_url for each node/relationship
- Handles all property assignment patterns (props={}, props[], props.update())
- One schema file per unique data source
"""

import argparse
import yaml
import ast
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Any, Optional
import urllib.parse


class FlowStyleListDumper(yaml.SafeDumper):
    """Custom YAML dumper that outputs lists in flow style (rectangular brackets)."""
    pass


def represent_list(dumper, data):
    """Represent lists in flow style for source/target fields."""
    return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)


FlowStyleListDumper.add_representer(list, represent_list)


class AdapterAnalyzer:
    def __init__(self, adapter_path: Path):
        self.adapter_path = adapter_path
        self.source_code = adapter_path.read_text()
        self.tree = ast.parse(self.source_code)
        self.class_attributes = self._extract_class_attributes()

    def _extract_class_attributes(self) -> Dict[str, Any]:
        class_attrs = {}
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ClassDef):
                for item in node.body:
                    if isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name):
                                if isinstance(item.value, ast.Dict):
                                    dict_value = {}
                                    for key, value in zip(item.value.keys, item.value.values):
                                        if isinstance(key, ast.Constant) and isinstance(value, ast.Constant):
                                            dict_value[key.value] = value.value
                                    if dict_value:
                                        class_attrs[target.id] = dict_value
                                elif isinstance(item.value, ast.Constant):
                                    class_attrs[target.id] = item.value.value
        return class_attrs

    def inherits_from_ontology_adapter(self) -> bool:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    if isinstance(base, ast.Name) and base.id == 'OntologyAdapter':
                        return True
                    if isinstance(base, ast.Attribute) and base.attr == 'OntologyAdapter':
                        return True
        return False

    def get_metadata_from_init(self) -> Dict[str, Any]:
        metadata = {}
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ClassDef):
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == '__init__':
                        # Extract default values from function signature
                        if item.args.defaults:
                            arg_names = [arg.arg for arg in item.args.args]
                            num_defaults = len(item.args.defaults)
                            default_start_idx = len(arg_names) - num_defaults
                            for i, default in enumerate(item.args.defaults):
                                arg_idx = default_start_idx + i
                                arg_name = arg_names[arg_idx]
                                if arg_name in ['label', 'version'] and isinstance(default, ast.Constant):
                                    if arg_name not in metadata:
                                        metadata[arg_name] = default.value

                        # Extract from assignments (these override defaults)
                        for stmt in ast.walk(item):
                            if isinstance(stmt, ast.Assign):
                                for target in stmt.targets:
                                    if isinstance(target, ast.Attribute):
                                        if target.attr in ['source', 'source_url', 'version', 'label']:
                                            if isinstance(stmt.value, ast.Constant):
                                                metadata[target.attr] = stmt.value.value
                                            elif isinstance(stmt.value, ast.JoinedStr):
                                                url_parts = []
                                                for value in stmt.value.values:
                                                    if isinstance(value, ast.Constant):
                                                        url_parts.append(str(value.value))
                                                if url_parts:
                                                    metadata[target.attr] = ''.join(url_parts)
                                            elif isinstance(stmt.value, ast.Subscript):
                                                if isinstance(stmt.value.value, ast.Attribute):
                                                    dict_name = stmt.value.value.attr
                                                    if dict_name in self.class_attributes:
                                                        dict_value = self.class_attributes[dict_name]
                                                        if isinstance(dict_value, dict) and dict_value:
                                                            metadata[target.attr] = next(iter(dict_value.values()))
        return metadata

    def extract_properties_from_dict(self, dict_node: ast.Dict) -> Set[str]:
        properties = set()
        for key in dict_node.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                if key.value not in ['source', 'source_url']:
                    properties.add(key.value)
        return properties

    def get_properties_from_method(self, method_name: str) -> Set[str]:
        properties = set()
        for node in ast.walk(self.tree):
            if isinstance(node, ast.FunctionDef) and node.name == method_name:
                for stmt in ast.walk(node):
                    # Pattern 1: props = {...}
                    if isinstance(stmt, ast.Assign):
                        for target in stmt.targets:
                            if isinstance(target, ast.Name) and target.id in ['props', '_props', 'properties']:
                                if isinstance(stmt.value, ast.Dict):
                                    properties.update(self.extract_properties_from_dict(stmt.value))

                    # Pattern 2: props['key'] = value
                    if isinstance(stmt, ast.Assign):
                        for target in stmt.targets:
                            if isinstance(target, ast.Subscript):
                                if isinstance(target.value, ast.Name) and target.value.id in ['props', '_props', 'properties']:
                                    if isinstance(target.slice, ast.Constant):
                                        prop_name = target.slice.value
                                        if prop_name not in ['source', 'source_url']:
                                            properties.add(prop_name)

                    # Pattern 3: props.update({...})
                    if isinstance(stmt, ast.Expr):
                        if isinstance(stmt.value, ast.Call):
                            if isinstance(stmt.value.func, ast.Attribute):
                                if (stmt.value.func.attr == 'update' and
                                    isinstance(stmt.value.func.value, ast.Name) and
                                    stmt.value.func.value.id in ['props', '_props', 'properties']):
                                    if stmt.value.args and isinstance(stmt.value.args[0], ast.Dict):
                                        properties.update(self.extract_properties_from_dict(stmt.value.args[0]))
        return properties

    def get_all_class_properties(self) -> Set[str]:
        """Extract properties from all methods in the class (including helper methods)."""
        properties = set()
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ClassDef):
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        for stmt in ast.walk(item):
                            # Pattern 1: props = {...}
                            if isinstance(stmt, ast.Assign):
                                for target in stmt.targets:
                                    if isinstance(target, ast.Name) and target.id in ['props', '_props', 'properties']:
                                        if isinstance(stmt.value, ast.Dict):
                                            properties.update(self.extract_properties_from_dict(stmt.value))

                            # Pattern 2: props['key'] = value
                            if isinstance(stmt, ast.Assign):
                                for target in stmt.targets:
                                    if isinstance(target, ast.Subscript):
                                        if isinstance(target.value, ast.Name) and target.value.id in ['props', '_props', 'properties']:
                                            if isinstance(target.slice, ast.Constant):
                                                prop_name = target.slice.value
                                                if prop_name not in ['source', 'source_url']:
                                                    properties.add(prop_name)

                            # Pattern 3: props.update({...})
                            if isinstance(stmt, ast.Expr):
                                if isinstance(stmt.value, ast.Call):
                                    if isinstance(stmt.value.func, ast.Attribute):
                                        if (stmt.value.func.attr == 'update' and
                                            isinstance(stmt.value.func.value, ast.Name) and
                                            stmt.value.func.value.id in ['props', '_props', 'properties']):
                                            if stmt.value.args and isinstance(stmt.value.args[0], ast.Dict):
                                                properties.update(self.extract_properties_from_dict(stmt.value.args[0]))
        return properties

    def get_node_properties(self) -> Set[str]:
        """Get properties from get_nodes method and all helper methods."""
        main_props = self.get_properties_from_method('get_nodes')
        all_props = self.get_all_class_properties()
        return main_props.union(all_props)

    def get_edge_properties(self) -> Set[str]:
        """Get properties from get_edges method and all helper methods."""
        main_props = self.get_properties_from_method('get_edges')
        all_props = self.get_all_class_properties()
        return main_props.union(all_props)

    def get_parent_class_properties(self, method_name: str, parent_adapter_path: Path) -> Set[str]:
        if not parent_adapter_path.exists():
            return set()
        try:
            parent_analyzer = AdapterAnalyzer(parent_adapter_path)
            return parent_analyzer.get_properties_from_method(method_name)
        except Exception as e:
            print(f"Warning: Could not analyze parent class {parent_adapter_path}: {e}")
            return set()


class SchemaGenerator:
    def __init__(self, schema_config_path: str, adapter_config_path: str,
                 adapters_dir: str, output_dir: str):
        self.schema_config_path = Path(schema_config_path)
        self.adapter_config_path = Path(adapter_config_path)
        self.adapters_dir = Path(adapters_dir)
        self.output_dir = Path(output_dir)

        with open(self.schema_config_path) as f:
            self.schema_config = yaml.safe_load(f)
        with open(self.adapter_config_path) as f:
            self.adapter_config = yaml.safe_load(f)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.adapter_cache = {}

    def get_schema_type_info(self, input_label: str) -> Optional[Dict]:
        for type_name, type_config in self.schema_config.items():
            if not isinstance(type_config, dict):
                continue
            if type_config.get('input_label') == input_label:
                return {
                    'name': type_name,
                    'config': type_config
                }
        return None

    def get_type_by_name(self, type_name: str) -> Optional[Dict]:
        if type_name in self.schema_config:
            config = self.schema_config[type_name]
            if isinstance(config, dict):
                return {'name': type_name, 'config': config}
        return None

    def get_all_properties_for_type(self, type_name: str, visited: Set[str] = None) -> Dict[str, Any]:
        if visited is None:
            visited = set()
        if type_name in visited:
            return {}
        visited.add(type_name)

        type_info = self.get_type_by_name(type_name)
        if not type_info:
            return {}

        config = type_info['config']
        all_props = {}

        if config.get('inherit_properties', False):
            parent_type = config.get('is_a')
            if parent_type:
                if isinstance(parent_type, str):
                    parent_types = [parent_type]
                elif isinstance(parent_type, list):
                    parent_types = parent_type
                else:
                    parent_types = []
                for parent in parent_types:
                    parent_props = self.get_all_properties_for_type(parent, visited)
                    all_props.update(parent_props)

        direct_props = config.get('properties', {})
        all_props.update(direct_props)
        return all_props

    def get_valid_properties(self, input_label: str, adapter_props: Set[str]) -> Dict[str, str]:
        type_info = self.get_schema_type_info(input_label)
        if not type_info:
            return {}

        type_name = type_info['name']
        schema_props = self.get_all_properties_for_type(type_name)
        valid_props = {}

        for prop in adapter_props:
            if prop in schema_props:
                prop_config = schema_props[prop]
                if isinstance(prop_config, dict):
                    prop_type = prop_config.get('type', 'str')
                else:
                    prop_type = 'str'
                valid_props[prop] = prop_type

        return valid_props

    def extract_base_url(self, url: str) -> str:
        if not url:
            return ""
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return url

    def analyze_adapter_file(self, adapter_file: str) -> Optional[Dict]:
        if adapter_file in self.adapter_cache:
            return self.adapter_cache[adapter_file]

        adapter_path = self.adapters_dir / adapter_file
        if not adapter_path.exists():
            print(f"Warning: Adapter file not found: {adapter_path}")
            return None

        analyzer = AdapterAnalyzer(adapter_path)
        metadata = analyzer.get_metadata_from_init()

        source = metadata.get('source', adapter_file.replace('_adapter.py', '').replace('_', ' ').title())
        source_url = metadata.get('source_url', '')

        # Override source for ontology adapters to group them under OBO Foundry
        if analyzer.inherits_from_ontology_adapter():
            source = 'OBO Foundry'
            if not source_url or 'obolibrary.org' in source_url or 'ebi.ac.uk/efo' in source_url:
                source_url = 'http://www.obofoundry.org/'

        result = {
            'analyzer': analyzer,
            'metadata': metadata,
            'source_name': source,
            'source_url': source_url,
        }

        self.adapter_cache[adapter_file] = result
        return result

    def generate_all_schemas(self, filter_adapters: Optional[List[str]] = None, filter_modules: Optional[List[str]] = None, filter_sources: Optional[List[str]] = None):
        if filter_adapters:
            print(f"Analyzing specific adapters: {', '.join(filter_adapters)}")
        elif filter_modules:
            print(f"Analyzing adapters by module: {', '.join(filter_modules)}")
        elif filter_sources:
            print(f"Analyzing specific data sources: {', '.join(filter_sources)}")
        else:
            print(f"Analyzing adapters and generating schemas...")

        by_source = defaultdict(list)

        for adapter_name, config in self.adapter_config.items():
            # Filter by adapter name if specified
            if filter_adapters and adapter_name not in filter_adapters:
                continue

            adapter_info = config.get('adapter', {})
            module = adapter_info.get('module', '')
            cls = adapter_info.get('cls', '')

            if not module or not cls:
                continue

            # Filter by module name if specified
            module_name = module.split('.')[-1]
            if filter_modules and module_name not in filter_modules:
                continue

            adapter_file = module_name + '.py'
            adapter_data = self.analyze_adapter_file(adapter_file)
            if not adapter_data:
                continue

            source_name = adapter_data['source_name']

            # Filter by source name if specified
            if filter_sources and source_name not in filter_sources:
                continue

            by_source[source_name].append({
                'adapter_name': adapter_name,
                'adapter_file': adapter_file,
                'config': config,
                'adapter_data': adapter_data
            })

        if not by_source:
            print(f"\n⚠ No adapters matched the filter criteria")
            return

        for source_name, adapters in sorted(by_source.items()):
            print(f"\nGenerating schema for: {source_name}")
            self.generate_schema(source_name, adapters)

        print(f"\n✓ Schema generation complete! Output directory: {self.output_dir}")
        print(f"  Generated {len(by_source)} schema files")

    def generate_schema(self, source_name: str, adapters: List[Dict]):
        source_urls = [a['adapter_data']['source_url'] for a in adapters if a['adapter_data']['source_url']]
        website = self.extract_base_url(source_urls[0]) if source_urls else ''

        # Check if output file already exists
        output_filename = source_name.replace(' ', '_').replace('-', '_').replace('/', '_') + '.yaml'
        output_path = self.output_dir / output_filename

        # Load existing schema if it exists
        if output_path.exists():
            with open(output_path, 'r') as f:
                existing_schema = yaml.safe_load(f)
            nodes = existing_schema.get('nodes', {})
            relationships = existing_schema.get('relationships', {})
            schema = {
                'name': existing_schema.get('name', source_name),
                'website': existing_schema.get('website', website)
            }
        else:
            schema = {
                'name': source_name,
            }
            if website:
                schema['website'] = website
            nodes = {}
            relationships = {}

        for adapter_info in adapters:
            adapter_name = adapter_info['adapter_name']
            adapter_cfg = adapter_info['config']
            adapter_data = adapter_info['adapter_data']
            analyzer = adapter_data['analyzer']

            writes_nodes = adapter_cfg.get('nodes', False)
            writes_edges = adapter_cfg.get('edges', False)
            is_ontology_adapter = analyzer.inherits_from_ontology_adapter()
            adapter_args = adapter_cfg.get('adapter', {}).get('args', {})

            label = adapter_args.get('label')
            if not label:
                label = adapter_data['metadata'].get('label', adapter_name)

            if is_ontology_adapter:
                ontology_type = adapter_args.get('type', '')
                if ontology_type == 'node':
                    pass

            type_info = self.get_schema_type_info(label)
            if not type_info:
                print(f"  Warning: No schema config found for label: {label} (adapter: {adapter_name})")
                continue

            type_config = type_info['config']
            type_name = type_info['name']
            is_edge = type_config.get('represented_as') == 'edge'

            adapter_source_url = adapter_data['source_url'] if adapter_data['source_url'] else ''

            # Process nodes
            if writes_nodes and not is_edge:
                node_props = analyzer.get_node_properties()

                if is_ontology_adapter:
                    ontology_adapter_path = self.adapters_dir / 'ontologies_adapter.py'
                    parent_props = analyzer.get_parent_class_properties('get_nodes', ontology_adapter_path)
                    node_props = node_props.union(parent_props)

                valid_props = self.get_valid_properties(label, node_props)
                description = type_config.get('description', '')

                # Add or update node
                if type_name not in nodes:
                    nodes[type_name] = {
                        'url': adapter_source_url,
                        'input_label': label,
                    }
                    if description:
                        nodes[type_name]['description'] = description.strip()
                    if valid_props:
                        nodes[type_name]['properties'] = valid_props
                else:
                    # Merge properties if node already exists
                    if valid_props:
                        if 'properties' not in nodes[type_name]:
                            nodes[type_name]['properties'] = {}
                        nodes[type_name]['properties'].update(valid_props)

            # Process edges
            elif writes_edges and is_edge:
                edge_props = analyzer.get_edge_properties()

                if is_ontology_adapter:
                    ontology_adapter_path = self.adapters_dir / 'ontologies_adapter.py'
                    parent_props = analyzer.get_parent_class_properties('get_edges', ontology_adapter_path)
                    edge_props = edge_props.union(parent_props)

                valid_props = self.get_valid_properties(label, edge_props)
                description = type_config.get('description', '')
                source = type_config.get('source')
                target = type_config.get('target')

                # Add or update relationship
                if type_name not in relationships:
                    relationships[type_name] = {
                        'url': adapter_source_url,
                        'input_label': label,
                    }
                    if description:
                        relationships[type_name]['description'] = description.strip()
                    if source:
                        relationships[type_name]['source'] = source
                    if target:
                        relationships[type_name]['target'] = target
                    if valid_props:
                        relationships[type_name]['properties'] = valid_props
                else:
                    # Merge properties if relationship already exists
                    if valid_props:
                        if 'properties' not in relationships[type_name]:
                            relationships[type_name]['properties'] = {}
                        relationships[type_name]['properties'].update(valid_props)

        if nodes:
            schema['nodes'] = nodes
        if relationships:
            schema['relationships'] = relationships

        if nodes or relationships:
            with open(output_path, 'w') as f:
                yaml.dump(schema, f, Dumper=FlowStyleListDumper, default_flow_style=False, sort_keys=False, allow_unicode=True)

            print(f"  ✓ Generated: {output_filename}")
            print(f"    - Nodes: {len(nodes)}, Relationships: {len(relationships)}")
        else:
            print(f"  Warning: No nodes or relationships found for {source_name}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate data source schema YAML files from adapters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--schema-config', required=True, help='Path to schema configuration YAML file')
    parser.add_argument('--adapter-config', required=True, help='Path to adapter configuration YAML file')
    parser.add_argument('--adapters-dir', required=True, help='Directory containing adapter Python files')
    parser.add_argument('--output-dir', required=True, help='Output directory for generated schema files')
    parser.add_argument('--adapter', action='append', help='Generate schema only for specific adapter(s). Can be used multiple times.')
    parser.add_argument('--module', action='append', help='Generate schema for all adapters using specific module(s). Example: candidate_cis_regulatory_promoter_adapter. Can be used multiple times.')
    parser.add_argument('--source', action='append', help='Generate schema only for specific data source(s). Can be used multiple times.')

    args = parser.parse_args()

    if not Path(args.schema_config).exists():
        print(f"Error: Schema config not found: {args.schema_config}")
        sys.exit(1)
    if not Path(args.adapter_config).exists():
        print(f"Error: Adapter config not found: {args.adapter_config}")
        sys.exit(1)
    if not Path(args.adapters_dir).exists():
        print(f"Error: Adapters directory not found: {args.adapters_dir}")
        sys.exit(1)

    generator = SchemaGenerator(
        args.schema_config,
        args.adapter_config,
        args.adapters_dir,
        args.output_dir
    )

    generator.generate_all_schemas(
        filter_adapters=args.adapter,
        filter_modules=args.module,
        filter_sources=args.source
    )


if __name__ == '__main__':
    main()