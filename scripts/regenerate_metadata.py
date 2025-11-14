#!/usr/bin/env python3
import os
import sys
import json
import re
import yaml
import hashlib
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, Set, List, Tuple, Optional
from datetime import datetime

from biocypher import BioCypher


def load_config_file(config_path: str) -> Dict:
    """Load any YAML configuration file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Warning: Could not load config {config_path}: {e}")
        return {}


def load_adapters_config(config_path: str) -> Dict:
    """Load adapters configuration from YAML file."""
    return load_config_file(config_path)


def get_adapter_outdirs(config: Dict) -> Dict[str, str]:
    adapter_outdirs = {}
    for adapter_name, adapter_config in config.items():
        if isinstance(adapter_config, dict) and 'outdir' in adapter_config:
            adapter_outdirs[adapter_name] = adapter_config['outdir']
    return adapter_outdirs


def compute_config_hash(config: Dict) -> str:
    """
    Compute deterministic hash of adapter configuration.
    This is used for versioning and change detection.
    """
    normalized = {}
    for adapter_name, adapter_config in sorted(config.items()):
        if not isinstance(adapter_config, dict):
            continue

        normalized[adapter_name] = {
            'outdir': adapter_config.get('outdir', ''),
            'nodes': adapter_config.get('nodes', False),
            'edges': adapter_config.get('edges', False),
            'args': adapter_config.get('adapter', {}).get('args', {})
        }

    config_json = json.dumps(normalized, sort_keys=True, separators=(',', ':'))

    return hashlib.sha256(config_json.encode('utf-8')).hexdigest()[:16]


def detect_config_changes(old_config_path: str, new_config_path: str) -> Dict:
    old_config = load_adapters_config(old_config_path)
    new_config = load_adapters_config(new_config_path)

    old_adapters = set(old_config.keys())
    new_adapters = set(new_config.keys())

    added_adapters = new_adapters - old_adapters
    removed_adapters = old_adapters - new_adapters
    common_adapters = old_adapters & new_adapters

    modified_adapters = []
    for adapter_name in common_adapters:
        old_conf = old_config[adapter_name]
        new_conf = new_config[adapter_name]

        old_hash = compute_config_hash({adapter_name: old_conf})
        new_hash = compute_config_hash({adapter_name: new_conf})

        if old_hash != new_hash:
            modified_adapters.append(adapter_name)

    removed_outdirs = [
        old_config[adapter]['outdir']
        for adapter in removed_adapters
        if isinstance(old_config[adapter], dict) and 'outdir' in old_config[adapter]
    ]

    return {
        'added': list(added_adapters),
        'removed': list(removed_adapters),
        'modified': modified_adapters,
        'removed_outdirs': removed_outdirs,
        'unchanged': list(common_adapters - set(modified_adapters))
    }


def detect_removed_adapters_from_metadata(old_graph_info: Dict, new_config_path: str, output_dir: str = None) -> List[str]:
    if not output_dir:
        return []

    output_path = Path(output_dir)
    existing_outdirs = set()
    for metadata_file in output_path.rglob('.adapter_metadata.json'):
        outdir = metadata_file.parent.relative_to(output_path)
        existing_outdirs.add(str(outdir))

    new_config = load_adapters_config(new_config_path)
    new_outdirs = set(get_adapter_outdirs(new_config).values())

    removed_outdirs = existing_outdirs - new_outdirs
    return list(removed_outdirs)


def detect_removed_adapters(old_config_path: str, new_config_path: str) -> List[str]:
    changes = detect_config_changes(old_config_path, new_config_path)
    return changes['removed_outdirs']


def map_label_to_output_type(label: str, schema_info: Dict, is_edge: bool = False) -> str:
    label_lower = label.lower()

    if is_edge:
        edge_info = schema_info.get('edge_types', {}).get(label_lower)
        if edge_info:
            return edge_info.get('output_label') or edge_info.get('type', label)
        return label
    else:
        node_info = schema_info.get('node_types', {}).get(label_lower)
        if node_info:
            return node_info.get('type', label)
        return label


def aggregate_adapter_metadata(output_dir: str, adapters_config: Dict, schema_info: Dict = None) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, Set[str]]]:
    node_counts = Counter()
    edge_counts = Counter()
    node_properties = defaultdict(set)

    adapters_found = 0
    adapters_missing = 0

    for adapter_name, adapter_config in adapters_config.items():
        if not isinstance(adapter_config, dict):
            continue

        outdir = adapter_config.get('outdir')
        if not outdir:
            continue

        metadata_path = Path(output_dir) / outdir / '.adapter_metadata.json'

        if metadata_path.exists():
            try:
                with open(metadata_path, 'r') as f:
                    meta = json.load(f)

                for node_label, count in meta.get('node_counts', {}).items():
                    if schema_info:
                        output_type = map_label_to_output_type(node_label, schema_info, is_edge=False)
                    else:
                        output_type = node_label
                    node_counts[output_type] += count

                for edge_label, count in meta.get('edge_counts', {}).items():
                    if schema_info:
                        output_type = map_label_to_output_type(edge_label, schema_info, is_edge=True)
                    else:
                        output_type = edge_label
                    edge_counts[output_type] += count

                for node_label, props in meta.get('properties', {}).items():
                    if schema_info:
                        output_type = map_label_to_output_type(node_label, schema_info, is_edge=False)
                    else:
                        output_type = node_label
                    node_properties[output_type].update(props)

                adapters_found += 1

            except Exception as e:
                print(f"  ⚠️  Warning: Could not load metadata for {adapter_name}: {e}")
                adapters_missing += 1
        else:
            print(f"  ⚠️  Warning: No metadata found for adapter {adapter_name} at {metadata_path}")
            adapters_missing += 1

    print(f"  Aggregated metadata from {adapters_found} adapters")
    if adapters_missing > 0:
        print(f"  ⚠️  {adapters_missing} adapters missing metadata")

    return dict(node_counts), dict(edge_counts), dict(node_properties)


def parse_type_defs(type_defs_path: str) -> Tuple[List[str], List[str]]:
    type_hierarchies = []
    data_constructors = []

    if not Path(type_defs_path).exists():
        return type_hierarchies, data_constructors

    with open(type_defs_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.startswith('(: ') or line.startswith('(<: '):
                type_hierarchies.append(line)
            else:
                data_constructors.append(line)

    return type_hierarchies, data_constructors


def merge_type_defs(old_path: str, new_path: str, output_path: str):
    old_hierarchies, old_constructors = parse_type_defs(old_path)
    new_hierarchies, new_constructors = parse_type_defs(new_path)

    all_hierarchies = set(old_hierarchies + new_hierarchies)
    all_constructors = set(old_constructors + new_constructors)

    with open(output_path, 'w') as f:
        for line in sorted(all_hierarchies):
            f.write(line + '\n')
        for line in sorted(all_constructors):
            f.write(line + '\n')

    print(f"Merged type_defs: {len(all_hierarchies)} hierarchies, {len(all_constructors)} constructors")


def load_schema_info(schema_config_path: str = "config/schema_config.yaml",
                     biocypher_config_path: str = "config/biocypher_config.yaml") -> Dict:
    def convert_input_labels(label, replace_char="_"):
        if isinstance(label, list):
            return [item.replace(" ", replace_char) for item in label]
        return label.replace(" ", replace_char)

    bcy = BioCypher(schema_config_path=schema_config_path, biocypher_config_path=biocypher_config_path)
    schema = bcy._get_ontology_mapping()._extend_schema()

    schema_info = {'node_types': {}, 'edge_types': {}}

    for k, v in schema.items():
        if v.get('abstract', False):
            continue

        if v.get('represented_as') == 'node':
            input_label = v["input_label"]
            if isinstance(input_label, list):
                labels = [convert_input_labels(lbl) for lbl in input_label]
            else:
                labels = [convert_input_labels(input_label)]

            properties = set(v.get('properties', {}).keys())

            for label in labels:
                schema_info['node_types'][label.lower()] = {
                    'type': k,
                    'properties': properties
                }

        elif v.get('represented_as') == 'edge':
            source_type = v.get("source", None)
            target_type = v.get("target", None)

            if source_type is not None and target_type is not None:
                input_label = v["input_label"]
                if isinstance(input_label, list):
                    label = convert_input_labels(input_label[0])
                else:
                    label = convert_input_labels(input_label)

                if isinstance(source_type, list):
                    processed_source = [convert_input_labels(s).lower() for s in source_type]
                else:
                    processed_source = convert_input_labels(source_type).lower()

                if isinstance(target_type, list):
                    processed_target = [convert_input_labels(t).lower() for t in target_type]
                else:
                    processed_target = convert_input_labels(target_type).lower()

                output_label = v.get("output_label", None)
                if output_label:
                    if isinstance(output_label, list):
                        processed_output_label = convert_input_labels(output_label[0]).lower()
                    else:
                        processed_output_label = convert_input_labels(output_label).lower()
                else:
                    processed_output_label = None

                properties = set(v.get('properties', {}).keys())

                schema_info['edge_types'][label.lower()] = {
                    'type': k,
                    'source': processed_source,
                    'target': processed_target,
                    'output_label': processed_output_label,
                    'properties': properties
                }

    print(f"Loaded {len(schema_info['node_types'])} node types, {len(schema_info['edge_types'])} edge types")
    return schema_info


def parse_metta_files(output_dir: str, schema_info: Dict) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, Set[str]]]:
    node_labels = set(schema_info['node_types'].keys())
    edge_labels = set(schema_info['edge_types'].keys())
    for edge_info in schema_info['edge_types'].values():
        if edge_info.get('output_label'):
            edge_labels.add(edge_info['output_label'])

    node_counts = Counter()
    edge_counts = Counter()
    node_properties = defaultdict(set)

    metta_files = [f for f in Path(output_dir).rglob("*.metta") if f.name != "type_defs.metta"]
    print(f"Scanning {len(metta_files)} MeTTa files in {output_dir}")

    for metta_file in metta_files:
        try:
            with open(metta_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith(';'):
                        continue

                    match = re.match(r'\((\S+)\s+(.+)\)$', line)
                    if not match:
                        continue

                    predicate = match.group(1)
                    rest = match.group(2)

                    if predicate in node_labels:
                        if '(' not in rest:
                            node_counts[predicate] += 1
                    elif predicate in edge_labels:
                        if '(' in rest:
                            edge_counts[predicate] += 1
                    else:
                        prop_match = re.match(r'\((\S+)\s+', rest)
                        if prop_match:
                            entity_type = prop_match.group(1)
                            if entity_type in node_labels:
                                node_properties[entity_type].add(predicate)
        except Exception as e:
            print(f"Error parsing {metta_file}: {e}", file=sys.stderr)

    print(f"Found {sum(node_counts.values()):,} nodes, {sum(edge_counts.values()):,} edges")
    return dict(node_counts), dict(edge_counts), dict(node_properties)


def parse_specific_adapters(base_dir: str, adapter_dirs: List[str], schema_info: Dict) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, Set[str]]]:
    node_labels = set(schema_info['node_types'].keys())
    edge_labels = set(schema_info['edge_types'].keys())
    for edge_info in schema_info['edge_types'].values():
        if edge_info.get('output_label'):
            edge_labels.add(edge_info['output_label'])

    node_counts = Counter()
    edge_counts = Counter()
    node_properties = defaultdict(set)

    metta_files = []
    base_path = Path(base_dir)

    for adapter_dir in adapter_dirs:
        adapter_path = base_path / adapter_dir
        if adapter_path.exists():
            adapter_files = [f for f in adapter_path.rglob("*.metta") if f.name != "type_defs.metta"]
            metta_files.extend(adapter_files)

    if not metta_files:
        return dict(node_counts), dict(edge_counts), dict(node_properties)

    for metta_file in metta_files:
        try:
            with open(metta_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith(';'):
                        continue

                    match = re.match(r'\((\S+)\s+(.+)\)$', line)
                    if not match:
                        continue

                    predicate = match.group(1)
                    rest = match.group(2)

                    if predicate in node_labels:
                        if '(' not in rest:
                            node_counts[predicate] += 1
                    elif predicate in edge_labels:
                        if '(' in rest:
                            edge_counts[predicate] += 1
                    else:
                        prop_match = re.match(r'\((\S+)\s+', rest)
                        if prop_match:
                            entity_type = prop_match.group(1)
                            if entity_type in node_labels:
                                node_properties[entity_type].add(predicate)
        except Exception as e:
            print(f"Error parsing {metta_file}: {e}", file=sys.stderr)

    return dict(node_counts), dict(edge_counts), dict(node_properties)


def generate_graph_info(output_dir: str, schema_info: Dict, node_counts: Dict[str, int],
                        edge_counts: Dict[str, int], node_properties: Dict[str, Set[str]]) -> Dict:
    total_nodes = sum(node_counts.values())
    total_edges = sum(edge_counts.values())

    schema_nodes = []
    for node_type, props in node_properties.items():
        schema_nodes.append({
            'data': {
                'name': node_type,
                'properties': sorted(list(props))
            }
        })

    schema_edges = []
    edge_connections = defaultdict(set)

    for edge_label, edge_info in schema_info['edge_types'].items():
        source_types = edge_info['source'] if isinstance(edge_info['source'], list) else [edge_info['source']]
        target_types = edge_info['target'] if isinstance(edge_info['target'], list) else [edge_info['target']]
        output_label = edge_info.get('output_label') or edge_label

        for source in source_types:
            for target in target_types:
                key = f"{source}|{target}"
                edge_connections[key].add(output_label)

    for conn_key, labels in edge_connections.items():
        source, target = conn_key.split('|')
        valid_labels = [l for l in labels if l is not None]
        schema_edges.append({
            'data': {
                'source': source,
                'target': target,
                'possible_connections': sorted(valid_labels)
            }
        })

    predicate_counts = Counter()
    for edge_label, count in edge_counts.items():
        edge_info = schema_info['edge_types'].get(edge_label, {})
        output_label = edge_info.get('output_label') or edge_label
        predicate_counts[output_label] += count

    top_connections = [{'name': pred, 'count': count}
                      for pred, count in predicate_counts.most_common()]

    relationships_freq = Counter()
    for edge_label, count in edge_counts.items():
        edge_info = schema_info['edge_types'].get(edge_label, {})
        source_types = edge_info.get('source', [])
        target_types = edge_info.get('target', [])

        if isinstance(source_types, str):
            source_types = [source_types]
        if isinstance(target_types, str):
            target_types = [target_types]

        for source in source_types:
            for target in target_types:
                relationships_freq[f'{source}|{target}'] += count

    frequent_relationships = [{'entities': rel.split('|'), 'count': count}
                             for rel, count in relationships_freq.most_common()]

    total_size = sum(file.stat().st_size for file in Path(output_dir).rglob('*') if file.is_file())
    total_size_gb = total_size / (1024 ** 3)

    graph_info = {
        'node_count': total_nodes,
        'edge_count': total_edges,
        'dataset_count': 0,
        'data_size': f"{total_size_gb:.2f} GB",
        'top_entities': [{'name': node, 'count': count}
                        for node, count in sorted(node_counts.items(), key=lambda x: x[1], reverse=True)],
        'top_connections': top_connections,
        'frequent_relationships': frequent_relationships,
        'schema': {
            'nodes': schema_nodes,
            'edges': schema_edges
        },
        'datasets': [],
        'metadata': {
            'generated_by': 'regenerate_metadata.py',
            'description': 'Regenerated after incremental build merge'
        }
    }

    return graph_info


def update_graph_info_incremental(base_graph_info: Dict, schema_info: Dict,
                                   old_adapter_counts: Tuple[Dict, Dict, Dict],
                                   new_adapter_counts: Tuple[Dict, Dict, Dict],
                                   output_dir: str) -> Dict:
    old_nodes, old_edges, old_props = old_adapter_counts
    new_nodes, new_edges, new_props = new_adapter_counts

    updated_info = base_graph_info.copy()

    old_total_nodes = updated_info.get('node_count', 0)
    old_total_edges = updated_info.get('edge_count', 0)

    node_delta = sum(new_nodes.values()) - sum(old_nodes.values())
    edge_delta = sum(new_edges.values()) - sum(old_edges.values())

    print(f"Delta: nodes {node_delta:+,}, edges {edge_delta:+,}")

    updated_info['node_count'] = old_total_nodes + node_delta
    updated_info['edge_count'] = old_total_edges + edge_delta

    old_entity_counts = {item['name']: item['count'] for item in updated_info.get('top_entities', [])}

    for node_type, count in old_nodes.items():
        old_entity_counts[node_type] = old_entity_counts.get(node_type, 0) - count
    for node_type, count in new_nodes.items():
        old_entity_counts[node_type] = old_entity_counts.get(node_type, 0) + count

    entity_counts = {k: v for k, v in old_entity_counts.items() if v > 0}
    updated_info['top_entities'] = [
        {'name': node, 'count': count}
        for node, count in sorted(entity_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    old_connection_counts = {}
    for item in updated_info.get('top_connections', []):
        old_connection_counts[item['name']] = item['count']

    for edge_type, count in old_edges.items():
        edge_info = schema_info['edge_types'].get(edge_type, {})
        output_label = edge_info.get('output_label') or edge_type
        old_connection_counts[output_label] = old_connection_counts.get(output_label, 0) - count

    for edge_type, count in new_edges.items():
        edge_info = schema_info['edge_types'].get(edge_type, {})
        output_label = edge_info.get('output_label') or edge_type
        old_connection_counts[output_label] = old_connection_counts.get(output_label, 0) + count

    connection_counts = {k: v for k, v in old_connection_counts.items() if v > 0}
    updated_info['top_connections'] = [
        {'name': pred, 'count': count}
        for pred, count in sorted(connection_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    old_relationship_counts = {}
    for item in updated_info.get('frequent_relationships', []):
        key = '|'.join(item['entities'])
        old_relationship_counts[key] = item['count']

    for edge_type, count in old_edges.items():
        edge_info = schema_info['edge_types'].get(edge_type, {})
        source_types = edge_info.get('source', [])
        target_types = edge_info.get('target', [])

        if isinstance(source_types, str):
            source_types = [source_types]
        if isinstance(target_types, str):
            target_types = [target_types]

        for source in source_types:
            for target in target_types:
                key = f'{source}|{target}'
                old_relationship_counts[key] = old_relationship_counts.get(key, 0) - count

    for edge_type, count in new_edges.items():
        edge_info = schema_info['edge_types'].get(edge_type, {})
        source_types = edge_info.get('source', [])
        target_types = edge_info.get('target', [])

        if isinstance(source_types, str):
            source_types = [source_types]
        if isinstance(target_types, str):
            target_types = [target_types]

        for source in source_types:
            for target in target_types:
                key = f'{source}|{target}'
                old_relationship_counts[key] = old_relationship_counts.get(key, 0) + count

    relationship_counts = {k: v for k, v in old_relationship_counts.items() if v > 0}
    updated_info['frequent_relationships'] = [
        {'entities': rel.split('|'), 'count': count}
        for rel, count in sorted(relationship_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    existing_properties = {}
    for node in updated_info.get('schema', {}).get('nodes', []):
        node_type = node['data']['name']
        existing_properties[node_type] = set(node['data']['properties'])

    for node_type, props in old_props.items():
        if node_type in existing_properties:
            existing_properties[node_type] -= props

    for node_type, props in new_props.items():
        if node_type not in existing_properties:
            existing_properties[node_type] = set()
        existing_properties[node_type] |= props

    schema_nodes = []
    for node_type, props in existing_properties.items():
        if props:
            schema_nodes.append({
                'data': {
                    'name': node_type,
                    'properties': sorted(list(props))
                }
            })

    schema_edges = updated_info.get('schema', {}).get('edges', [])

    if 'schema' not in updated_info:
        updated_info['schema'] = {}
    updated_info['schema']['nodes'] = schema_nodes
    updated_info['schema']['edges'] = schema_edges

    total_size = sum(file.stat().st_size for file in Path(output_dir).rglob('*') if file.is_file())
    total_size_gb = total_size / (1024 ** 3)
    updated_info['data_size'] = f"{total_size_gb:.2f} GB"

    if 'metadata' not in updated_info:
        updated_info['metadata'] = {}
    updated_info['metadata']['last_updated_by'] = 'regenerate_metadata.py (incremental delta)'
    updated_info['metadata']['update_description'] = 'Computed delta from changed adapters'

    return updated_info


def update_graph_info_counts(base_graph_info: Dict, schema_info: Dict,
                             node_counts: Dict[str, int], edge_counts: Dict[str, int],
                             node_properties: Dict[str, Set[str]], output_dir: str) -> Dict:
    updated_info = base_graph_info.copy()

    updated_info['node_count'] = sum(node_counts.values())
    updated_info['edge_count'] = sum(edge_counts.values())

    updated_info['top_entities'] = [
        {'name': node, 'count': count}
        for node, count in sorted(node_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    predicate_counts = Counter()
    for edge_label, count in edge_counts.items():
        edge_info = schema_info['edge_types'].get(edge_label, {})
        output_label = edge_info.get('output_label') or edge_label
        predicate_counts[output_label] += count

    updated_info['top_connections'] = [
        {'name': pred, 'count': count}
        for pred, count in predicate_counts.most_common()
    ]

    relationships_freq = Counter()
    for edge_label, count in edge_counts.items():
        edge_info = schema_info['edge_types'].get(edge_label, {})
        source_types = edge_info.get('source', [])
        target_types = edge_info.get('target', [])

        if isinstance(source_types, str):
            source_types = [source_types]
        if isinstance(target_types, str):
            target_types = [target_types]

        for source in source_types:
            for target in target_types:
                relationships_freq[f'{source}|{target}'] += count

    updated_info['frequent_relationships'] = [
        {'entities': rel.split('|'), 'count': count}
        for rel, count in relationships_freq.most_common()
    ]

    schema_nodes = []
    for node_type, props in node_properties.items():
        schema_nodes.append({
            'data': {
                'name': node_type,
                'properties': sorted(list(props))
            }
        })

    schema_edges = []
    edge_connections = defaultdict(set)

    for edge_label, edge_info in schema_info['edge_types'].items():
        source_types = edge_info['source'] if isinstance(edge_info['source'], list) else [edge_info['source']]
        target_types = edge_info['target'] if isinstance(edge_info['target'], list) else [edge_info['target']]
        output_label = edge_info.get('output_label') or edge_label

        for source in source_types:
            for target in target_types:
                key = f"{source}|{target}"
                edge_connections[key].add(output_label)

    for conn_key, labels in edge_connections.items():
        source, target = conn_key.split('|')
        valid_labels = [l for l in labels if l is not None]
        schema_edges.append({
            'data': {
                'source': source,
                'target': target,
                'possible_connections': sorted(valid_labels)
            }
        })

    if 'schema' not in updated_info:
        updated_info['schema'] = {}
    updated_info['schema']['nodes'] = schema_nodes
    updated_info['schema']['edges'] = schema_edges

    total_size = sum(file.stat().st_size for file in Path(output_dir).rglob('*') if file.is_file())
    total_size_gb = total_size / (1024 ** 3)
    updated_info['data_size'] = f"{total_size_gb:.2f} GB"

    if 'metadata' not in updated_info:
        updated_info['metadata'] = {}
    updated_info['metadata']['last_updated_by'] = 'regenerate_metadata.py (full scan)'

    return updated_info


def regenerate_metadata(output_dir: str,
                       schema_config: str = "config/schema_config.yaml",
                       biocypher_config: str = "config/biocypher_config.yaml",
                       previous_graph_info_path: str = None,
                       old_version_path: str = None,
                       new_partial_path: str = None,
                       changed_adapters: str = None,
                       old_adapters_config: str = None,
                       new_adapters_config: str = None) -> None:
    print("=" * 60)
    print("SMART CONFIG-BASED METADATA GENERATION")
    print("=" * 60)

    if not os.path.isdir(output_dir):
        print(f"Error: Output directory does not exist: {output_dir}", file=sys.stderr)
        sys.exit(1)

    config_changes = None
    if old_adapters_config and new_adapters_config:
        print(f"\nData Config Changes:")
        config_changes = detect_config_changes(old_adapters_config, new_adapters_config)
        print(f"  Added: {len(config_changes['added'])} adapters")
        print(f"  Removed: {len(config_changes['removed'])} adapters")
        print(f"  Modified: {len(config_changes['modified'])} adapters")
        print(f"  Unchanged: {len(config_changes['unchanged'])} adapters")
    else:
        print(f"\nSkipping config change detection (incremental build)")

    print("=" * 60)

    schema_info = load_schema_info(schema_config, biocypher_config)

    print(f"\nGathering metadata...")

    is_incremental = not (old_adapters_config and new_adapters_config)

    if is_incremental:
        print(f"  Incremental build: aggregating from all adapter metadata files...")
        node_counts = Counter()
        edge_counts = Counter()
        node_properties = defaultdict(set)

        metadata_files = list(Path(output_dir).rglob('.adapter_metadata.json'))
        print(f"  Found {len(metadata_files)} adapter metadata files")

        for metadata_path in metadata_files:
            try:
                with open(metadata_path, 'r') as f:
                    meta = json.load(f)

                for node_label, count in meta.get('node_counts', {}).items():
                    output_type = map_label_to_output_type(node_label, schema_info, is_edge=False)
                    node_counts[output_type] += count

                for edge_label, count in meta.get('edge_counts', {}).items():
                    output_type = map_label_to_output_type(edge_label, schema_info, is_edge=True)
                    edge_counts[output_type] += count

                for node_label, props in meta.get('properties', {}).items():
                    output_type = map_label_to_output_type(node_label, schema_info, is_edge=False)
                    node_properties[output_type].update(props)
            except Exception as e:
                print(f"  ⚠️  Warning: Could not read {metadata_path}: {e}")

        node_counts = dict(node_counts)
        edge_counts = dict(edge_counts)
        node_properties = dict(node_properties)

        print(f"  ✓ Fast aggregation: {sum(node_counts.values()):,} nodes, {sum(edge_counts.values()):,} edges")
    elif new_adapters_config and Path(new_adapters_config).exists():
        print(f"  Full build: attempting fast aggregation from adapter metadata...")
        adapters_config_data = load_adapters_config(new_adapters_config)
        node_counts, edge_counts, node_properties = aggregate_adapter_metadata(output_dir, adapters_config_data, schema_info)

        total_nodes = sum(node_counts.values())
        total_edges = sum(edge_counts.values())

        if total_nodes > 0 or total_edges > 0:
            print(f"  ✓ Fast aggregation successful: {total_nodes:,} nodes, {total_edges:,} edges")
        else:
            print(f"  ⚠️  No adapter metadata found, falling back to full scan...")
            node_counts, edge_counts, node_properties = parse_metta_files(output_dir, schema_info)
            print(f"  Scanned: {sum(node_counts.values()):,} nodes, {sum(edge_counts.values()):,} edges")
    else:
        print(f"  No adapters config available, scanning MeTTa files...")
        node_counts, edge_counts, node_properties = parse_metta_files(output_dir, schema_info)
        print(f"  Scanned: {sum(node_counts.values()):,} nodes, {sum(edge_counts.values()):,} edges")

    base_graph_info = None
    if previous_graph_info_path and Path(previous_graph_info_path).exists():
        try:
            with open(previous_graph_info_path, 'r', encoding='utf-8') as f:
                base_graph_info = json.load(f)
            print(f"Loaded previous metadata (preserving {len(base_graph_info.get('datasets', []))} datasets)")
        except Exception as e:
            print(f"Warning: Could not load previous graph_info: {e}")

    if base_graph_info:
        graph_info = update_graph_info_counts(
            base_graph_info, schema_info, node_counts, edge_counts, node_properties, output_dir
        )
    else:
        graph_info = generate_graph_info(
            output_dir, schema_info, node_counts, edge_counts, node_properties
        )

    if 'metadata' not in graph_info:
        graph_info['metadata'] = {}

    if config_changes:
        graph_info['metadata']['incremental_build'] = {
            'changed_adapters': config_changes['modified'] + config_changes['added'],
            'removed_adapters': config_changes['removed'],
            'unchanged_adapters_count': len(config_changes['unchanged'])
        }

    print("\nHandling type_defs.metta...")
    type_defs_path = Path(output_dir) / "type_defs.metta"

    if new_partial_path:
        new_type_defs = Path(new_partial_path) / "type_defs.metta"
        if new_type_defs.exists():
            import shutil
            shutil.copy2(new_type_defs, type_defs_path)
            print("  ✓ Copied type_defs from partial build")
        else:
            print("  ℹ️  No type_defs in partial build (already in merged output)")
    else:
        print("  ℹ️  Full build - type_defs already in output")

    if not type_defs_path.exists():
        print("  ⚠️  Warning: type_defs.metta not found in output", file=sys.stderr)
    else:
        hierarchies, constructors = parse_type_defs(str(type_defs_path))
        print(f"  ✓ Type system: {len(hierarchies)} hierarchies, {len(constructors)} constructors")

        graph_info['metadata']['type_system'] = {
            'hierarchies': len(hierarchies),
            'constructors': len(constructors)
        }

    graph_info_path = Path(output_dir) / "graph_info.json"
    print(f"\nWriting graph_info.json...")
    with open(graph_info_path, 'w', encoding='utf-8') as f:
        json.dump(graph_info, f, indent=2)
    print(f"  ✓ Saved: {graph_info['node_count']:,} nodes, {graph_info['edge_count']:,} edges")

    print("\n" + "=" * 60)
    print("✅ Metadata generation complete!")
    print(f"\n   📊 Data:")
    print(f"      Nodes: {graph_info['node_count']:,}")
    print(f"      Edges: {graph_info['edge_count']:,}")

    if 'metadata' in graph_info:
        metadata = graph_info['metadata']
        if 'type_system' in metadata:
            print(f"\n   🔤 Type System:")
            print(f"      Hierarchies: {metadata['type_system']['hierarchies']}")
            print(f"      Constructors: {metadata['type_system']['constructors']}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python regenerate_metadata.py <output_directory> [previous_graph_info.json]")
        print("\nEnvironment variables (for incremental updates):")
        print("  OLD_VERSION_PATH      - Path to previous build directory")
        print("  NEW_PARTIAL_PATH      - Path to new partial build directory")
        print("  CHANGED_ADAPTERS      - Comma-separated list of changed adapter directories")
        print("  OLD_ADAPTERS_CONFIG   - Path to old adapters config (for detecting removed adapters)")
        print("  NEW_ADAPTERS_CONFIG   - Path to new adapters config")
        sys.exit(1)

    output_dir = sys.argv[1]
    previous_graph_info = sys.argv[2] if len(sys.argv) == 3 else None

    old_version_path = os.environ.get('OLD_VERSION_PATH')
    new_partial_path = os.environ.get('NEW_PARTIAL_PATH')
    changed_adapters = os.environ.get('CHANGED_ADAPTERS')
    old_adapters_config = os.environ.get('OLD_ADAPTERS_CONFIG')
    new_adapters_config = os.environ.get('NEW_ADAPTERS_CONFIG')

    regenerate_metadata(
        output_dir,
        previous_graph_info_path=previous_graph_info,
        old_version_path=old_version_path,
        new_partial_path=new_partial_path,
        changed_adapters=changed_adapters,
        old_adapters_config=old_adapters_config,
        new_adapters_config=new_adapters_config
    )
