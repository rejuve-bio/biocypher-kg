import json
import os
import gzip
import csv
import pickle
from pathlib import Path
from typing import Dict, Any, List, Tuple
from adapter_automation.llm_client import make_llm_client

def _detect_filename_pattern(filenames: List[str]) -> dict:
    """
    Detect patterns in filenames that might indicate special data structures.
    
    Returns:
        dict with pattern information or None if no special pattern detected
    """
    if not filenames:
        return None
    
    numeric_count = sum(1 for f in filenames if f.isdigit())
    if numeric_count / len(filenames) > 0.8:  # 80% threshold
        return {
            'type': 'numeric_ids',
            'description': 'Filenames are numeric identifiers (e.g., Entrez IDs)',
            'structure': 'one_file_per_entity',
            'filename_is_data': True,
            'filter_code': 'filename.isdigit()'
        }
  
    
    # Check for UUID pattern
    import re
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    uuid_count = sum(1 for f in filenames if uuid_pattern.match(f))
    if uuid_count / len(filenames) > 0.8:
        return {
            'type': 'uuid',
            'description': 'Filenames are UUIDs',
            'structure': 'one_file_per_entity',
            'filename_is_data': True,
            'filter_code': 'uuid_pattern.match(filename)'
        }
    
    return None


def parse_specification_file(spec_path: str) -> dict:
    """Extract key information from a specification YAML for prompt building."""
    try:
        import yaml
        with open(spec_path) as f:
            spec = yaml.safe_load(f)
        
        auxiliary_files = spec.get('auxiliary_files', {})
        analysis = spec.get('analysis', {})
        
        return {
            'auxiliary_files': auxiliary_files,
            'analysis': analysis,
            'relationships': spec.get('relationships', []),
            'purpose': analysis.get('purpose', ''),
            'data_quality_issues': analysis.get('data_quality_issues', []),
            'data_format': spec.get('data_format', {}),
            'expected_columns': spec.get('expected_columns', {}),
            'additional_file_instructions': spec.get('additional_file_instructions', []),
            'implementation_steps': spec.get('implementation_steps', []),
            'processing_steps': spec.get('processing_steps', []),
            'main_file_param': spec.get('main_file_param')
        }
    except Exception as e:
        return {"error": str(e)}



def _robust_inspect(path: str) -> dict:
    """
    Delegates all file inspection to SourceInspector (the single source of truth).
    """
    try:
        from .source_inspector import SourceInspector
    except ImportError as e:
        return {'headers': [], 'sample_rows': [], 'delimiter': '\t',
                'compression': 'gzip' if (path.endswith('.gz') or path.endswith('.bgz')) else 'none',
                'error': f'Could not import SourceInspector: {e}'}
    
    try:
        result = SourceInspector(path).inspect()
        return result
    except Exception as e:
        compression = 'gzip' if (path.endswith('.gz') or path.endswith('.bgz')) else 'none'
        return {'headers': [], 'sample_rows': [], 'delimiter': '\t',
                'compression': compression, 'error': str(e)}


def inspect_adapter_files(adapter_config: dict) -> dict:
    """
    Inspect all files mentioned in the adapter configuration.
    Uses _robust_inspect() for text/compressed files.
    Supports recursive scanning.
    
    Args:
        adapter_config: Adapter configuration dictionary
    """
    inspection_results = {
        'files': {},
        'parameters': {}
    }

    def _inspect_single_path(path, key):
        """Helper to run the actual inspection on a single path."""
        if not path or not isinstance(path, str):
            return

        # Avoid duplicate inspection
        if path in [f.get('path') for f in inspection_results['files'].values()]:
            return

        print(f"[*] Inspecting path for {key}: {path}")
        try:
            if os.path.isdir(path):
                # For directories, sample the first non-hidden file
                sample_files = sorted([
                    f for f in os.listdir(path)
                    if os.path.isfile(os.path.join(path, f)) and not f.startswith('.')
                ])
                if sample_files:
                    sample_path = os.path.join(path, sample_files[0])
                    meta = _robust_inspect(sample_path)
                    meta['sampled_file'] = sample_path
                    meta['all_files_count'] = len(sample_files)
                    meta['sample_filenames'] = sample_files[:30]
                    
                    # Detect filename patterns
                    filename_pattern = _detect_filename_pattern(sample_files)
                    if filename_pattern:
                        meta['filename_pattern'] = filename_pattern
                    
                    inspection_results['files'][key] = {
                        'path': path, 'type': 'directory', 'is_directory': True, 'metadata': meta
                    }
                else:
                    inspection_results['files'][key] = {'path': path, 'type': 'directory', 'metadata': {}}
            elif os.path.exists(path):
                if path.endswith(('.pkl', '.pickle')):
                    with open(path, 'rb') as f:
                        data = pickle.load(f)
                        meta = {
                            'total_keys': len(data) if isinstance(data, dict) else '?',
                            'sample_mappings': dict(list(data.items())[:30]) if isinstance(data, dict) else {}
                        }
                        inspection_results['files'][key] = {'path': path, 'type': 'pickle', 'metadata': meta}
                else:
                    meta = _robust_inspect(path)
                    inspection_results['files'][key] = {'path': path, 'type': 'data_file', 'metadata': meta}
            else:
                inspection_results['files'][key] = {'path': path, 'error': 'File not found'}
        except Exception as e:
            inspection_results['files'][key] = {'path': path, 'error': str(e)}

    def _recursive_scan(data, parent_key=None):
        """Recursively find paths and parameters in the config."""
        if isinstance(data, dict):
            path_keys = ['path', 'filepath', 'file_path', 'data_file']
            found_path = False
            for pk in path_keys:
                if pk in data and isinstance(data[pk], str) and (os.path.exists(data[pk]) or '/' in data[pk]):
                    result_key = parent_key if parent_key else pk
                    _inspect_single_path(data[pk], result_key)
                    found_path = True

            for k, v in data.items():
                if k not in path_keys:
                    if isinstance(v, str) and v not in ('None', 'none', '') and (os.path.exists(v) or (v.startswith('./') or v.startswith('/'))):
                        _inspect_single_path(v, k)
                    else:
                        _recursive_scan(v, k)
                elif not found_path and isinstance(v, str) and (os.path.exists(v) or '/' in v):
                    _inspect_single_path(v, k)

        elif isinstance(data, list):
            for i, item in enumerate(data):
                _recursive_scan(item, f"{parent_key}_{i}" if parent_key else str(i))
        
        elif isinstance(data, (str, int, float, bool)) and parent_key:
            inspection_results['parameters'][parent_key] = data

    # Start recursive scan on the args
    adapter_args = adapter_config.get('adapter', {}).get('args', {})
    _recursive_scan(adapter_args)

    main_file = None
    files = inspection_results.get('files', {})
    if files:
        priority_keys = ['filepath', 'file_path', 'data_file', 'enhancers_file', 'input_file']
        for key in priority_keys:
            if key in files and 'error' not in files[key]:
                main_file = files[key]
                main_file['param_name'] = key
                break
       
        if not main_file:
            for key, f in files.items():
                if f.get('type') == 'data_file' and not f.get('is_directory') and 'error' not in f:
                    main_file = f
                    main_file['param_name'] = key
                    break
        
        if not main_file:
            for key, f in files.items():
                if 'error' not in f:
                    main_file = f
                    main_file['param_name'] = key
                    break
    
    inspection_results['main_file'] = main_file

    return inspection_results


def build_inspection_context(inspection: dict) -> str:
    """Build a markdown context string from inspection results for LLM prompts."""
    context = "### Actual Files Inspected\n"

    for param_name, file_info in inspection['files'].items():
        context += f"\n#### {param_name} ({file_info['path']})\n"
        if 'error' in file_info:
            context += f"- **Error**: {file_info['error']}\n"
            continue

        context += f"- **Type**: {file_info['type']}\n"
        meta = file_info.get('metadata', {})

        if 'filename_pattern' in meta:
            pattern = meta['filename_pattern']
            context += f"\n**SPECIAL STRUCTURE DETECTED**:\n"
            context += f"- **Pattern Type**: {pattern['type']}\n"
            context += f"- **Description**: {pattern['description']}\n"
            context += f"- **Structure**: {pattern['structure']}\n"
            context += f"- **Filename is Data**: {pattern['filename_is_data']}\n"
            context += f"- **Filter Code**: `{pattern['filter_code']}`\n"
            context += f"- **IMPORTANT**: The SOURCE entity ID is the FILENAME, not a column in the file!\n\n"

        if 'sample_filenames' in meta:
            context += f"- **Sample Filenames**: {', '.join(meta['sample_filenames'])} (Total: {meta.get('all_files_count')})\n"

        if file_info['type'] == 'pickle':
            sample = meta.get('sample_mappings', {})
            context += f"- **Sample Mappings** (first 3):\n"
            for k, v in list(sample.items())[:3]:
                context += f"  - {k} -> {v}\n"
        else:
            headers = meta.get('headers', [])
            sample_rows = meta.get('sample_rows', [])

            context += f"- **Column Structure**:\n"
            for i, header in enumerate(headers):
                context += f"  - Column {i}: {header}\n"

            delimiter_repr = repr(meta.get('delimiter', '\t'))
            context += f"- **Format**: {meta.get('compression', 'none')} delimiter: {delimiter_repr}\n"

            warnings = meta.get('warnings', [])
            if warnings:
                context += f"\n**DATA QUALITY WARNINGS**:\n"
                for warning in warnings:
                    context += f"- {warning}\n"
                context += "\n"

            if sample_rows:
                context += f"\n- **Sample Data** ({len(sample_rows)} rows shown):\n"
                col_labels = [f"Col {i} ({h})" for i, h in enumerate(headers)]
                context += f"  | {' | '.join(col_labels)} |\n"
                context += f"  | {' | '.join(['---'] * len(headers))} |\n"
                for row in sample_rows[:5]:
                    padded = list(row) + [''] * max(0, len(headers) - len(row))
                    cells = [str(padded[i])[:100] for i in range(len(headers))]
                    context += f"  | {' | '.join(cells)} |\n"

    return context
