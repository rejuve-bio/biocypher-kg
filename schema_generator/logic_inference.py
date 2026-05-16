"""
Logic Inference for Auxiliary Files

Analyzes auxiliary files alongside the main data file to detect key format
mismatches and generate the exact transformation logic needed to join them.

This is kept separate from llm_adapter_generator.py so it can be tested,
reused, and iterated on independently.
"""

import gzip
import json
import pickle
import re
from pathlib import Path
from typing import Dict, Any

from schema_generator.llm_client import make_llm_client
from schema_generator.inspector_utils import parse_specification_file





def generate_unified_logic_inference(
    spec_path: str,
    adapter_config: dict,
    inspection: Dict[str, Any],
    processor_info: Dict[str, Any] = None,
    enable_auxiliary_analysis: bool = True,
    enable_processor_analysis: bool = True,
) -> Dict[str, Any]:
    """
    Unified inference for both auxiliary files and processor ID normalization.
    Uses high-rigor prompts with specific examples and structural evidence.
    
    Args:
        spec_path: Path to the specification file
        adapter_config: Adapter configuration dict
        inspection: File inspection results
        processor_info: Processor configuration from specification (e.g., {'processor_name': 'entrez_ensembl_processor', 'processor_target': 'source'})
        join_info: Join information for multi-file operations
        enable_auxiliary_analysis: Flag to enable/disable auxiliary file transformation analysis
        enable_processor_analysis: Flag to enable/disable processor ID normalization analysis
    """
    if not parse_specification_file:
        return {}

    spec_info = parse_specification_file(spec_path)
    if 'error' in spec_info:
        return {}



    args = adapter_config.get('adapter', {}).get('args', {})
    auxiliary_files = spec_info.get('auxiliary_files', {})
    main_file_param = spec_info.get('main_file_param')
    relationships = spec_info.get('relationships', [])
    
    implementation_steps = []
    for rel in relationships:
        steps = rel.get('implementation', {}).get('steps', [])
        if steps:
            implementation_steps.extend(steps)

    main_file_samples = []
    main_file_columns = {}
    main_file_info = None
    main_filename = "unknown"

    if main_file_param and main_file_param in inspection.get('files', {}):
        main_file_info = inspection['files'][main_file_param]
    else:
        main_file_info = next(
            (f for p, f in inspection.get('files', {}).items()
             if any(k in p.lower() for k in ['filepath', 'input', 'source'])),
            {}
        )

    if main_file_info:
        metadata = main_file_info.get('metadata', {})
        headers = metadata.get('headers', [])
        rows = metadata.get('sample_rows', [])
        
        main_filename = Path(main_file_info.get('path', 'unknown')).name
        if metadata.get('sampled_file'):
            main_filename = Path(metadata.get('sampled_file')).name
        
        for idx, header in enumerate(headers):
            main_file_columns[idx] = header
        for row in rows[:5]:
            if headers and len(row) == len(headers):
                main_file_samples.append(dict(zip(headers, row)))
            else:
                main_file_samples.append(row)

    aux_batch = {}
    if enable_auxiliary_analysis: 
        for aux_param, aux_info in auxiliary_files.items():
            aux_inspection_data = inspection.get('files', {}).get(aux_param, {})
            
            aux_path = aux_info.get('filepath') if isinstance(aux_info, dict) else None
            is_pickle = aux_path and aux_path.endswith(('.pkl', '.pickle'))
            
            if aux_inspection_data and not is_pickle:
                # Use inspection data for tabular files
                metadata = aux_inspection_data.get('metadata', {})
                headers = metadata.get('headers', [])
                sample_rows = metadata.get('sample_rows', [])
                
                sample_data = {
                    "type": "tabular",
                    "headers": headers,
                    "sample_rows": sample_rows[:5],
                    "source": "inspection"
                }
            else:
                if not aux_path: 
                    continue
                    
                sample_data = {}
                try:
                    if aux_path.endswith(('.pkl', '.pickle')):
                        with open(aux_path, 'rb') as f:
                            data = pickle.load(f)
                        if isinstance(data, dict):
                            all_main_identifiers = set()
                            if main_filename:
                                all_main_identifiers.add(main_filename)
                            
                            relevant_keys = set()
                            keys_list = list(data.keys())
                            for ident in all_main_identifiers:
                                for k in keys_list:
                                    k_str = str(k)
                                    if k_str in ident or ident in k_str:
                                        relevant_keys.add(k)
                                    if len(relevant_keys) >= 20: break
                                if len(relevant_keys) >= 20: break
                            
                            head_items = list(data.items())[:20]
                            relevant_items = [(k, data[k]) for k in relevant_keys if k not in dict(head_items)]
                            items = head_items + relevant_items
                            
                            value_type_analysis = {}
                            if items:
                                first_value = items[0][1]
                                value_type_analysis['value_type'] = type(first_value).__name__
                                value_type_analysis['consistent_type'] = all(type(v) == type(first_value) for k, v in items)
                                if isinstance(first_value, (list, tuple)):
                                    value_type_analysis['is_sequence'] = True
                                    value_type_analysis['sequence_length'] = len(first_value)
                            
                            sample_data = {
                                "type": "dict",
                                "sample_size": len(data),
                                "samples": items,
                                "key_examples": [str(k)[:200] + ('...' if len(str(k)) > 200 else '') for k, v in items],
                                "value_examples": [str(v)[:200] + ('...' if len(str(v)) > 200 else '') for k, v in items],
                                "value_structure": value_type_analysis,
                                "overlap_detected": len(relevant_keys) > 0
                            }
                        elif isinstance(data, list):
                            relevant_rows = []
                            all_main_identifiers = set()
                            if main_filename:
                                all_main_identifiers.add(main_filename)
                            if main_file_samples:
                                for row in main_file_samples:
                                    for val in row:
                                        if val: all_main_identifiers.add(str(val))

                            for row in data:
                                if any(str(val) in all_main_identifiers or any(str(ident) in str(val) for ident in all_main_identifiers) for val in row if val):
                                    relevant_rows.append(row)
                                if len(relevant_rows) >= 20: break
                            
                            sample_data = {
                                "type": "list",
                                "sample_size": len(data),
                                "samples": data[:10] + [r for r in relevant_rows if r not in data[:10]],
                                "overlap_detected": len(relevant_rows) > 0
                            }
                        else:
                            sample_data = {
                                "type": str(type(data)),
                                "sample": str(data)[:500],
                            }
                    elif aux_path.endswith('.json'):
                        with open(aux_path, 'r') as f:
                            data = json.load(f)
                        sample_data = {
                            "data": data if len(str(data)) < 1000 else str(data)[:1000]
                        }
                    elif aux_path.endswith(('.csv', '.tsv', '.txt')):
                        opener = gzip.open if aux_path.endswith('.gz') else open
                        with opener(aux_path, 'rt') as f:
                            sample_data = {"type": "text", "lines": [next(f) for _ in range(10)]}
                except Exception as e:
                    print(f"Warning: Could not load auxiliary file {aux_path}: {e}")
                    sample_data = {"error": str(e)}

            aux_batch[aux_param] = {
                "filename": Path(aux_info.get('filepath', aux_param)).name if isinstance(aux_info, dict) else aux_param,
                "filepath": aux_info.get('filepath') if isinstance(aux_info, dict) else 'unknown',
                "usage": aux_info.get('usage') if isinstance(aux_info, dict) else aux_info,
                "samples": sample_data
            }
            
            # Debug: Print what auxiliary data was loaded
            if sample_data.get('sample_rows'):
                print(f"[DEBUG] Loaded tabular auxiliary data for {aux_param}:")
                print(f"  Headers: {sample_data.get('headers', [])}")
                print(f"  Sample rows: {sample_data.get('sample_rows', [])[:3]}")
            elif sample_data.get('key_examples'):
                print(f"[DEBUG] Loaded pickle auxiliary data for {aux_param}:")
                print(f"  Type: {sample_data.get('type')}")
                print(f"  Sample size: {sample_data.get('sample_size')}")
                print(f"  Key examples: {sample_data.get('key_examples', [])[:40]}")
                print(f"  Value examples: {sample_data.get('value_examples', [])[:3]}")
            elif sample_data.get('error'):
                print(f"[DEBUG] Failed to load auxiliary data for {aux_param}: {sample_data['error']}")
            else:
                print(f"[DEBUG] No sample data found for {aux_param} (type: {sample_data.get('type', 'unknown')})")

    proc_hints = {}     
    id_samples = {}
    
    if enable_processor_analysis:  # Only collect if flag is enabled
        spec_str = json.dumps(spec_info)
        proc_dir = Path("biocypher_metta/processors")
        
        # If processor_info is provided from specification, use it directly
        if processor_info and processor_info.get('processor_name'):
            processor_name = processor_info['processor_name']
            processor_target = processor_info.get('processor_target', 'source')
            print(f"[DEBUG] Using processor from specification: {processor_name} (target: {processor_target})")
            
            proc_path = proc_dir / f"{processor_name}.py"
            if proc_path.exists():
                source = proc_path.read_text()
                lines = source.splitlines()
                method_hints = []
                
                # Extract method signatures with their docstrings
                i = 0
                while i < len(lines):
                    line = lines[i]
                    if 'def get_' in line or 'def lookup' in line or 'def map_' in line or 'def process_' in line:
                        method_hints.append(line.strip())
                        for j in range(i+1, min(i+11, len(lines))):
                            next_line = lines[j].strip()
                            if next_line and not next_line.startswith('def '):
                                method_hints.append(next_line)
                            if 'Returns:' in next_line or 'return' in next_line.lower():
                                for k in range(j+1, min(j+4, len(lines))):
                                    method_hints.append(lines[k].strip())
                                break
                    i += 1
                
                proc_hints[processor_name] = '\n'.join(method_hints[:100])  # Increased limit
                print(f"[DEBUG] Loaded processor interface: {processor_name}")

        if not proc_hints:
 
            available_processors = []
            if proc_dir.exists():
                available_processors = [f.stem for f in proc_dir.glob("*.py") if f.name != "__init__.py"]
            
            mentioned_processors = [p for p in available_processors if p in spec_str]
            
           
            for proc in mentioned_processors:
                proc_path = proc_dir / f"{proc}.py"
                if not proc_path.exists():
                    continue
                
                source = proc_path.read_text()
                lines = source.splitlines()
                method_hints = []
                
                i = 0
                while i < len(lines):
                    line = lines[i]
                    if 'def get_' in line or 'def lookup' in line or 'def map_' in line or 'def process_' in line:
                        method_hints.append(line.strip())
                        for j in range(i+1, min(i+11, len(lines))):
                            next_line = lines[j].strip()
                            if next_line and not next_line.startswith('def '):
                                method_hints.append(next_line)
                            if 'Returns:' in next_line or 'return' in next_line.lower():
                                for k in range(j+1, min(j+4, len(lines))):
                                    method_hints.append(lines[k].strip())
                                break
                    i += 1
                
                proc_hints[proc] = '\n'.join(method_hints[:100])
            
            # If no processors mentioned, check for processor keywords as fallback
            if not mentioned_processors:
                processor_keywords = ['Processor', 'processor', 'HGNC', 'Ensembl', 'UniProt', 'get_ensembl_id', 'get_current_symbol']
                has_processor_mention = any(keyword in spec_str for keyword in processor_keywords)
                if has_processor_mention:
                    proc_hints['mentioned_processor'] = "Processor mentioned in specification"

            
        if relationships and main_file_info:
            rel = relationships[0]
            metadata = main_file_info.get('metadata', {})
            headers = metadata.get('headers', [])
            sample_rows = metadata.get('sample_rows', [])
            
            def get_col_samples(col_spec):
                """Get sample values for a column spec (index or name)."""
                samples = []
                if str(col_spec).lower() == 'filename':
                    filenames = metadata.get('sample_filenames', [])
                    return filenames[:30]
                
                try:
                    col_idx = int(col_spec)
                    for row in sample_rows[:10]:
                        if col_idx < len(row):
                            samples.append(row[col_idx])
                except (ValueError, TypeError):
                    # Try by column name
                    if col_spec in headers:
                        col_idx = headers.index(col_spec)
                        for row in sample_rows[:10]:
                            if col_idx < len(row):
                                samples.append(row[col_idx])
                return [str(s)[:200] + ('...' if len(str(s)) > 200 else '') for s in samples]
            
            source_col = rel.get('source_column', '')
            if source_col != '':
                src_samples = get_col_samples(source_col)
                if src_samples:
                    id_samples['source'] = {
                        'column': source_col,
                        'entity_type': rel.get('source', ''),
                        'samples': src_samples,
                    }
            
            target_col = rel.get('target_column', '')
            if target_col != '' and target_col != source_col:
                tgt_samples = get_col_samples(target_col)
            if tgt_samples:
                id_samples['target'] = {
                    'column': target_col,
                    'entity_type': rel.get('target', ''),
                    'samples': tgt_samples,
                }

    if not aux_batch and not proc_hints:
        return {}  

    aux_section = ""
    if aux_batch:
        aux_section = f"""
### PART B: Mapping Alignment Protocol
Perform a rigorous comparison between the ACTUAL sample values from the main file and the ACTUAL keys from the auxiliary file. Use the following protocol:

**PHASE 1: Subsequence & Overlap Analysis**
Perform a forensic, character-by-character comparison between the main identifiers (ALL columns AND the 'File Name') and the auxiliary map keys.
- **Canonical Overlap**: Is an auxiliary key a substring of any main value or the 'File Name'? **Search aggressively for these overlaps.**
- **Transformation Patterns**: If a match is found, identify the exact boundaries. Is it a prefix? A suffix? Is it separated by a delimiter like '_', '.', or '$'?
- **PHASE 2: Transformation Logic**
Determine the EXACT Python expression to convert the raw identifier into the auxiliary key.
- If the identifier comes from a column, use the variable `value`.
- If the identifier is derived from the file name, use the variable `filename`.
- **SUBSTRING MATCHES**: If the main identifier contains the auxiliary key as a substring but includes additional characters (prefixes, suffixes, or versioning), you MUST provide a transformation to isolate the exact key. "Aligning directly" is strictly reserved for exact, character-for-character equality.
- **DELIMITER PRECISION**: Your transformation must use the exact literal characters found in the samples to separate the key from the rest of the string.
- If they match exactly, specify `None`.

**PHASE 3: Value Access Logic**
Describe how to access the *result* from the auxiliary mapping once you have the key:
- If the mapping is a simple dict of `key: value`, specify `mapping.get(key)`.
- If it's a dict of `key: [val1, val2]`, specify `mapping.get(key)[0]` if you need the first element.

**MANDATORY**: You MUST detect mismatches by comparing actual sample values provided. Do NOT guess based on names. If you see a mismatch and skip the transformation step, the pipeline will FAIL.
"""

    proc_section = ""
    if proc_hints:
        entity_type_info = ""
        if processor_info and relationships:
            processor_name = processor_info.get('processor_name', '')
            processor_target = processor_info.get('processor_target', 'source')  # 'source', 'target', or 'both'
            
            rel = relationships[0]
            source_entity = rel.get('source', '')
            target_entity = rel.get('target', '')
            
            # Determine which entity type(s) the processor applies to
            if processor_target == 'source':
                relevant_entity = source_entity
                entity_type_info = f"""
**Processor Configuration**:
- Processor: `{processor_name}`
- Applies to: **source** entity
- Source entity type: `{source_entity}` (e.g., if it contains 'ensembl', the output should be Ensembl IDs like ENSG00000141510)
"""
            elif processor_target == 'target':
                relevant_entity = target_entity
                entity_type_info = f"""
**Processor Configuration**:
- Processor: `{processor_name}`
- Applies to: **target** entity
- Target entity type: `{target_entity}`
"""
            elif processor_target == 'both':
                entity_type_info = f"""
**Processor Configuration**:
- Processor: `{processor_name}`
- Applies to: **both** source and target entities
- Source entity type: `{source_entity}`
- Target entity type: `{target_entity}`
"""
        
        proc_section = f"""
### PART C: Processor Method Selection for ID Conversion
{entity_type_info}

**IMPORTANT**: The processor is ONLY applied to the entity specified in 'Applies to' above. 
- If it applies to 'source', you are converting the SOURCE column values
- If it applies to 'target', you are converting the TARGET column values
- The other entity (not being processed) is irrelevant to this analysis

**Sample Values from Processor Target Column**:
The processor will process values from the column specified by processor_target. Here are sample values:
{json.dumps(id_samples, indent=2)}

For the ID column that needs processor conversion, perform this analysis:

1. **Input format analysis**: Look at the sample values above from the processor target column
   - What is the actual format of these values?
   - Examples: plain gene symbols (TP53, BRCA1), Ensembl IDs (ENSG00000141510), compound identifiers, etc.
   - **CRITICAL**: Base your analysis on the ACTUAL sample values shown above, not assumptions

2. **Required output format**: What ID format should the processor produce?
   - Look at the entity type name from 'Processor Configuration' above
   - The entity type name indicates what ID format is needed

4. **Processor method selection**: Which processor method produces the required output?
   - Look at ALL available methods in the processor's code (shown in 'Processors' section below)
   - For EACH method, read its docstring to see what it RETURNS
   - Match the method's RETURN VALUE with the required output format from step 2
   - **Example**: If you need Ensembl IDs, find the method whose docstring says it returns "Ensembl gene ID"
   - **CRITICAL**: The method's return type (from docstring) must match the entity type requirement
   - **MANDATORY (HGNC)**: If you are using `hgnc_processor` and the entity label is `gene`, you MUST use `get_ensembl_id`. Do NOT use `get_current_symbol` as the primary identifier for genes, as BioCypher prefers Ensembl IDs for gene nodes.
   - You MUST specify the exact method name from the processor code

5. **Pre-processor transformation**: Does the input need transformation before passing to the processor?
   - Example: `value.split()[0]` to extract ID from "E063 Adipose Nuclei" → "E063"
   - **MANDATORY**: Review the 'PLANNED IMPLEMENTATION STEPS' below. You MUST translate any prose normalization instructions (e.g., stripping suffixes, splitting strings, or format cleanup) into a valid Python expression in the 'transformation' field. Do NOT return 'None' if the specification identifies a required transformation.
   - If no transformation is described in the steps and none is detected in the samples, write "None".

**MANDATORY**: You MUST include 'processor_method' in your response for processor-based normalization.
"""



    prompt = f"""### UNIFIED DATA ALIGNMENT & NORMALIZATION INFERENCE
You are a senior bioinformatician. Analyze the alignment between the main data file and its dependencies.

## 1. MAIN DATA SAMPLES
File Name: {main_filename}
Columns: {json.dumps(main_file_columns)}
Sample Rows: {json.dumps(main_file_samples[:10])}

## 2. DATA CONTEXT
Auxiliary Files: {json.dumps(aux_batch, indent=2)}
Processors: {json.dumps(proc_hints, indent=2)}
ID Column Samples: {json.dumps(id_samples, indent=2)}

## 3. PLANNED IMPLEMENTATION STEPS
{json.dumps(implementation_steps, indent=2)}


---

{aux_section}
{proc_section}

---

## OUTPUT FORMAT
Return ONLY valid JSON:
{{
  "id_normalizations": {{ 
    "source": {{
      "processor_method": "method_name",
      "transformation": "Python expression or 'None'",
      "reasoning": "..."
    }},
    "target": {{
      "processor_method": "method_name",
      "transformation": "Python expression or 'None'",
      "reasoning": "..."
    }}
  }},
  "auxiliary_inferences": {{ 
    "param_name": {{
      "transformation": "Python expression or 'None'",
      "reasoning": "Logic for key alignment and value extraction"
    }}
  }},
  "join_normalizations": {{
    "main_column_X_to_aux_column_Y": {{
      "transformation": "Python expression or 'None'",
      "target": "main or auxiliary",
      "reasoning": "Logic for join column format alignment"
    }}
  }},
  "alignment_summary": "Technical summary of transformations detected."
}}

**CRITICAL**: 
- For 'id_normalizations', the key should be 'source' or 'target'. 
- **CRITICAL**: If the 'Processor Configuration' says 'Applies to: **both**', you MUST provide TWO entries in 'id_normalizations': one with key "source" and one with key "target". Even if they use the same method, you must specify them both separately.
- You MUST include 'processor_method' field with the exact method name from the processor
- Focus ONLY on the entity specified by processor_target - ignore the other entity"""

    try:
        llm = make_llm_client()
        response = llm(prompt, system="You are a data engineering expert. Resolve all format mismatches and return JSON.")
        
        # Debug: Print the raw response
        print(f"[DEBUG] Raw Logic Inference Response: {response}")
        
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            result = json.loads(match.group())
            # Ensure alignment_summary is present
            if 'alignment_summary' not in result:
                result['alignment_summary'] = "Alignments resolved."
            return result
        else:
            print("[DEBUG] Logic Inference: No JSON found in response.")
    except Exception as e:
        print(f"Unified inference failed: {e}")
    
    return {}
