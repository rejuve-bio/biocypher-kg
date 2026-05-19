import sys
import yaml
import json
import datetime
import os
from pathlib import Path
from typing import Dict, Any, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from schema_generator.llm_client import make_llm_client
from schema_generator.code_fixer import extract_code, validate_syntax
from schema_generator.logic_inference import generate_unified_logic_inference
from schema_generator.source_inspector import SourceInspector

from schema_generator.inspector_utils import inspect_adapter_files, build_inspection_context
from schema_generator.code_fixer import fix_code_hallucinations
from schema_generator.semantic_validator import validate_semantic_correctness, print_semantic_report



def generate_adapter_from_specification(
    spec_path: str,
    adapter_config: dict,
    adapter_name: str,
    output_path: str,
    use_logic_inference: bool = True,
) -> str:
    """
    Generate adapter code from a specification.
    
    Process:
    1. Read specification
    2. Inspect actual files (like specification generator does)
    3. Extract config args
    4. Generate logic inference for auxiliary files (NEW)
    5. Build LLM prompt with specification + file inspection + logic inference
    6. LLM generates code
    7. Validate and save
    """
    
    # Load specification
    with open(spec_path) as f:
        spec = yaml.safe_load(f)
    
    print(f"[*] Generating adapter from specification: {spec_path}")
    print(f"[*] Adapter name: {adapter_name}")
    
    analysis = spec.get('analysis', {})
    purpose = analysis.get('purpose', 'N/A')
    logic = analysis.get('logic_interpretation', 'N/A')
    
    print(f"\n[*] Blueprint Analysis:")
    print(f"    Purpose: {purpose}")
    print(f"    Logic: {logic}\n")
    
    print(f"[*] Inspecting files for adapter...")
    inspection = inspect_adapter_files(adapter_config)

    data_format = spec.get('data_format', {})
    if 'main_file' in inspection and inspection['main_file'] and data_format:
        meta = inspection['main_file'].get('metadata', {})
        if 'delimiter' in data_format:
            meta['delimiter'] = data_format['delimiter']
        if 'has_header' in data_format:
            meta['has_header'] = data_format['has_header']
        if 'compression' in data_format:
            meta['compression'] = data_format['compression']
        
        aux_files = spec.get('auxiliary_files', {})
        for param, aux_info in aux_files.items():
            if param in inspection['files']:
                aux_meta = inspection['files'][param].get('metadata', {})
                if 'delimiter' in aux_info: aux_meta['delimiter'] = aux_info['delimiter']
                if 'has_header' in aux_info: aux_meta['has_header'] = aux_info['has_header']
                if 'compression' in aux_info: aux_meta['compression'] = aux_info['compression']
    
    logic_inferences = {}
    has_rich_spec = bool(spec.get('relationships', [{}])[0].get('implementation', {}).get('steps'))

    if has_rich_spec:
        print(f"[*] Specification has rich implementation steps — logic inference will SUPPLEMENT them.")

    implementation_steps = spec.get('implementation_steps', [])
    if not implementation_steps:
        for rel in spec.get('relationships', []):
            rel_steps = rel.get('implementation', {}).get('steps', [])
            implementation_steps.extend([s for s in rel_steps if s and s.strip()])


    adapter_type = spec.get('adapter_type')
    if not adapter_type:
        nodes = adapter_config.get('adapter', {}).get('nodes', True)
        edges = adapter_config.get('adapter', {}).get('edges', True)
        if nodes and edges: adapter_type = 'both'
        elif nodes: adapter_type = 'nodes_only'
        elif edges: adapter_type = 'edges_only'
        else: adapter_type = 'none'
    generate_nodes = adapter_type in ['nodes_only', 'both']
    generate_edges = adapter_type in ['edges_only', 'both']

    # Auto-detect if logic inference is needed
    needs_logic_inference = use_logic_inference and (adapter_type != 'nodes_only')
    if not use_logic_inference:
        print("[*] Logic inference disabled by parameter/CLI flag")
    elif adapter_type == 'nodes_only':
        print(f"[*] Adapter is nodes-only - skipping logic inference (no edges to enrich)")
    
    # Run unified logic inference if needed
    unified_inference = {}
    if needs_logic_inference:
        processor_info = spec.get('processors', {})
        auxiliary_files = spec.get('auxiliary_files', {})
        
        # Determine which analyses to enable
        enable_processor_analysis = bool(processor_info)
        enable_auxiliary_analysis = bool(auxiliary_files)
        
        if enable_processor_analysis:
            print(f"[*] Processor analysis enabled: {processor_info.get('processor_name', 'multiple processors')}")
        if enable_auxiliary_analysis:
            print(f"[*] Auxiliary file analysis enabled: {len(auxiliary_files)} file(s)")
        
        unified_inference = generate_unified_logic_inference(
            spec_path, 
            adapter_config, 
            inspection, 
            processor_info,
            enable_auxiliary_analysis=enable_auxiliary_analysis,
            enable_processor_analysis=enable_processor_analysis
        )
        if unified_inference:
            summary = unified_inference.get('alignment_summary', 'No summary available')
            print(f"[+] Logic Inference Complete: {summary}")

    
    # Extract config args
    args = adapter_config.get('adapter', {}).get('args', {})

    # Build prompt with ALL context
    prompt = build_adapter_prompt(
        spec, args, adapter_name, inspection, unified_inference, 
        adapter_config, adapter_type, generate_nodes, generate_edges,
        implementation_steps
    )
    
    print(f"[*] Calling LLM to generate adapter code...")
    llm = make_llm_client()
    response = llm(prompt)
    
    from schema_generator.code_fixer import extract_json
    result = extract_json(response)
    
    reasoning = ""
    if result and 'code' in result:
        reasoning = result.get('reasoning', 'No reasoning provided.')
        code = result['code']
        print(f"\n[*] LLM Implementation Reasoning:")
        for line in reasoning.split('\n'):
            print(f"    {line}")
        print()
    else:
        # Fallback for old prompt or non-JSON response
        code = extract_code(response)

    # Fix common LLM hallucinations before syntax validation
    code = fix_code_hallucinations(code)

    # Validate syntax — if it doesn't parse, stop here
    ok, err = validate_syntax(code)
    if not ok:
        print(f"[-] Syntax error: {err}")
        print(f"[*] Generated Code (with line numbers):")
        for i, line in enumerate(code.split('\n'), 1):
            print(f"{i:4}: {line}")
        
        print(f"[*] Attempting automatic syntax fix...")
        from schema_generator.code_fixer import llm_fix_syntax_error
        fixed_code = llm_fix_syntax_error(code, err)
        if fixed_code:
            code = fixed_code
            print(f"[+] Automatic syntax fix successful")
        else:
            print(f"[!] Automatic syntax fix failed")
            sys.exit(1)

    print(f"[+] Syntax validation passed")

    # Semantic hallucination validation
    semantic_report = validate_semantic_correctness(code, spec, inspection)
    print_semantic_report(semantic_report)
    if semantic_report["overall_verdict"] == "fail":
        print("[!] Semantic validation FAILED — the generated code may use wrong columns or produce invalid IDs.")
        print("[!] Review the issues above, add a Logic Recipe to clarify the mapping, and re-run.")
        # Save debug trace with semantic report before exiting
        try:
            debug_dir = Path("debug_traces")
            debug_dir.mkdir(exist_ok=True)
            with open(debug_dir / f"{adapter_name}_semantic_report.json", "w") as f:
                json.dump(semantic_report, f, indent=2)
        except Exception:
            pass
        sys.exit(1)

    import datetime
    if implementation_steps:
        print(f"\n[*] Implementation step coverage ({len(implementation_steps)} steps):")
        for i, step in enumerate(implementation_steps, 1):
            clean = step.lstrip('- ').strip()
            keywords = [w for w in clean.split() if len(w) > 4][:4]
            found = any(kw.lower() in code.lower() for kw in keywords) if keywords else False
            short = step[:90] + ('...' if len(step) > 90 else '')
            status = 'PASSED' if found else 'FAILED'
            print(f"    Step {i} ({status}): {short}")
        print()

    # Save the generated adapter
    with open(output_path, 'w') as f:
        f.write(code)
    print(f"[+] Adapter written to: {output_path}")

    # Save debug trace
    try:
        debug_dir = Path("debug_traces")
        debug_dir.mkdir(exist_ok=True)
        trace_data = {
            "adapter_name": adapter_name,
            "timestamp": datetime.datetime.now().isoformat(),
            "prompt": prompt,
            "response": response,
            "reasoning": reasoning,
            "logic_inferences": logic_inferences,
            "specification": spec,
        }
        trace_path = debug_dir / f"{adapter_name}_adapter_trace.json"
        with open(trace_path, 'w') as f:
            json.dump(trace_data, f, indent=2)
        print(f"[*] Debug trace saved to {trace_path}")
    except Exception as e:
        print(f"Warning: Could not save debug trace: {e}")

    return code
def build_adapter_prompt(spec, args, adapter_name, inspection, unified_inference, adapter_config, adapter_type, generate_nodes, generate_edges, implementation_steps):
    """Build the LLM prompt for adapter generation."""
    data_format = spec.get('data_format', {})
    columns_info = spec.get('expected_columns', {})
    relationships = spec.get('relationships', [])
    auxiliary_files = spec.get('auxiliary_files', {})
    analysis = spec.get('analysis', {})
    

    if not auxiliary_files and 'auxiliary_file_usage' in analysis:
        aux_usage = analysis['auxiliary_file_usage']
        for i, file_path in enumerate(aux_usage.get('needed_files', [])):
            instructions = aux_usage.get('technical_instructions', [])
            usage_text = instructions[i] if i < len(instructions) else "Enrich data using this file"
      
            name = Path(file_path).stem
            auxiliary_files[name] = {"usage": usage_text}
   


    relationships_info = ""
    for rel in relationships:
        source_type = str(rel.get('source', 'unknown')).lower().replace(' ', '_')
        source_col = rel.get('source_column', '')
        
        # Target info is optional (only for edges)
        target_type = rel.get('target')
        target_col = rel.get('target_column')

        relationships_info += f"\nRelationship/Entity: {rel.get('name', 'unknown')}\n"
        relationships_info += f"  Source/Node entity type (label): '{source_type}'\n"
        source_loc = "filename" if str(source_col).lower() == 'filename' else f"column index {source_col}"
        if ',' in str(source_col):
            source_loc = f"COMPOSITE ID from columns: {source_col}"
        relationships_info += f"  Source/Node ID location: {source_loc}\n"

        if target_type is not None:
            target_type_str = str(target_type).lower().replace(' ', '_')
            relationships_info += f"  Target entity type (label): '{target_type_str}'\n"
            target_loc = "filename" if str(target_col).lower() == 'filename' else f"column index {target_col}"
            if target_col and ',' in str(target_col):
                target_loc = f"COMPOSITE ID from columns: {target_col}"
            relationships_info += f"  Target ID location: {target_loc}\n"
            relationships_info += f"  **MANDATORY YIELD FORMAT**: `yield (source_id, target_id, self.label, props)`\n"
        else:
            relationships_info += f"  **MANDATORY YIELD FORMAT (NODE)**: `yield (node_id, self.label, props)`\n"

        relationships_info += f"  Edge Label: {rel.get('input_label')}\n"
        relationships_info += f"  Properties: {rel.get('properties', {})}\n"


    # 🛠️ NEW: Provide Processor Context (Autonomous)
    processor_context = ""
    proc_dir = Path("biocypher_metta/processors")
    if proc_dir.exists():
        available_processors = [f.stem for f in proc_dir.glob("*.py") if f.name != "__init__.py"]
        
        # Search the specification for mentioned processors
        contract_str = yaml.dump(spec)
        mentioned_processors = [p for p in available_processors if p in contract_str]
        
        if mentioned_processors:
            processor_context = "\n### 🛠️ PROCESSOR INTERFACE CONTEXT\n"
            processor_context += "The following processor(s) are mentioned in the specification. Use their interface to implement the logic:\n\n"
            
            for proc in mentioned_processors:
                proc_path = proc_dir / f"{proc}.py"
                try:
                    with open(proc_path, 'r') as pf:
                        lines = pf.readlines()
                    
                    # Extract the class name dynamically from the source file
                    class_name = None
                    for line in lines:
                        if line.strip().startswith("class ") and "(" in line:
                            class_name = line.strip().split("class ")[1].split("(")[0].strip()
                            break
                    
                    if class_name:
                        exact_import = f"from biocypher_metta.processors.{proc} import {class_name}"
                        processor_context += f"**Processor: `{proc}`**\n"
                        processor_context += f"EXACT IMPORT (copy this exactly, do not modify):\n"
                        processor_context += f"```python\n{exact_import}\n```\n\n"
                    
                    # Provide the full source (keeping imports this time so LLM sees real interface)
                    full_content = "".join(lines[:500])
                    processor_context += f"**Full Interface:**\n```python\n{full_content}\n```\n"
                except Exception as e:
                    processor_context += f"- {proc} (Code could not be read: {e})\n"
            processor_context += "\n**MANDATORY**: You MUST import, instantiate, and use the correct methods from the processor(s) above to satisfy the specification's requirements (e.g., ID normalization).\n"
            processor_context += "**IMPORTANT**: Always call `processor.load_or_update()` in `__init__` immediately after instantiating the processor. This ensures the full mapping is loaded before any data processing begins. Example:\n"
            processor_context += "```python\nself.hgnc_processor = HGNCProcessor()\nself.hgnc_processor.load_or_update()  # load mapping upfront\n```\n"
            
                  
    # Build auxiliary files info for the prompt
    aux_info = ""
    config_args_usage = analysis.get('config_args_usage', {})
    for file_param, file_data in auxiliary_files.items():
        aux_info += f"\n- **{file_param}**: {file_data.get('filepath', 'Path not specified')}\n"
        aux_info += f"  Format: {file_data.get('format', 'unknown')}\n"
        
        # Usage instructions (rescued from V3 or from standard contract)
        usage = file_data.get('usage') or file_data.get('purpose')
        if usage:
            if isinstance(usage, dict):
                for k, v in usage.items():
                    aux_info += f"  - {k}: {v}\n"
            else:
                aux_info += f"  Usage: {usage}\n"
        
        # Column metadata from inspection/specification
        if 'column_descriptions' in file_data:
            aux_info += f"  Columns:\n"
            for col_idx, col_desc in file_data['column_descriptions'].items():
                aux_info += f"    - {col_desc}\n"
    
    if not aux_info:
        aux_info = "None"

    alignment_info = ""
    if unified_inference:
        alignment_info = "\n### DATA ALIGNMENT & TRANSFORMATION RULES\n"
        alignment_info += "The following transformations are REQUIRED to align the data files with processors and auxiliary keys:\n\n"
        
        # Auxiliary transformations
        aux_norms = unified_inference.get('auxiliary_inferences', {})
        if aux_norms:
            alignment_info += "**Auxiliary File Normalization:**\n"
            for param, data in aux_norms.items():
                if isinstance(data, dict):
                    trans = data.get('transformation', 'None')
                    reason = data.get('reasoning', '')
                    alignment_info += f"- For `{param}`: Apply `{trans}` ({reason})\n"
                else:
                    alignment_info += f"- For `{param}`: Apply `{data}`\n"    
        id_norms = unified_inference.get('id_normalizations', {})
        if id_norms:
            alignment_info += "\n**ID/Processor Normalization (CRITICAL):**\n"
            for param, data in id_norms.items():
                if isinstance(data, dict):
                    method = data.get('processor_method', 'None')
                    trans = data.get('pre_transformation') or data.get('transformation', 'None')
                    alignment_info += f"- For `{param}`: \n"
                    alignment_info += f"  - **Selected Processor Method**: `{method}`\n"
                    alignment_info += f"  - **Pre-Processor Transformation**: `{trans}`\n"
                    alignment_info += f"  - Reasoning: {data.get('reasoning', '')}\n"
                else:
                    alignment_info += f"- For `{param}`: Apply `{data}`\n"

        alignment_info += f"\n**Technical Summary**: {unified_inference.get('alignment_summary', '')}\n"
    
    config_usage_info = ""
    if config_args_usage:
        config_usage_info = "\n### Config Arguments Usage Instructions\n"
        for arg_name, usage_desc in config_args_usage.items():
            config_usage_info += f"- **{arg_name}**: {usage_desc}\n"
        config_usage_info += "\n"
    
    target_class_name = adapter_config.get('adapter', {}).get('cls')
    if not target_class_name:
    
        target_class_name = ''.join(x.capitalize() for x in adapter_name.split('_')) + "Adapter"

    # Build config args info
    args_info = json.dumps(args, indent=2)
    
    delimiter = data_format.get('delimiter', '\t')
    
    prompt_text = f"""You are an expert BioCypher adapter developer.

## Adapter: {adapter_name}

### Logic Interpretation
{spec.get('analysis', {}).get('logic_interpretation', 'No interpretation available')}

---

## Processor Interface
{processor_context}

---

## CRITICAL CONFIGURATION
- **MANDATORY CLASS NAME**: You MUST name your adapter class `{target_class_name}`.
- **Generate Nodes**: {generate_nodes} (If False, `def get_nodes(self): pass`)
- **Generate Edges**: {generate_edges} (If False, `def get_edges(self): pass`)
- **Adapter Type**: {adapter_type}

## REAL-WORLD INSPECTION DATA
Use the exact indices and headers below. This is ground truth—do NOT hallucinate file structures.

### Actual Files Inspected
{build_inspection_context(inspection)}

{alignment_info}

### Adapter Specification Information

**Config Arguments**:
{args_info}

**CRITICAL**: The following arguments are ACTUALLY available in __init__: {list(args.keys())}
DO NOT use any other instance variables (like self.taxon_id) unless they are in this list!

### Configuration Argument Analysis
{json.dumps(analysis.get('args_analysis', {}), indent=2)}

{config_usage_info}

**Expected Columns**:
{columns_info}

### Data Quality Issues Detected
{json.dumps(analysis.get('data_quality_issues', []), indent=2)}

**Data Format**:
- Delimiter: {repr(delimiter)}
- Compression: {data_format.get('compression', 'none')}
- Has Header: {data_format.get('has_header', False)}

**Relationships**:
{relationships_info}

**Auxiliary Files**:
{aux_info}


## Your Task

### 1. Algorithm & Implementation Steps
The following steps from the specification are the PRIMARY source of truth and MUST be implemented exactly.
**CRITICAL**: If a transformation (e.g., stripping a suffix, splitting a string) is mentioned in these steps but NOT in the 'DATA ALIGNMENT & TRANSFORMATION RULES' section below, you MUST still implement it. The steps below represent the core logic requirement.
"""
    
    # Build implementation steps from specification
    meaningful_steps = []
    if implementation_steps:
        for step in implementation_steps:
            if step and not step.startswith("Follow the processing_steps"):
                meaningful_steps.append(step)

    if meaningful_steps:
        for step in meaningful_steps:
            # Handle internal newlines in steps to maintain list formatting
            indented_step = step.replace("\n", "\n  ")
            prompt_text += f"- {indented_step}\n"
    else:
        prompt_text += "- (No implementation steps found in specification — check specification generation)\n"
    
    prompt_text += """
### 2. Structural Requirements
Your class MUST adhere to these structural rules:

1.  **Inherits from Adapter**: `from biocypher_metta.adapters import Adapter`
2.  **Strict Tuple Structure (CRITICAL)**:
   - **get_edges()** MUST yield a 4-tuple: `(source_id, target_id, label, properties_dict)`
   - **get_nodes()** MUST yield a 3-tuple: `(node_id, label, properties_dict)`
   - **NEVER** yield a 3-tuple in `get_edges()` and **NEVER** yield a 4-tuple in `get_nodes()`.
   - If you use a helper method like `_process_file`, use an `is_node` flag to ensure the correct tuple length is returned.

3. **Constructor (`__init__`)**:
   - **MANDATORY PATTERN**: `def __init__(self, """ + ', '.join(args.keys()) + """, write_properties, add_provenance):`
   - You MUST explicitly include `write_properties` and `add_provenance` as required arguments in the `__init__` argument list (no default values).
   - Call `super().__init__(write_properties=write_properties, add_provenance=add_provenance)` as the VERY FIRST line of the constructor.
   - **Step 1: Store ALL parameters as instance attributes FIRST** (e.g., `self.filepath = filepath`, `self.mapping_file = mapping_file`). This ensures all paths are available.
   - **Step 2: Load auxiliary files AFTER all attributes are set**:
     - Use a separate attribute for the loaded data if it would overwrite the path (e.g., `self.mapping_data = self._load_mapping(self.mapping_file)`).
     - Load mapping/lookup files FIRST (pickle, JSON, dictionary files) to ensure they are available for enrichment.
     - Load secondary data files SECOND.
   - **FOR PROCESSORS**: Pre-load the entire dictionary in __init__, NOT individual lookups.
   - **PROVENANCE (MANDATORY)**: Set `self.source = "{source}"` and `self.source_url = "{source_url}"`. Do NOT use placeholders and do NOT use the filename as the source name unless explicitly instructed.

"""
    
    if generate_edges:
        prompt_text += """3. **get_edges() method** (REQUIRED - edges: True in config):
   - Open the main data file (filepath parameter)
   - Handle both single files and directories
   - Use csv.reader with column indices (not DictReader)
   - Skip header if has_header is true
   - Extract columns according to expected_columns
   - Apply auxiliary file mappings as specified in usage instructions
   - Yield 4-tuple: (source_id, target_id, label, _props)
   - **DELIMITER HARDENING (CRITICAL)**: If the specification implementation steps mention multiple delimiters (e.g., 'DELIM_A' and 'DELIM_B'), you MUST use `re.split(r'[DELIM_A|DELIM_B]', string)` instead of a simple `.split()`. NEVER assume a single delimiter if the specification describes a composite structure.
   - **FILENAME METADATA (CRITICAL)**: If the specification mentions deriving information from the file name or path, use `os.path.basename(filepath)` and string splitting/regex to extract that metadata.
   - **BIOCYPHER ID NORMALIZATION (CRITICAL)**: BioCypher writers (especially the Neo4j writer) often interpret colons `:` in identifiers as internal delimiters or label separators, which leads to runtime failures. You MUST normalize EVERY identifier (`source_id`, `target_id`) that comes from an ontology (e.g., ONTOLOGY:ID) by replacing the colon with an underscore (e.g., `id.replace(':', '_')`). 
     - **CRITICAL TIMING**: This normalization MUST happen AFTER any processor or auxiliary file lookups. You MUST pass the raw identifier (with colons) to the processor first, and only replace colons with underscores in the final value used for yielding.
   - **COMPOSITE KEYS (MANDATORY)**: If the Source ID or Target ID location specifies multiple columns (e.g., '0,1,2'), you MUST construct a composite key string using an underscore `_` as the separator. 
     - **CRITICAL**: Use the exact indices specified for the ID in the Relationship section. 
     - **CRITICAL**: If the first part of a composite ID is a chromosome, you MUST call `.upper()` on it.
     - **Example Implementation**: `source_id = "_".join([str(row[i]).upper() if idx == 0 else str(row[i]) for idx, i in enumerate(indices)])` (where `indices` are the ones specified for the ID).
   - **EXPLICIT NODE LABELING (LOGIC-DRIVEN)**: You MUST ensure that yielded nodes and edges use the correct labels defined in the specification's `relationships` section. 
     - If the specification maps a target to a specific schema entity (e.g., `target: anatomy`), use that entity name as the label in your yield tuple: `('anatomy', target_id)`.
     - This ensures that nodes are correctly grouped and validated against the schema.
   - **CRITICAL ID VALIDATION (MANDATORY)**: Before yielding, you MUST check that `source_id` and `target_id` are present. Skip the row and log a warning ONLY if an ID is missing. Do NOT skip rows if properties (score, etc.) are missing; use `None` for missing properties.
   - Include error handling and validation

"""
    else:
        prompt_text += """3. **get_edges() method** (NOT REQUIRED - edges: False in config):
   - Return empty (use `pass` or `return`)

"""
    
    if generate_nodes:
        prompt_text += """4. **get_nodes() method** (REQUIRED - nodes: True in config):
   - Open the main data file (filepath parameter)
   - Handle both single files and directories
   - Use csv.reader with column indices (not DictReader)
   - Skip header if has_header is true
   - Extract columns according to expected_columns
   - **CRITICAL ID EXTRACTION**: Extract the node ID from the source_column specified in the relationships section. Assign it to a variable called `node_id` (NOT source_id or target_id).
   - Apply auxiliary file mappings as specified in usage instructions
    - Yield 3-tuple: (node_id, label, _props)
    - **SYNTAX SAFEGUARD**: Ensure ALL `try:` blocks have a matching `except:` or `finally:` block. Never leave a block unfinished.
    - **DELIMITER HARDENING (CRITICAL)**: If the specification implementation steps mention multiple delimiters (e.g., 'DELIM_A' and 'DELIM_B'), you MUST use `re.split(r'[DELIM_A|DELIM_B]', string)` instead of a simple `.split()`. NEVER assume a single delimiter if the specification describes a composite structure.
    - **FILENAME METADATA (CRITICAL)**: If the specification mentions deriving information from the file name or path, use `os.path.basename(filepath)` and string splitting/regex to extract that metadata.
     - **BIOCYPHER ID NORMALIZATION (CRITICAL)**: BioCypher writers often interpret colons `:` in identifiers as internal delimiters or label separators, which leads to runtime failures. You MUST normalize EVERY identifier (`node_id`) that comes from an ontology (e.g., ONTOLOGY:ID) by replacing the colon with an underscore (e.g., `id.replace(':', '_')`). 
     - **CRITICAL TIMING**: This normalization MUST happen AFTER any processor or auxiliary file lookups. You MUST pass the raw identifier (with colons) to the processor first, and only replace colons with underscores in the final value used for yielding.
     - **CRITICAL ID VALIDATION (MANDATORY)**: Before yielding, you MUST check that `node_id` is present. Skip the row and log a warning ONLY if the ID is missing. Do NOT skip rows if properties (score, etc.) are missing; use `None` for missing properties.
    - Include error handling and validation

"""
    else:
        prompt_text += """4. **get_nodes() method** (NOT REQUIRED - nodes: False in config):
   - Return empty (use `pass` or `return`)

"""
    
    prompt_text += """5. **Property and Provenance Construction (MANDATORY)**:
   Build properties ONLY inside the `if self.write_properties:` block:
   ```python
   props = {}
   if self.write_properties:
       props['property_name'] = value
       props['another_property'] = value2
       if self.add_provenance:
           props['source'] = self.source
           props['source_url'] = self.source_url
   yield (node_id, label, props)  # or (source, target, label, props) for edges
   ```
   CRITICAL: All properties must be inside the `if self.write_properties:` block.

6. **Helper methods**:
   - Implement `_load_*` methods for loading auxiliary files with ACTUAL logic
   - `_process_file` for handling single files
   - Error handling with try-except blocks
   - Mapping/transformation methods as needed

8. **NUMERIC CONVERSION GUARDRAIL (CRITICAL)**: When converting strings to `float` or `int`, you MUST handle non-numeric indicators like `.` or `NA`.
      - Use: `float(val) if val not in ('.', 'NA', 'nan', '') else None` or a `try-except` block.
      - Never perform a raw `float(row[i])` without a safeguard if the data is from a bioinformatics file.

## Critical Rules

1. **Column Indices**: Use the exact indices from expected_columns
2. **Data Format**: Use the delimiter, compression, and has_header from specification
3. **Yield Format**: ALWAYS yield 4-tuple (source, target, label, _props) for edges and 3-tuple (id, label, _props) for nodes
4. **File Handling**: Handle both single files and directories
5. **Error Handling**: Add try-except blocks for file operations
6. **Validation**: Check row length and required fields before processing
7. **Imports**: Include all necessary imports (csv, gzip, os, pickle, re, etc.)
8. **Auxiliary Files**: IMPLEMENT the actual loading and usage logic - NO placeholders
9. **Mappings**: Use auxiliary files to transform/enrich data as specified
10. **Target Transformation**: If auxiliary files provide target mappings (e.g., RNA ID -> GO ID), use the mapped value as the target, not the original column
11. **NO type hints on methods**: Do NOT add return type annotations to any method. Write `def get_edges(self):` not `def get_edges(self) -> Generator[...]:`
12. **IMPORTANT**: If sample rows show empty columns, DO NOT use those columns as IDs
13. **NEW**: Use the logic inference results above to implement sophisticated transformation logic
14. **NEW**: Handle multiple values, data type conversions, and error cases as suggested in logic inference
15. **STRICT FILTERING**: You MUST implement all filters specified in the **MANDATORY DATA FILTERING RULES** section. This is NOT optional.
16. **TYPE HINTS**: If you add type hints, use complete forms. For Generator, either omit the type hint entirely OR use the full form: `Generator[YieldType, None, None]`. NEVER use incomplete forms like `Generator[Tuple]` which cause TypeErrors.
18. **PROVENANCE**: Always set `self.source` and `self.source_url` in `__init__` based on the specification or known sources.
19. **MANDATORY PRESENCE CHECKS**: In your processing loops, you MUST explicitly validate that `source_id`, `target_id` (for edges), and `node_id` (for nodes) are not empty, None, or invalid. If a required value is missing, log a warning and `continue` to the next row.
20. **DEFENSIVE PROCESSOR HANDLING (MANDATORY)**: You must handle unpredictable return types from auxiliary processors (e.g., `None`, single strings, or empty lists) with absolute robustness.
    - **Normalization**: Always normalize processor outputs before consumption. If a method might return `None` or a single string where a list is expected, implement a normalization block (e.g., `if not isinstance(val, list): val = [val] if val else []`).
    - **Safe Aggregation**: Never perform string aggregation (like `', '.join()`) directly on raw processor outputs. Use defensive guard clauses to ensure you only join non-null iterables.
    - **Runtime Stability**: The goal is to prevent `TypeError` or `AttributeError` at runtime if a mapping lookup fails or returns a data format inconsistent with the expected BioCypher schema property type.
21. **ROBUST PROCESSOR CALLS (FALLBACK)**: If a processor lookup fails or the method is missing, you MUST use the original raw ID. Do NOT skip the row.
    - **No Silent Skips**: Never `continue` or skip a row just because a mapping failed. Only skip if the raw identifier itself is empty.
    - **Pattern**:
      ```python
      mapped_id = raw_value
      try:
          if hasattr(self.processor, 'method_name'):
              val = self.processor.method_name(raw_value)
              if val: mapped_id = val
      except Exception as e:
          print(f"Warning: mapping failed for {raw_value}, using raw ID: {e}")
      ```
22. **DEFENSIVE FILTERING (SKIP)**: If a filtering expression (like `int(row[5])`) fails due to malformed data, you MUST `continue` to skip the row.
    - **Pattern**: `try: if row[5] != 'X': continue except: continue`
**NOTE**: Use SEPARATE blocks for Filtering and Normalization. Filtering failures should skip the record (data is bad), but Normalization failures should keep the record (mapping is just missing).


## FINAL ADHERENCE CHECKLIST (ABSOLUTE MANDATE)
Before generating the code, verify that you have implemented the following EXACTLY:
1. **PRESENCE CHECKS**: Did you add `if not source_id or not target_id: continue` (or similar) logic for required fields?
2. **PROPERTIES & PROVENANCE**: Did you put properties directly inside the `_props` dictionary (e.g. `_props.update(properties)`) inside the `if self.write_properties:` block?
# 3. **FILTERING**: Did you implement all filters from the **MANDATORY DATA FILTERING RULES** section (including column and argument filters)?
# 4. **NO HARDCODED DATA VALUES**: Did you avoid hardcoding specific data values as filters (e.g., specific cell types, chromosomes, tissues)?


## Enhanced Auxiliary File Usage with Logic Inference

When implementing auxiliary file loading and usage, follow both the specification instructions AND the logic inference results above. The logic inference provides specific guidance on:
- How to handle multiple values in mappings
- Data type conversions needed
- Error handling for missing or invalid data
- Filtering and selection logic
- Join operations and transformation patterns

## Example Structure for Auxiliary File Usage

```python
import csv
import gzip
import os
import pickle
import re
from biocypher_metta.adapters import Adapter

class ExampleAdapter(Adapter):
    def __init__(self, filepath, mapping_file, label, write_properties, add_provenance):
        super().__init__(write_properties, add_provenance)
        self.filepath = filepath
        self.mapping_file = mapping_file
        self.label = label
        self.source = "source_name"
        self.source_url = "url"
        
        # Load auxiliary files immediately
        self.mapping_data = self._load_mapping_file()
    
    def _load_mapping_file(self):
        # Load and return mapping data from auxiliary file.
        try:
            # MANDATORY: You MUST implement the specific logic found in the 
            # 'MANDATORY LOGIC INFERENCE' section above for this file.
            # Use the inferred delimiters, column indices, and transformations.
            
            mapping = {}
            # Template structure:
            # 1. Open file (handle .gz if inferred)
            # 2. Parse using inferred format (CSV, Pickle, etc.)
            # 3. Apply inferred transformations
            return mapping
        except Exception as e:
            print(f"Error loading mapping file: {e}")
            return {}
    
    def get_edges(self):
        # Process main file and use mappings
        for source_id, raw_target, label, properties in self._process_main_file():
            # Construct _props with provenance
            _props = {}
            if self.write_properties:
                _props.update(properties)
                if self.add_provenance:
                    _props['source'] = self.source
                    _props['source_url'] = self.source_url
            
            yield source_id, raw_target, label, _props
    
    def get_nodes(self):
        pass


## MANDATORY CODE REQUIREMENTS

Make sure the code:
- Implements ACTUAL auxiliary file loading as specified in the **Logic Inference** section
- Uses the EXACT logic, column indices, and transformations from the inference
- Incorporates the detected ID pattern normalization (e.g., adding prefixes, case conversion)
- Is syntactically correct and handles all specified edge cases
- Includes proper error handling
- Uses the exact column indices from the specification
- FOLLOWS THE MANDATORY PROPERTY AND PROVENANCE STRUCTURE

## Output

Return ONLY valid JSON with the following structure:
{
  "reasoning": "A concise explanation of the technical design decisions made for this adapter, specifically addressing how you implemented ID extraction, auxiliary file lookups, normalization logic, and any required data transformations.",
  "code": "The complete Python code for the adapter."
}

**IMPORTANT**: 
1. The 'code' field must contain the FULL Python source code.
2. Ensure the JSON is valid and correctly escaped.
3. No preamble or postscript - just the JSON.
"""
    
    return prompt_text


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--specification", required=True, help="Path to adapter specification YAML")
    parser.add_argument("--adapters-config", required=True, help="Path to adapters config")
    parser.add_argument("--adapter-name", required=True, help="Adapter name in config")
    parser.add_argument("--output", required=True, help="Output adapter file path")
    parser.add_argument("--no-logic-inference", action="store_true", help="Disable logic inference for auxiliary files")
    
    args = parser.parse_args()
    
    # Load configs
    with open(args.adapters_config) as f:
        adapters_config = yaml.safe_load(f)
    
    adapter_config = adapters_config[args.adapter_name]
    
    # Generate adapter
    generate_adapter_from_specification(
        args.specification,
        adapter_config,
        args.adapter_name,
        args.output,
        use_logic_inference=not args.no_logic_inference
    )
