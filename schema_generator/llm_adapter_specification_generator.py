#!/usr/bin/env python3
"""
LLM Adapter Specification Generator - Enhanced with Column Mapping and Source Inspection

This version integrates:
1. source_inspector - to analyze actual file structure
2. llm_column_mapper - to generate semantic column mappings
3. Self-questioning approach - to understand adapter requirements

Usage:
    uv run python3 -m schema_generator.llm_adapter_specification_generator \
        --adapter-config config/hsa/hsa_adapters_config_sample.yaml \
        --adapter-name <adapter_name> \
        --output data_source_schemas/adapter_specs/<adapter_name>_specification.yaml
"""

import argparse
import json
import os
import sys
import tempfile
import yaml
import re, os
from pathlib import Path
import yaml
from .code_fixer import extract_json
import json as _json
from pathlib import Path
from typing import Optional, Dict, Any

from schema_generator.llm_client import make_llm_client
from schema_generator.inspector_utils import inspect_adapter_files, build_inspection_context


def save_debug_trace(adapter_name: str, trace_data: dict):
    """Save intermediate LLM outputs for debugging and transparency."""
    debug_dir = Path("debug_traces")
    debug_dir.mkdir(exist_ok=True)
    
    trace_path = debug_dir / f"{adapter_name}_trace.json"
    with open(trace_path, 'w') as f:
        json.dump(trace_data, f, indent=2)
    print(f"[*] Debug trace saved to {trace_path}")


# Add project root to path
root = str(Path(__file__).resolve().parent.parent)
if root not in sys.path:
    sys.path.insert(0, root)

try:
    from .llm_column_mapper import LLMColumnMapper
except ImportError:
    try:
        from llm_column_mapper import LLMColumnMapper
    except ImportError:
        LLMColumnMapper = None
        print("[!] Warning: Could not import LLMColumnMapper. Semantic mapping will be skipped.")


def generate_column_mappings(inspection: dict, adapter_name: str, adapter_label: str = None, 
                             source_entity_hint: str = None, target_entity_hint: str = None,
                             source_id: Any = None, target_id: Any = None,
                             adapter_type: str = 'both') -> dict:
    """Generate semantic column mappings using LLMColumnMapper."""

    try:
        main_file_meta = inspection['main_file']['metadata']
        
        # Skip if no columns detected (file inspection failed)
        if not main_file_meta.get('headers'):
            print("[*] Skipping semantic column mapping: No columns detected in file")
            return {}
        
        print("[*] Generating semantic column mappings with LLMColumnMapper...")
            
        print(f"[DEBUG] main_file_meta keys: {list(main_file_meta.keys())}")
        if 'sample_rows' in main_file_meta:
            print(f"[DEBUG] sample_rows length: {len(main_file_meta['sample_rows'])}")
        mapper = LLMColumnMapper(main_file_meta)
        
        hint = adapter_label or adapter_name
        mappings = mapper.generate_mappings(
            relationship_type=hint,
            source_entity_hint=source_entity_hint,
            target_entity_hint=target_entity_hint,
            source_id_hint=source_id,
            target_id_hint=target_id,
            adapter_type=adapter_type
        )
        
        aux_mappings = {}
        for param_name, file_info in inspection.get('files', {}).items():
            if param_name == 'main_file' or file_info.get('path') == inspection['main_file'].get('path'):
                continue
            
            if file_info.get('type') == 'data_file' and 'metadata' in file_info:
                print(f"[*] Generating semantic mappings for auxiliary file: {param_name}")
                aux_mapper = LLMColumnMapper(file_info['metadata'])
                aux_map = aux_mapper.generate_mappings(
                    relationship_type=f"supplemental data for {hint}",
                    adapter_type='nodes_only' # Treat as property/lookup source
                )
                if aux_map:
                    aux_mappings[param_name] = aux_map
        
        if aux_mappings:
            mappings['auxiliary_mappings'] = aux_mappings

        print(f"[+] Generated semantic column mappings (including {len(aux_mappings)} auxiliary files)")
        return mappings
            
    except Exception as e:
        print(f"[!] Error: Could not generate semantic column mappings: {e}")
        import traceback
        traceback.print_exc()
        return {}


def build_inspection_prompt(inspection: dict, adapter_config: dict, adapter_name: str) -> str:
    """Build a prompt for the LLM to understand the adapter structure."""
    
    # Build the inspection context
    inspection_context = build_inspection_context(inspection)
    
    prompt = f"""# Adapter Structure Analysis for {adapter_name}

## Adapter Configuration
```yaml
{yaml.dump(adapter_config, sort_keys=False)}
```

## File Inspection Results

{inspection_context}
"""
    return prompt

def _llm_call_1_analysis(inspection: dict, adapter_config: dict, adapter_name: str, 
                        basic_params: dict, logic_recipe_rules: str, llm, 
                        semantic_mappings: dict = None, adapter_type: str = 'both') -> dict:
    """LLM Call 1 — Adapter identity, args classification, logic interpretation.

    Produces: args_analysis, adapter_analysis (logic_interpretation, purpose,
    source_column, target_column, properties, data_quality_issues, join_type,
    auxiliary_file_usage, config_args_usage, filters).
    """



    args = adapter_config.get('adapter', {}).get('args', {})

    basic_params = basic_params or {}
    source_type      = basic_params.get('source_type')
    target_type      = basic_params.get('target_type')
    processor_info   = basic_params.get('processor_info')

    basic_params_block = "\n## BASIC PARAMETERS PROVIDED:\n"
    if source_type:
        basic_params_block += f"- **Source Entity Type**: {source_type}\n"
    if target_type:
        basic_params_block += f"- **Target Entity Type**: {target_type}\n"
    # source_id and target_id removed - LLM column mapper auto-detects from entity types
    if processor_info:
        basic_params_block += f"- **Processor Info**: {processor_info}\n"
    
    basic_params_block += f"- **Adapter Type**: {adapter_type}\n"

    inspection_prompt = build_inspection_prompt(inspection, adapter_config, adapter_name)

    analysis_prompt = f"""
## TASK 1: Adapter Identity Analysis
Analyze the adapter name '{adapter_name}' and determine the primary entity types and relationships it handles.



## TASK 2: Configuration Arguments Analysis
Analyze these arguments from the adapter config:
```yaml
{yaml.dump(args, sort_keys=False)}
```

For each argument, determine its **functional role** by asking: **"What does the code DO with this value?"**
Do NOT classify by the argument's name — classify by its observable behavior:

1. **Primary Data Source**: Its value is a file path that is opened and iterated over to produce the output records.
2. **Supplemental Data Source**: Its value is a file path that is opened and joined with the primary source using a shared key.
3. **Identifier Mapping**: Its value is a file path (usually .pkl) that is loaded into a lookup dictionary. A column value from the data is used as the lookup key.
4. **Execution Switch**: Its value is compared in an if/elif block to decide WHICH file to open, WHAT type of record to create, or WHICH code path to follow.
   - **CRITICAL**: Arguments like `type`, `edge_type`, `node_type`, `category`, `biotype`, `subontology` are almost always Execution Switches, NOT Static Metadata.
   - **CRITICAL**: If an argument's value specifies WHAT KIND of output to generate (e.g., "nearest", "distal", "promoter"), it's an Execution Switch.
5. **Static Metadata**: A value passed through as-is to every output record. Very few arguments qualify.

Your `logic_interpretation` must be a technical blueprint that synthesizes the Logic Recipe with your args_analysis.

**SEMANTIC AUTHORITY & INTENT CROSS-REFERENCE (MANDATORY)**:
The **Logic Recipe** is the absolute source of truth and must be used to override or correct any part of the automated analysis. Perform your analysis in two stages:
1.  **Baseline Analysis**: Evaluate the raw data inspection, config arguments, and pre-analyzed semantic mappings.
2.  **Recipe Cross-Reference**: Analyze the Logic Recipe to identify specific user intentions that conflict with the baseline.
- **Universal Override**: If the recipe specifies logic for IDs, auxiliary files, processors, filtering, or column indices that differs from the baseline, you MUST prioritize the recipe.
- **Mismatch Identification**: Explicitly look for mismatches between the user's instructions and the inferred data. For example, if the user specifies a processor but the baseline analysis doesn't see a need for it, you MUST implement the processor as instructed.
- **Grounded Implementation**: Explain exactly how the user's intended logic is physically implemented, even if it deviates from standard patterns.
- **ENTITY FILTERING (CRITICAL)**: Analyze the 'ENTITY IDENTIFICATION REASONING' provided in the semantic mappings. If the reasoning specifies that an entity is identified based on a specific column value or condition, you MUST include this as a mandatory filtering requirement in your logic.
- **ENTITY FILTERING (CRITICAL)**: Analyze the 'ENTITY IDENTIFICATION REASONING' provided in the semantic mappings. If the reasoning specifies that an entity is identified based on a specific column value or condition, you MUST include this as a mandatory filtering requirement in your logic.
"""

    logic_recipe_section = ""
    if logic_recipe_rules:
        logic_recipe_section = f"""
## LOGIC RECIPE PROVIDED BY RESEARCHER:
```
{logic_recipe_rules}
```
**CRITICAL — UNIVERSAL SEMANTIC AUTHORITY**: This Logic Recipe is your primary directive and must override any automated analysis.
1.  **Cross-Reference**: Compare this recipe against the 'SEMANTIC COLUMN MAPPINGS' and 'TECHNICAL COLUMN STRUCTURE' sections.
2.  **Identify Mismatches**: If the user's recipe specifies a column, ID source, auxiliary file usage or processor that differs from the automated mappings, you MUST identify this as a mismatch and follow the recipe.
3.  **Implement Intent**: Focus on the technical implementation of the user's specific rules for filtering, ID mapping, and transformations.
**CRITICAL - ADAPTER TYPE**: 
- If 'Adapter Type: nodes_only', you MUST NOT identify a Target ID. 
- If 'Adapter Type: edges_only', you MUST identify BOTH a Source ID AND a Target ID. An edge REQUIRES two nodes.
"""
    elif semantic_mappings:
        # If no explicit recipe, provide a generic one to guide the LLM
        recipe_text = "Extract source and target IDs based on semantic column names."
        if adapter_type == 'nodes_only':
            recipe_text = "Extract source ID only (Node-only adapter)."
        
        logic_recipe_section = f"""
## LOGIC HINT:
```
{recipe_text}
```
**CRITICAL**: Analyze the semantic column mappings to identify the most appropriate columns for IDs.
"""

    # Add semantic context if available
    semantic_context = ""
    if semantic_mappings and semantic_mappings.get('column_definitions'):
        col_defs = semantic_mappings['column_definitions']
        semantic_context = f"""
## SEMANTIC COLUMN MAPPINGS (Pre-analyzed):
The following column mappings have been pre-analyzed:
"""
        for idx, name in col_defs.items():
            semantic_context += f"- Column {idx}: {name}\n"
        
        # Add structure analysis if available (CRITICAL for composite fields)
        if semantic_mappings.get('structure_analysis'):
            struct = semantic_mappings['structure_analysis']
            semantic_context += "\n## TECHNICAL COLUMN STRUCTURE (Authoritative):\n"
            
            for col_idx, col_info in struct.get('columns', {}).items():
                if col_info.get('is_composite'):
                    semantic_context += f"- **Column {col_idx} (COMPOSITE)**:\n"
                    semantic_context += f"  - Internal Delimiters: {col_info.get('internal_delimiters', [])}\n"
                    if col_info.get('parts'):
                        semantic_context += "  - Parts Sequence:\n"
                        for p_idx, part in enumerate(col_info['parts']):
                            semantic_context += f"    * Part {p_idx}: {part.get('type')} (Position: {part.get('position')})\n"
            
            if struct.get('source_id_example'):
                semantic_context += f"- **Source ID Example**: {struct['source_id_example']}\n"
            if struct.get('target_id_example') and adapter_type != 'nodes_only':
                semantic_context += f"- **Target ID Example**: {struct['target_id_example']}\n"
                
            semantic_context += "\n**CRITICAL**: Use the Technical Column Structure and Semantic Column Mappings above to identify which columns to use for IDs in your logic_interpretation.\n"
        
        # Add auxiliary mappings
        if semantic_mappings.get('auxiliary_mappings'):
            semantic_context += "\n## SUPPLEMENTAL DATA MAPPINGS (Pre-analyzed):\n"
            for param, aux_map in semantic_mappings['auxiliary_mappings'].items():
                semantic_context += f"### {param}\n"
                if aux_map.get('column_definitions'):
                    for idx, name in aux_map['column_definitions'].items():
                        semantic_context += f"- Column {idx}: {name}\n"
                if aux_map.get('structure_analysis'):
                    aux_struct = aux_map['structure_analysis']
                    for col_idx, col_info in aux_struct.get('columns', {}).items():
                        if col_info.get('is_composite'):
                            semantic_context += f"- **Column {col_idx}** in {param} is composite (splitting needed).\n"

        # Add entity reasoning
        if semantic_mappings.get('relationship_mappings'):
            semantic_context += "\n## ENTITY IDENTIFICATION REASONING (Pre-analyzed):\n"
            for entity_name, mapping in semantic_mappings['relationship_mappings'].items():
                semantic_context += f"- **Entity: {entity_name}**\n"
                semantic_context += f"  - Reasoning: {mapping.get('reasoning', 'No reasoning provided.')}\n"
                semantic_context += f"  - Confidence: {mapping.get('confidence', 0.0)}\n"
                if mapping.get('properties'):
                    semantic_context += "  - **Property Mappings**:\n"
                    for prop_name, col_idx in mapping['properties'].items():
                        semantic_context += f"    * {prop_name}: Column {col_idx}\n"

        semantic_context += "\n**Use these semantic names when referring to columns in your analysis.**\n"

    output_format = """
## TASK 4: Gold Standard Narrative Example
Use this example as a template for your `logic_interpretation`. Notice the explicit column indices and transformation details:
"**EXAMPLE**: (1) RECIPE: Source ID: [IDX1], Target ID: [IDX2]. (2) ARGS SYNTHESIS: '[MAIN_FILE]' is the primary source. '[AUX_FILE]' is an Identifier Mapping loaded as a dict. (3) SOURCE COLUMN: Column [IDX1] ([NAME1]). (4) TARGET COLUMN: Column [IDX2] ([NAME2]). **DATA TRANSFORMATION**: The raw value in Column [IDX] is [DESCRIPTION]. The code MUST [ACTION]. (5) PROPERTIES: Column [IDX3] ([NAME3]). (6) JOIN TYPE: N/A. (7) AUXILIARY FILES: '[AUX_FILE]' is loaded as a dict. **EXPLICIT MAPPING**: Map Column [IDX4] ([NAME4]) to its [VALUE_TYPE] by looking up its value in the '[AUX_FILE]' dictionary; use the result for [mapped_to: property:PROP_NAME] as [lookup_intent: enrichment]. (8) ID SUBSTITUTION: The raw Target ID from column [IDX2] MUST be replaced by the [MAPPED_ID] retrieved from '[AUX_FILE]' using the [KEY]. (9) CONFIG ARGS USAGE: If '[ARG]' is '[VAL]', iterate over filepath."

**CRITICAL - USE COLUMN STRUCTURE EXPLANATIONS**: If the SEMANTIC COLUMN MAPPINGS section above provides column indices, you MUST reference them in your logic_interpretation sections (3), (4), and (8). 

**SEMANTIC FILTER EXTRACTION (CRITICAL)**: Analyze the "reasoning" provided in the Semantic Mappings. If the reasoning identifies the target entity type based on specific column values or internal conditions (e.g., "identified when Column X contains Y"), you MUST explicitly include a corresponding technical filtering step in your logic (e.g., "Skip rows unless Column X matches Y").

---

IMPORTANT: Return ONLY valid JSON with this structure:
{
  "args_analysis": {
    "arg_name": {
      "role": "Primary Data Source | Supplemental Data Source | Identifier Mapping | Execution Switch | Static Metadata",
      "purpose": "What this argument does",
      "usage": "For Execution Switches: specify branching logic. For others: describe how the value is used.",
      "typical_values": ["example values"]
    }
  },
    "adapter_analysis": {
      "logic_interpretation": "MANDATORY SEMANTIC SYNTHESIS — 1. BASELINE: Briefly summarize the inferred logic from file inspection. 2. MISMATCH ANALYSIS: Identify every point where the Researcher's Logic Recipe conflicts with or adds to the baseline. 3. SEMANTIC AUTHORITY: Explain how you are overriding the baseline to implement the recipe's specific intent for IDs, auxiliary files, processors, and filtering. 4. TECHNICAL STEPS: Provide a step-by-step blueprint of the final implementation.",
    "purpose": "Technical mission statement referencing the Primary Data Source and the logical condition that defines the adapter output.",
    "source_column": { "index": "Exact column index or composite notation — must be grounded in logic_interpretation section 3", "description": "What this ID represents" },
    "target_column": { "index": "Exact column index or composite notation (REQUIRED for edges) — must be grounded in logic_interpretation section 4", "description": "What this ID represents" },
    "properties": [ { "column_index": "<number>", "name": "snake_case_property_name", "description": "What this column represents — must be grounded in logic_interpretation section 5" } ],
    "data_quality_issues": ["Any known issues: missing values, type coercions, composite keys, encoding problems. If composite fields exist, mention them here."],
    "join_type": "N/A | left-join | lookup | cross-file (only specify if logic recipe requires joins)",
    "auxiliary_file_usage": {
      "needed_files": ["exact_param_name_from_args"],
      "technical_instructions": ["Load 'param_name' into a dict (Specify KEY column index and VALUE column index)", "Map Column Z (specify index) to its ontology ID using this dict"]
    },
    "explicit_auxiliary_mappings": [
      {
        "auxiliary_file_param": "exact_param_name",
        "primary_column_index": "index of the column in the main data used for lookup",
        "primary_column_name": "semantic name of the column",
        "lookup_key_description": "Description of how the key is derived (e.g., 'raw value', 'split by space and take first')",
        "mapped_to": "Exact target: Use 'id:source_id' or 'id:target_id' if replacing an ID, or 'property:PROPERTY_NAME' if adding/enriching a property",
        "lookup_intent": "substitution | enrichment (substitution: e.g., Column 10 is 'BRCA1' but lookup replaces it with 'ENSG...' to use as Target ID. enrichment: e.g., Column 27 is 'Liver', lookup adds 'UBERON:...' as a property without changing the IDs)"
      }
    ],
    "config_args_usage": {
      "arg_name": "One concrete technical instruction — must mirror logic_interpretation section 8"
    }
  }
}
"""

    prompt = (
        inspection_prompt
        + basic_params_block
        + semantic_context
        + analysis_prompt
        + logic_recipe_section
        + output_format
    )

    print("[*] LLM Call 1/2 — Adapter analysis & logic interpretation...")
    try:
        response = llm(
            prompt,
            system=(
                "You are a senior bioinformatics developer and data engineer. "
                "Analyze the adapter structure in DEEP TECHNICAL DETAIL. "
                "Capture the complete algorithm, all business rules, "
                "join logic, and technical implementation details. Return valid JSON only."
            ),
        )
        analysis = extract_json(response)
        
        if not analysis:
            import sys
            print("[!] Error: Could not parse JSON from Call 1 response", file=sys.stderr)
            print(f"[!] Raw response (first 1000 chars): {response[:1000]}", file=sys.stderr)
            if len(response) > 1000:
                print(f"[!] Raw response (last 500 chars): ...{response[-500:]}", file=sys.stderr)
            return {}

        print("[+] LLM Call 1 complete — analysis & interpretation received")
        if analysis:
            print(f"\n[*] LLM Logic Interpretation (Stage 1):")
            # In Stage 1, logic interpretation might be inside 'adapter_analysis' or at top level
            logic = analysis.get('logic_interpretation')
            if not logic and 'adapter_analysis' in analysis:
                logic = analysis['adapter_analysis'].get('logic_interpretation')
            print(f"    {logic or 'N/A'}")

        # Flatten adapter_analysis to top level for backward compatibility
        if 'adapter_analysis' in analysis:
            for key, value in analysis['adapter_analysis'].items():
                analysis[key] = value

        return analysis

    except Exception as e:
        import sys
        print(f"[!] Warning: LLM Call 1 failed: {e}", file=sys.stderr)
        return {}


def _llm_call_2_requirements(analysis: dict, llm, semantic_mappings: dict = None, adapter_type: str = 'both', logic_recipe_rules: str = None) -> dict:
    """LLM Call 2 — Comprehensive Implementation Steps.

    Takes the analysis dict from Call 1 and produces a COMPLETE sequence of
    technical steps, including ID extraction, property mapping, and processor usage.
    """

    # Add semantic context if available
    semantic_context = ""
    if semantic_mappings and semantic_mappings.get('structure_analysis'):
        struct = semantic_mappings['structure_analysis']
        semantic_context = "\n## TECHNICAL COLUMN STRUCTURE (MANDATORY IMPLEMENTATION DETAILS):\n"
        
        for col_idx, col_info in struct.get('columns', {}).items():
            if col_info.get('normalization'):
                semantic_context += f"- **Column {col_idx} NORMALIZATION**: {col_info['normalization']}\n"
            
            if col_info.get('is_composite'):
                semantic_context += f"- **Column {col_idx} COMPOSITE STRUCTURE**:\n"
                semantic_context += f"  - Delimiters found: {col_info.get('internal_delimiters', [])}\n"
                if col_info.get('parts'):
                    for p_idx, part in enumerate(col_info['parts']):
                        semantic_context += f"    * Part {p_idx}: {part.get('type')} ({part.get('position')})\n"
                        if part.get('normalization'):
                            semantic_context += f"      - **NORMALIZATION REQUIRED**: {part['normalization']}\n"
        
        if struct.get('source_id_example'):
            semantic_context += f"- **Source ID Example**: {struct['source_id_example']}\n"
        if struct.get('target_id_example') and adapter_type != 'nodes_only':
            semantic_context += f"- **Target ID Example**: {struct['target_id_example']}\n"
            
        semantic_context += "\n**CRITICAL**: Your technical steps MUST exactly match the delimiter sequence and column indices described in the technical structure. Do NOT guess the delimiters.\n"

    prompt = f"""
## CONTEXT: Adapter Analysis Already Completed

Use the following narrative as your AUTHORITATIVE source of truth.

### logic_interpretation:
{analysis.get('logic_interpretation', 'No interpretation available')}

### purpose:
{analysis.get('purpose', '')}

### filtering_guards (MANDATORY Logic):
{analysis.get('filtering_guards', [])}

### SUPREME DIRECTIVE (Logic Recipe):
```
{logic_recipe_rules or 'No specific recipe provided.'}
```

### PROPERTY MAPPING:
{json.dumps(analysis.get('property_mapping', {}), indent=2)}

### EXPLICIT AUXILIARY MAPPINGS:
{json.dumps(analysis.get('explicit_auxiliary_mappings', []), indent=2)}

{semantic_context}
---

## YOUR TASK: Generate the COMPLETE Implementation Sequence

Convert the logic_interpretation above into a sequence of concrete, actionable technical steps. 
**You are responsible for the entire sequence.** Do NOT assume the code will add standard steps later.

**YOU MUST INCLUDE STEPS FOR:**
1. **Setup**: Loading any auxiliary files or initializing processors.
2. **Iteration**: Opening and iterating over the primary data source.
3. **Metadata Extraction & Missing Values**: 
    - If specific context (e.g., tissue, species) is required but NOT in the tabular columns, identify and extract it from the appropriate metadata source as defined in the Logic Recipe.
    - **CRITICAL**: Only Source ID and Target ID are mandatory for row validity. Do NOT generate steps that skip rows solely because a property (score, prediction, etc.) is missing. Instead, map missing property values to `None`.
4. **ID Extraction & Mapping**: State exactly which columns provide the raw IDs. 
    - **SEMANTIC INHERITANCE**: You MUST use the EXACT normalization and extraction instructions provided in the **TECHNICAL COLUMN STRUCTURE** section. 
    - **COMPOSITE EXTRACTION**: 
        * For `key_value` structures, specify: "Extract the value for key 'KEY_NAME' using the exact delimiters discovered. You MUST provide the literal delimiters."
        * For `positional` structures, specify: "Extract the value from Part X (position) using the exact delimiters discovered. You MUST provide the literal delimiters."
    - **CRITICAL**: For Source ID, you MUST use the column(s) identified in the logic interpretation. If multiple columns are needed for a unique ID (e.g., coordinates), specify them as a comma-separated list of the actual indices found in the file (e.g., 'idx1,idx2').
    - **CRITICAL**: If this is a 'nodes_only' adapter ({adapter_type}), you MUST NOT extract a Target ID.
    - **CRITICAL**: If this is an edge relationship, you MUST extract the Target ID from the column(s) identified in the logic interpretation. Again, use a comma-separated list of indices for composite IDs if the data requires it.
    - The final yield step MUST explicitly mention yielding the Source ID (and Target ID if applicable) as part of the BioCypher tuple.
5. **Property Mapping**: You MUST explicitly list each property name and the EXACT column index or metadata source used to populate it (e.g., 'Map Column X to property Y'). 
6. **Filtering & Logic Guards (MANDATORY)**: If the `filtering_guards`, `logic_interpretation`, or `purpose` mentions any conditions or focus constraints (e.g., "only when Column X contains Y"), you MUST include a corresponding technical filtering step immediately after the iteration step (e.g., "Check if Column X matches Y; if not, skip the row").
7. **Auxiliary File Usage (MANDATORY if auxiliary files exist)**: For EACH auxiliary file listed in the `auxiliary_files` section, you MUST include a specific technical step explaining:
    - **A**: How the file is loaded into a lookup dictionary in the constructor.
    - **B**: Exactly which column index from the primary data is used as the lookup key.
    - **C**: Exactly which property or ID is updated with the result of the lookup.
    - **Example**: "Use [COLUMN_INDEX] ([SEMANTIC_NAME]) as a key to look up the [VALUE_TYPE] in [AUXILIARY_FILE_PARAM]; store the result in property [PROPERTY_NAME]."
8. **Processing (MANDATORY if processor exists)**: If a processor or processors are listed in the `processors` section or mentioned in the 'ALGORITHM INTENT', you MUST include a specific technical step explaining exactly where it is called, what its input column is, and what its output represents (e.g., 'Pass the raw value from Column X to Processor Y to obtain the mapped value Z used for yielding').
9. **Output**: Yielding the final record.

**MANDATORY RULES:**
- **NO PLACEHOLDERS**: Never use 'column X', 'TARGET COLUMN', or 'Y'. Use the exact indices or keys from the narrative and technical context.
- **TRANSFORMATIONS**: If the narrative or technical context mentions a transformation (e.g., split by a specific character), you MUST include that specific transformation step as part of the ID extraction logic.
- **Exact Keys**: Use exact YAML keys (e.g., 'filepath', 'label').
- **Verb-First**: Start each step with Load, Build, Iterate, Skip, Map, or Yield.
- **Order Matters**: List steps in the order they must be executed in code.

Return ONLY valid JSON:
{{
  "logic_requirements": [
    "- [ORDERED STEP 1]",
    "- [ORDERED STEP 2]",
    "... etc ..."
  ],
  "verification": {{
    "all_logic_covered": true,
    "property_mapping_included": true,
    "id_extraction_included": true,
    "notes": "Any issues found"
  }}
}}
"""

    print("[*] LLM Call 2/2 — Generating comprehensive implementation steps...")
    try:
        response = llm(
            prompt,
            system="You are a senior bioinformatics developer. Generate a complete, sequential list of implementation steps based on the provided logic narrative. Return valid JSON only.",
        )
        result = extract_json(response)
        
        if not result:
            import sys
            print("[!] Error: Could not parse JSON from Call 2 response", file=sys.stderr)
            print(f"[!] Raw response (first 1000 chars): {response[:1000]}", file=sys.stderr)
            if len(response) > 1000:
                print(f"[!] Raw response (last 500 chars): ...{response[-500:]}", file=sys.stderr)
            return {}

        print("[+] LLM Call 2 complete — comprehensive steps received")
        if result:
            print(f"\n[*] LLM Implementation Plan (Stage 2):")
            steps = result.get('logic_requirements', [])
            if steps:
                for i, step in enumerate(steps[:5], 1):
                    print(f"    {i}. {step}")
                if len(steps) > 5:
                    print(f"    ... ({len(steps)-5} more steps)")
        return result

    except Exception as e:
        import sys
        print(f"[!] Warning: LLM Call 2 failed: {e}", file=sys.stderr)
        return {}

def ask_llm_questions(inspection: dict, adapter_config: dict, adapter_name: str,
                      basic_params: dict = None, logic_recipe_rules: str = None,
                      semantic_mappings: dict = None, adapter_type: str = 'both') -> dict:
    """Orchestrate two focused LLM calls to stay within output token limits.

    Call 1 - adapter identity, args classification, logic interpretation.
    Call 2 - concrete implementation steps + self-correction verification.
    Returns a merged dict compatible with the existing downstream code.
    """
    llm = make_llm_client()

    analysis = _llm_call_1_analysis(
        inspection, adapter_config, adapter_name, basic_params, logic_recipe_rules, llm, semantic_mappings, adapter_type=adapter_type
    )
    if not analysis:
        return {}

    call2 = _llm_call_2_requirements(analysis, llm, semantic_mappings=semantic_mappings, adapter_type=adapter_type, logic_recipe_rules=logic_recipe_rules)

    if call2:
        analysis['logic_requirements'] = call2.get('logic_requirements', [])
        analysis['verification'] = call2.get('verification', {})
        verification = analysis['verification']
        if not verification.get('no_contradictions', True):
            print(f"  Self-correction note: {verification.get('notes', '')}")

    return analysis


def generate_specification_from_analysis(analysis: dict, inspection: dict, adapter_config: dict, adapter_name: str, 
                                   logic_recipe_rules: str = None, source_type: str = None, target_type: str = None,
                                   source_id: str = None, target_id: str = None,
                                   semantic_mappings: dict = None, processor_name: str = None, 
                                   processor_target: str = None, processors_list: list = None,
                                   adapter_type: str = 'both') -> dict:
    """Generate adapter specification from LLM analysis and file inspection."""
    semantic_mappings = semantic_mappings or {}
    
    main_file = inspection.get('main_file', {})
    metadata = main_file.get('metadata', {})
    
    file_format = metadata.get('file_format', 'txt').lower()
    format_type = metadata.get('format_type', 'tabular')
    
    # Always use LLM column mapper for all formats
    use_llm_column_mapper = True
    print(f"[*] Using LLM column mapper for {file_format} format ({format_type})")
    
    source_col_info = analysis.get('source_column', 0)
    target_col_info = analysis.get('target_column', 1)

    if isinstance(source_col_info, dict):
        source_col = source_col_info.get('index', 0)
    else:
        source_col = source_col_info
    
    if isinstance(target_col_info, dict):
        target_col = target_col_info.get('index', 1)
    else:
        target_col = target_col_info
    
   
    source_col = source_id if source_id is not None else source_col
    target_col = target_id if target_id is not None else target_col
    
    # Nullify target column for nodes_only adapters
    if adapter_type == 'nodes_only':
        target_col = None
    
    print(f"[*] Authoritative Source column: {source_col}")
    print(f"[*] Authoritative Target column: {target_col}")

    analysis['source_column'] = {"index": source_col, "description": "Authoritative Mapping"}
    if adapter_type != 'nodes_only':
        analysis['target_column'] = {"index": target_col, "description": "Authoritative Mapping"}
    else:
        analysis['target_column'] = None


    properties = analysis.get('properties', [])
    
    expected_columns = {}
    semantic_col_defs = semantic_mappings.get('column_definitions', {})
    headers = metadata.get('headers', [])
    for i, header in enumerate(headers):
        # Use semantic name from mapper if it exists, otherwise fallback to index/header
        col_name = semantic_col_defs.get(i, semantic_col_defs.get(str(i), header))
        expected_columns[i] = col_name
    
    properties_map = {}
    for prop in properties:
        if isinstance(prop, dict):
            col_idx = prop.get('column_index', prop.get('column'))
            prop_name = prop.get('name', f'property_{col_idx}')
            
            is_source = str(col_idx).strip().upper() == str(source_col).strip().upper()
            is_target = str(col_idx).strip().upper() == str(target_col).strip().upper()
            
            if not is_source and not is_target:
                properties_map[prop_name] = col_idx
    
    auxiliary_files = {}
    main_file_param = None

    main_file = inspection.get('main_file', {})
    main_file_param = main_file.get('param_name')
    if main_file_param:
        print(f"[*] Confirmed main file parameter: {main_file_param}")

    for param_name, file_info in inspection.get('files', {}).items():
        if param_name == main_file_param:
            continue 
        ftype = file_info.get('type', '')
        aux_format = 'pickle' if ftype == 'pickle' else 'tabular'
        entry = {
            'filepath': file_info.get('path'),
            'format': aux_format,
            'purpose': 'Auxiliary data for enrichment/mapping',
            'usage': f"Use {param_name} to enrich/map data values",
        }
        if aux_format == 'tabular' and 'metadata' in file_info:
            meta = file_info['metadata']
            headers = meta.get('headers', [])

            aux_semantic = semantic_mappings.get('auxiliary_mappings', {}).get(param_name, {})
            aux_col_defs = aux_semantic.get('column_definitions', {})
            
            columns_dict = {}
            for idx, header in enumerate(headers):
                # Use semantic name if available
                col_name = aux_col_defs.get(idx, aux_col_defs.get(str(idx), header))
                columns_dict[idx] = col_name
            
            entry['columns'] = columns_dict
            entry['delimiter'] = meta.get('delimiter', '\t')
            entry['compression'] = meta.get('compression', 'none')
            entry['has_header'] = meta.get('has_header', False)
            # Add column descriptions for clarity
            entry['column_descriptions'] = {
                idx: aux_col_defs.get(idx, aux_col_defs.get(str(idx), f"Column {idx}: {header}")) 
                for idx, header in enumerate(headers)
            }
        auxiliary_files[param_name] = entry
    
    # Build adapter specification
    spec = {
        'name': adapter_name,
        'source_name': inspection.get('file_metadata', {}).get('input_label', adapter_name),
        'source': adapter_name,
        'source_url': '<url>',
        'config_arguments': list(adapter_config.get('adapter', {}).get('args', {}).keys()),
        'expected_columns': expected_columns,
        'data_format': {
            'delimiter': metadata.get('delimiter', '\t'),
            'compression': metadata.get('compression', 'none'),
            'has_header': metadata.get('has_header', False)
        },
        'main_file_param': main_file_param
    }
    
    generate_nodes = adapter_config.get('nodes', adapter_config.get('adapter', {}).get('nodes', True))
    generate_edges = adapter_config.get('edges', adapter_config.get('adapter', {}).get('edges', True))
    
    if generate_nodes and generate_edges:
        adapter_type = 'both'
    elif generate_nodes and not generate_edges:
        adapter_type = 'nodes_only'
    elif not generate_nodes and generate_edges:
        adapter_type = 'edges_only'
    else:
        adapter_type = 'none'
    
    spec['adapter_type'] = adapter_type
    
    column_explanations = semantic_mappings.get('column_explanations', {})
    
    rel_entry = {
        'name': f'{adapter_name}_relationship',
        'input_label': adapter_config.get('adapter', {}).get('args', {}).get('label', 'node' if adapter_type == 'nodes_only' else 'edge'),
        'source': source_type or adapter_name,
        'source_column': 'filename' if (source_id and str(source_id).upper() == 'FILENAME') else source_col,
        'source_column_explanation': column_explanations.get('source_column', ''),
        'properties': properties_map
    }
    
    if adapter_type != 'nodes_only':
        rel_entry['target'] = target_type or 'gene'
        rel_entry['target_column'] = 'filename' if (target_id and str(target_id).upper() == 'FILENAME') else target_col
        rel_entry['target_column_explanation'] = column_explanations.get('target_column', '')
    
    spec['relationships'] = [rel_entry]
    
    # Trigger rich steps if there's a join, a custom recipe, logic requirements, auxiliary files, or processors
    has_complex_logic = (
        logic_recipe_rules or 
        analysis.get('logic_requirements') or 
        auxiliary_files or 
        processor_name or 
        processors_list
    )
    
    if has_complex_logic:
        if not logic_recipe_rules:
            self_recipe = analysis.get('purpose', "Implement adapter logic")
            recipe = f"Self-Generated Logic: {self_recipe}"
        else:
            recipe = logic_recipe_rules
            
        comprehensive_steps = [
            f"ALGORITHM INTENT: {recipe}",
            "",
            "TECHNICAL IMPLEMENTATION REQUIREMENTS:",
        ]
        
        for issue in analysis.get('data_quality_issues', []):
            if 'composite' not in issue.lower():
                comprehensive_steps.append(f"- {issue}")

        for file_param, file_info in auxiliary_files.items():
            if file_info.get('has_header') is True:
                comprehensive_steps.append(f"- Skip header row when processing {file_param}")
        
        if metadata.get('has_header') is True:
            main_file_name = main_file.get('param_name', 'main file')
            comprehensive_steps.append(f"- Skip header row when processing {main_file_name}")

        logic_specific_requirements = analysis.get('logic_requirements', [])
        comprehensive_steps.extend(logic_specific_requirements)

        # Standard safety steps
        comprehensive_steps.extend([
            "- Parse main data file according to data_format specification",
            "- Validate row structure and skip malformed rows", 
            "- Handle empty/null values appropriately (None for numeric, '' for text)",
            "- Implement type conversions with error handling",
        ])

        spec['relationships'][0]['implementation'] = {
            'algorithm': 'comprehensive_adapter_logic',
            'steps': comprehensive_steps
        }
    else:
        spec['relationships'][0]['implementation'] = {
            'algorithm': 'direct_mapping',
            'steps': [
                " ALGORITHM INTENT: Direct column-to-column mapping",
                "Load main data file and extract specified columns",
                " Yield (source_column, target_column, label, properties) for each row"
            ]
        }
    
    # Add processor configuration if provided
    if processor_name or processor_target:
        spec['processors'] = {
            'processor_name': processor_name,
            'processor_target': processor_target,
            'description': f"Use {processor_name} to convert {processor_target} ID(s)"
        }
    elif processors_list:
        spec['processors'] = {
            'processors_list': processors_list,
            'count': len(processors_list),
            'description': f"Use {len(processors_list)} processors for ID conversion"
        }
    
    # Add auxiliary files if present
    if auxiliary_files:
        spec['auxiliary_files'] = auxiliary_files
    
    # Add analysis notes
    spec['analysis'] = {
        'logic_interpretation': analysis.get('logic_interpretation', 'No deconstruction available'),
        'purpose': analysis.get('purpose', 'Unknown'),
        'data_quality_issues': analysis.get('data_quality_issues', []),
        'auxiliary_file_usage': analysis.get('auxiliary_file_usage', {}),
        'explicit_auxiliary_mappings': analysis.get('explicit_auxiliary_mappings', []),
        'config_args_usage': analysis.get('config_args_usage', {}),
        'args_analysis': analysis.get('args_analysis', {})
    }

    
    
    return spec


def parse_property_mappings(properties_json: str) -> dict:
    """Parse property mappings/types from JSON string."""
    if not properties_json:
        return {}
    try:
        return json.loads(properties_json)
    except json.JSONDecodeError:
        print(f"[!] Warning: Could not parse properties JSON: {properties_json}")
        return {}


def main():
    parser = argparse.ArgumentParser(description='Generate specification from adapter config with enhanced inspection')
    parser.add_argument('--adapter-config', required=True, help='Path to adapter config YAML')
    parser.add_argument('--adapter-name', required=True, help='Name of adapter in config')
    parser.add_argument('--output', required=True, help='Output specification YAML path')
    parser.add_argument('--logic-recipe', help='Researcher algorithm/join logic recipe')
    parser.add_argument('--source-type', help='Source entity type (e.g., protein, gene)')
    parser.add_argument('--target-type', help='Target entity type (e.g., biological_process, disease)')
    parser.add_argument('--source-id', help='Explicit source ID column index or name')
    parser.add_argument('--target-id', help='Explicit target ID column index or name')
    parser.add_argument('--properties', help='JSON dict of property name to column index mappings')
    parser.add_argument('--processor-name', help='Name of processor to use (e.g., hgnc_processor, entrez_ensembl)')
    parser.add_argument('--processor-target', choices=['source', 'target', 'both'], help='Which ID(s) to process (source, target, or both)')
    parser.add_argument('--processors', help='JSON list of multiple processors with their targets')
   

    
    args = parser.parse_args()
    
    # Load adapter config
    print(f"[*] Loading adapter config from {args.adapter_config}...")
    with open(args.adapter_config) as f:
        adapters_config = yaml.safe_load(f)
    
    if args.adapter_name not in adapters_config:
        print(f"[!] Error: Adapter '{args.adapter_name}' not found in config")
        sys.exit(1)
    
    adapter_config = adapters_config[args.adapter_name]
    
    # Initialize debug trace for observability
    from datetime import datetime
    debug_trace = {
        "adapter_name": args.adapter_name,
        "timestamp": datetime.now().isoformat(),
        "input_config": adapter_config,
        "merged_schema": None,
        "llm_steps": {}
    }
    
    adapter_label = adapter_config.get('adapter', {}).get('args', {}).get('label')
    print(f"[*] Adapter label: {adapter_label}")
    
    adapter_label = adapter_config.get('adapter', {}).get('args', {}).get('label')
    print(f"[*] Adapter label: {adapter_label}")
    
    print(f"[*] Inspecting files for {args.adapter_name}...")
    inspection = inspect_adapter_files(adapter_config)
    main_file = inspection.get('main_file')
    if not main_file:
        files = inspection.get('files', {})
        print(f"[!] Error: Could not find main data file for adapter in {list(files.keys())}")
        sys.exit(1)
    
    # Detect adapter type
    generate_nodes = adapter_config.get('nodes', adapter_config.get('adapter', {}).get('nodes', True))
    generate_edges = adapter_config.get('edges', adapter_config.get('adapter', {}).get('edges', True))
    
    adapter_type = 'both'
    if generate_nodes and not generate_edges:
        adapter_type = 'nodes_only'
    elif not generate_nodes and generate_edges:
        adapter_type = 'edges_only'

    adapter_label = adapter_config.get('adapter', {}).get('args', {}).get('label')
    semantic_mappings = generate_column_mappings(
        inspection, 
        args.adapter_name, 
        adapter_label,
        source_entity_hint=args.source_type,
        target_entity_hint=args.target_type,
        source_id=args.source_id,
        target_id=args.target_id,
        adapter_type=adapter_type
    )
    
    # Debug: Print semantic mappings
    print(f"[DEBUG] Semantic mappings result: {semantic_mappings}")
    if semantic_mappings:
        print(f"[DEBUG] Column definitions: {semantic_mappings.get('column_definitions', {})}")
    
    print(f"[*] Analyzing adapter structure with LLM...")
    
    basic_params = {
        'source_type': args.source_type,
        'target_type': args.target_type,
        'source_id': args.source_id,
        'target_id': args.target_id,
        'processor_info': None
    }
    
    if args.processor_name:
        basic_params['processor_info'] = {
            'processor_name': args.processor_name,
            'processor_target': args.processor_target or 'source'
        }
    elif args.processors:
        try:
            basic_params['processor_info'] = json.loads(args.processors)
        except json.JSONDecodeError:
            print(f"[!] Warning: Invalid JSON for processors: {args.processors}")
            basic_params['processor_info'] = None
    
    logic_recipe_rules = args.logic_recipe
    
    src_idx = args.source_id
    tgt_idx = args.target_id
    
    if not src_idx or not tgt_idx:
        if semantic_mappings and 'relationship_mappings' in semantic_mappings:
            mapping = next(iter(semantic_mappings['relationship_mappings'].values()))
            if not src_idx:
                src_idx = mapping.get('source_column')
            if not tgt_idx and adapter_type != 'nodes_only':
                tgt_idx = mapping.get('target_column')
    
    if src_idx is not None or tgt_idx is not None:
        recipe_prefix = ""
        if src_idx is not None:
             recipe_prefix += f"AUTHORITATIVE MAPPING: Source ID is in Column {src_idx}. "
        if tgt_idx is not None and adapter_type != 'nodes_only':
             recipe_prefix += f"AUTHORITATIVE MAPPING: Target ID is in Column {tgt_idx}. "
            
        if logic_recipe_rules:
            if src_idx is not None:
                logic_recipe_rules = logic_recipe_rules.replace("Source ID: None", f"Source ID: {src_idx}")
            if tgt_idx is not None:
                logic_recipe_rules = logic_recipe_rules.replace("Target ID: None", f"Target ID: {tgt_idx}")
            
            # Prepend authoritative prefix if not already mentioned
            if src_idx is not None and f"Source ID: {src_idx}" not in logic_recipe_rules:
                logic_recipe_rules = recipe_prefix + logic_recipe_rules
        else:
            logic_recipe_rules = recipe_prefix
    
    try:
        analysis = ask_llm_questions(
            inspection, 
            adapter_config, 
            args.adapter_name, 
            basic_params=basic_params,
            logic_recipe_rules=logic_recipe_rules,
            semantic_mappings=semantic_mappings,
            adapter_type=adapter_type
        )
    except Exception as e:
        print(f"[!] Warning: LLM analysis timed out or failed: {e}")
        print(f"[*] Using fallback analysis...")
        analysis = {
            'source_column': 0,  # Will be overridden by semantic mappings
            'target_column': 1,  # Will be overridden by semantic mappings
            'properties': [],
            'join_type': 'direct',
            'purpose': logic_recipe_rules or 'Direct mapping',
            'logic_interpretation': logic_recipe_rules or 'Direct column mapping',
            'logic_requirements': [],
            'data_quality_issues': [],
            'auxiliary_file_usage': {},
            'config_args_usage': {},
            'args_analysis': {}
        }
 
    
    if not analysis:
        print(f"[!] Error: LLM analysis failed")
        sys.exit(1)
    
    user_properties = parse_property_mappings(args.properties) if args.properties else {}
    if user_properties:
        print(f"[*] User provided {len(user_properties)} property definitions - these will be MERGED with LLM suggestions")
        
        existing_props = analysis.get('properties', [])
        props_dict = {p['name']: p for p in existing_props if isinstance(p, dict) and 'name' in p}
        
        for prop_name, prop_val in user_properties.items():
            desc = f"User-specified property of type {prop_val}" if isinstance(prop_val, str) else f"User-specified property from column {prop_val}"
            props_dict[prop_name] = {
                "column_index": prop_val,
                "name": prop_name,
                "description": desc
            }
        
        # Update analysis with merged list
        analysis['properties'] = list(props_dict.values())
        print(f"[+] Merged property definitions. Total properties: {len(analysis['properties'])}")

    debug_trace["llm_steps"]["pattern_analysis"] = inspection.get("pattern_analysis", {})
    print(f"[*] Generating specification from analysis...")
    spec = generate_specification_from_analysis(
        analysis, inspection, adapter_config, args.adapter_name, 
        logic_recipe_rules=logic_recipe_rules,
        source_type=args.source_type,
        target_type=args.target_type,
        source_id=args.source_id,
        target_id=args.target_id,
        semantic_mappings=semantic_mappings,
        processor_name=args.processor_name,
        processor_target=args.processor_target,
        processors_list=basic_params['processor_info'].get('processors') if (basic_params.get('processor_info') and isinstance(basic_params['processor_info'], dict)) else None,
        adapter_type=adapter_type
    )
    debug_trace["llm_steps"]["final_specification"] = spec
    
    # Save debug trace
    save_debug_trace(args.adapter_name, debug_trace)
    
    # Write specification
    print(f"[*] Writing specification to {args.output}...")
    with open(args.output, 'w') as f:
        yaml.dump(spec, f, sort_keys=False, default_flow_style=False)
    
    print(f"[+] Specification written to: {args.output}")
    
    # Print summary
    print(f"\n[+] Specification Summary:")
    print(f"    - Adapter: {args.adapter_name}")
    print(f"    - Main File: {inspection['main_file']['path']}")
    print(f"    - Columns: {len(spec['expected_columns'])}")
    print(f"    - Source Column: {spec['relationships'][0].get('source_column', 'N/A')}")
    print(f"    - Target Column: {spec['relationships'][0].get('target_column', 'N/A')}")
    print(f"    - Join Type: {spec['relationships'][0].get('join_type', 'N/A')}")
    print(f"    - Properties: {len(spec['relationships'][0].get('properties', {}))}")
    if inspection.get('auxiliary_files'):
        print(f"    - Auxiliary Files: {len(inspection['auxiliary_files'])}")


if __name__ == '__main__':
    main()
