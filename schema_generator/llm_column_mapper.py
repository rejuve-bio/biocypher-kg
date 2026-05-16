"""LLM-based column mapper for BioCypher data files.

Uses LLM with structured output to generate column definitions and
relationship property mappings based on source file metadata alone.
No external schema is required. The LLM infers semantics from column
names and sample data rows.
"""

import json
import sys
import yaml
import re
import os
from .code_fixer import extract_json

from pathlib import Path
from typing import Dict, Any, List

try:
    from pydantic import BaseModel, Field
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = object
    Field = lambda **kwargs: None

from .llm_client import make_llm_client


class ColumnDefinitions(BaseModel):
    """Column index to semantic name mappings."""
    mappings: Dict[int, str] = Field(
        description="Map from column index to semantic name"
    )


class RelationshipMapping(BaseModel):
    """Mapping for a single relationship."""
    properties: Dict[str, Any] = Field(
        description="Map from property name to column index or part"
    )
    confidence: float = Field(default=1.0, description="Confidence score between 0.0 and 1.0")
    reasoning: str = Field(default="", description="Brief explanation for the mapping")


class MappingOutput(BaseModel):
    """Complete mapping output structure."""
    column_definitions: Dict[int, str]
    relationship_mappings: Dict[str, RelationshipMapping]
    file_structure_type: str = Field(default="unknown")
    reasoning_for_type: str = Field(default="")
    inspector_validation: Dict[str, Any] = Field(default_factory=dict)
    columns: Dict[str, Any] = Field(default_factory=dict)
    parsing_required: bool = Field(default=False)
    recommendations: str = Field(default="")


class LLMColumnMapper:
    """Generates column mappings using LLM with structured output.

    The mapper uses only the file metadata (headers + sample rows) to infer
    semantic column names and source/target/property assignments.  No external
    schema file is needed.
    """

    def __init__(self, metadata: Dict[str, Any], llm_client=None, file_path: str = None):
        """Initialize mapper with file metadata.

        Args:
            metadata: Source metadata dict produced by SourceInspector.inspect()
                      Must contain: headers, sample_rows, delimiter, has_header
            llm_client: Optional LLM client (created automatically if omitted)
            file_path: Optional path to original file for re-reading if needed
        """
        if isinstance(metadata, (str, Path)):
            with open(metadata) as f:
                self.metadata = json.load(f)
        else:
            self.metadata = metadata

        self.llm = llm_client or make_llm_client()
        self.file_path = file_path


    def _is_headerless_data(self) -> bool:
        """Return True when headers are numeric indices (no real header row)."""
        headers = self.metadata.get("headers", [])
        if not headers:
            return True
        return all(str(h).isdigit() for h in headers)


    def read_raw_samples(self, num_samples: int = 10) -> List[str]:
        """Read raw lines directly from file, bypassing source inspector.
        
        Args:
            num_samples: Number of sample lines to read
            
        Returns:
            List of raw lines from the file
        """
        if not self.file_path:
            return []
        
        import gzip
        
        file_path = Path(self.file_path)
        if not file_path.exists():
            return []
        
        is_gzip = file_path.suffix == '.gz'
        comment_prefix = self.metadata.get("comment_lines")
        
        try:
            if is_gzip:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    lines = []
                    for _ in range(100):
                        line = f.readline()
                        if not line: break
                        line = line.rstrip('\n\r')
                        if comment_prefix and line.startswith(comment_prefix):
                            continue
                        # Also skip empty lines
                        if not line.strip():
                            continue
                        lines.append(line)
                        if len(lines) >= num_samples: break
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = []
                    for _ in range(100):
                        line = f.readline()
                        if not line: break
                        line = line.rstrip('\n\r')
                        if comment_prefix and line.startswith(comment_prefix):
                            continue
                        if not line.strip():
                            continue
                        lines.append(line)
                        if len(lines) >= num_samples: break
            
            return lines
        except Exception as e:
            print(f"Warning: Could not read raw file: {e}", file=sys.stderr)
            return []


    def analyze_column_structure(self, source_entity_hint: str = None, target_entity_hint: str = None, 
                                 source_id_hint: Any = None, target_id_hint: Any = None,
                                 adapter_type: str = 'both') -> Dict[str, Any]:
        """Analyze column structure to identify composite fields and patterns.
        
        This pre-analysis phase helps the LLM understand:
        - Which columns contain composite/concatenated data
        - What delimiters are used within columns
        - Potential source/target entity patterns
        - Normalization needs for identifiers (e.g., version stripping)
        
        Args:
            source_entity_hint: Optional hint about source entity type (e.g., "gene", "variant")
            target_entity_hint: Optional hint about target entity type (e.g., "protein", "disease")
            source_id_hint: Optional index/name of the column used for Source ID
            target_id_hint: Optional index/name of the column used for Target ID
        
        Returns:
            Structure analysis dict with patterns and recommendations
        """
        sample_rows = self.metadata.get("sample_rows", [])
        if not sample_rows:
            print(f"[DEBUG] analyze_column_structure: No sample_rows found in metadata. Keys: {list(self.metadata.keys())}")
            return {}
        
        system = "You are a data structure analysis expert. You analyze data patterns to identify composite fields, delimiters, and biological identifier normalization needs. You can override incorrect file format detection."
        
        # Get source inspector metadata
        delimiter = self.metadata.get("delimiter", "\\t")
        has_header = self.metadata.get("has_header", False)
        comment_lines = self.metadata.get("comment_lines", 0)
        delimiter_confidence = self.metadata.get("delimiter_confidence", 0.0)
        
        # Read raw samples if file path is available
        raw_samples = []
        filename = ""
        if self.file_path:
            raw_samples = self.read_raw_samples(10)
            filename = os.path.basename(self.file_path)
        
        prompt = f"Analyze the structure of this data file to identify composite fields and entity patterns.\n"
        if filename:
            prompt += f"**File Name**: {filename}\n"
        prompt += f"**Adapter Type**: {adapter_type}\n\n"
        
        prompt += f"""You are a data architect. Analyze the structure of this file to determine if columns are composite (contain multiple semantic values) and to identify the optimal delimiter.

**Source Metadata:**
- Detected Delimiter: {repr(delimiter)}
- Comment Line Prefix: {repr(comment_lines)}
- Has Header Row: {has_header}
- Delimiter Confidence: {delimiter_confidence:.2f}

**Sample Data (Comments Stripped):**
"""
        for i, row in enumerate(sample_rows[:5], 1):
            prompt += f"Row {i}: {row}\n"
        
        if raw_samples:
            prompt += f"""
**Raw File Lines (for verification):**
"""
            for i, line in enumerate(raw_samples[:10], 1):
                prompt += f"Line {i}: {line}\n"
        
        # Add entity hints if provided
        if source_entity_hint or target_entity_hint or source_id_hint is not None or target_id_hint is not None:
            prompt += f"""
**User Context & Intent:**
"""
            if source_entity_hint:
                prompt += f"- Source Entity Type: {source_entity_hint}\n"
            if target_entity_hint:
                prompt += f"- Target Entity Type: {target_entity_hint}\n"
            if source_id_hint is not None:
                prompt += f"- **DESIGNATED SOURCE ID COLUMN**: {source_id_hint}\n"
            if target_id_hint is not None:
                prompt += f"- **DESIGNATED TARGET ID COLUMN**: {target_id_hint}\n"
        
        prompt += """
**Your Task:**
1. VALIDATE the source inspector output - is the delimiter correct? Is has_header correct?
   - If delimiter confidence is low (<0.5), be CRITICAL and suggest corrections
   - Compare parsed rows with raw lines to verify delimiter detection
   - Check if this is actually a delimited tabular file or a different format
2. If source inspector is WRONG, provide corrected values in inspector_validation
3. Analyze each column (especially the DESIGNATED ID COLUMNS) to determine:
   - Does the column contain COMPOSITE data (multiple pieces of information concatenated)?
   - If composite, what are the internal delimiters? (common: '_', '$', '|', ':', '-', ';')
     - Check if IDs have version numbers.
     - Check for quotes, prefixes, or trailing characters.
     - Specify the exact action in the "normalization" field below.
   - What type of entities or information does each part represent?
   - Match the data type to semantic meanings from hints.
4. Extract actual example values.

**Analysis Guidelines:**
- Be CRITICAL of low-confidence delimiter detection.
- Compare raw lines with parsed rows to verify correctness.
- Look for repeating patterns across multiple rows.
- Identify delimiters that consistently separate different types of information.
- USE ENTITY HINTS to identify column types.
- **IDENTIFIER NORMALIZATION**: Analyze identifiers for any suffixes (like version numbers), prefixes, or quotes. If found, specify the normalization in the "normalization" field.
- **COMPOSITE COLUMNS**: If you don't see the entities in separate columns, look INSIDE the columns for internal delimiters like `_`, `$`, `:`, `|`, `;`, or ` `. 
- **COMPOSITE STRUCTURE DISCOVERY**: If a column contains multiple values, identify its structure:
    * **key_value**: Contains pairs like `key=value`, `key "value"`, or `key:value`.
    * **positional**: Contains values at fixed positions separated by delimiters (e.g., `chr1_100_200`).
- **NESTED DELIMITERS**: Some columns use MULTIPLE internal delimiters (e.g., `_` then `$`). Identify the exact sequence.
- **NO STRUCTURE**: If a column has no consistent internal delimiter (e.g., '_', ':', '|'), set `is_composite` to `false`. DO NOT invent structure or parts for plain text or simple IDs.
- **REQUIRED VERIFICATION**: Before selecting 'normal_tabular', verify that NO COLUMN contains multiple semantic values (e.g., 'ID;Name', 'Type=Value', 'chr:start-end'). If even ONE column contains multiple semantic parts, you MUST select 'composite'.

**Output Format - return ONLY valid JSON:**
{
  "file_structure_type": "composite" | "normal_tabular",
  "reasoning_for_type": "DETAILED EVIDENCE: Cite specific columns and patterns seen in the SAMPLE DATA that prove this structure (e.g., 'Column 8 contains key-value pairs separated by semicolons').",
  "inspector_validation": {
    "delimiter_correct": true/false,
    "delimiter_issues": "description if incorrect, empty string if correct",
    "has_header_correct": true/false,
    "has_header_issues": "description if incorrect, empty string if correct",
    "comment_lines_correct": true/false,
    "comment_lines_issues": "description if incorrect, empty string if correct",
    "suggested_delimiter": "suggested delimiter if current is wrong",
    "suggested_has_header": true/false
  },
    "columns": {
      "0": {
      "is_composite": true/false,
      "internal_delimiters": ["list", "of", "delimiters"],
      "structure": "brief description of what this column contains",
      "normalization": "normalization action if needed, else empty string",
        "parts": [
          {
          "position": "describe position",
          "type": "semantic type of this part",
          "example": "actual example from the data",
          "normalization": "normalization action if needed , else empty string"
          }
        ]
    },
    "1": {
      "is_composite": false,
      "type": "semantic type",
      "normalization": "normalization action if needed, else empty string"
      }
    },
  "parsing_required": true/false,
  "recommendations": "Brief recommendation for handling this structure"
}


**Important:**
- FIRST validate the source inspector output before analyzing columns
- Base your analysis ONLY on the actual data shown above

Analyze the data now:"""

        response = self.llm(prompt, system=system)
        
        try:
            structure = extract_json(response)
            if not structure:
                return {
                    "file_structure_type": "unknown",
                    "reasoning_for_type": "Technical Error: LLM response did not contain a valid JSON structure block.",
                    "columns": {}
                }
            
            # Ensure mandatory fields exist for logging and downstream logic
            if 'file_structure_type' not in structure:
                structure['file_structure_type'] = "unknown"
            if 'reasoning_for_type' not in structure:
                structure['reasoning_for_type'] = "Technical Warning: LLM omitted reasoning field."
            
            print(f"  [*] File Structure Analysis: {structure['file_structure_type']}")
            print(f"  [*] Structure Reasoning: {structure['reasoning_for_type']}")
            return structure
        except Exception as e:
            return {
                "file_structure_type": "error",
                "reasoning_for_type": f"Exception during structure analysis: {str(e)}",
                "columns": {}
            }


    def build_prompt(self, relationship_type: str = None, structure_analysis: Dict[str, Any] = None, adapter_type: str = 'both') -> str:
        """Build LLM prompt from file metadata and structure analysis.

        Args:
            relationship_type: Optional hint about the relationship being mapped
                               (e.g. 'gtex_variant_gene').  Used as context only.
            structure_analysis: Optional pre-analysis of column structure

        Returns:
            Prompt string
        """
        # Extract structure analysis overrides
        inspector_val = structure_analysis.get('inspector_validation', {}) if structure_analysis else {}
        
        final_delimiter = inspector_val.get('suggested_delimiter', self.metadata.get("delimiter", "\\t"))
        
        # Determine header status
        if 'suggested_has_header' in inspector_val:
            final_has_header = inspector_val['suggested_has_header']
        else:
            final_has_header = self.metadata.get('has_header', not self._is_headerless_data())

        headers = self.metadata["headers"]
        sample_rows = self.metadata["sample_rows"]
        
        is_composite = structure_analysis.get('file_structure_type') == 'composite' if structure_analysis else True
        
        prompt = f"""You are a data mapping expert. Your task is to map source data columns to semantic names and identify relationship properties.

**Source File Information:**
- Delimiter: {repr(final_delimiter)}
- Has Header Row: {final_has_header}
- Column Names / Indices: {headers}
- Comment Line Prefix: {repr(self.metadata.get("comment_lines", "None"))}
"""
        if relationship_type:
            prompt += f"- Relationship Hint: {relationship_type}\n"
        prompt += f"- Adapter Type: {adapter_type}\n"

        if not final_has_header:
            prompt += "\nNOTE: This file has NO header row. Column names above are numeric indices. Infer semantics from the sample data values below.\n"

        if is_composite and structure_analysis and structure_analysis.get('columns'):
            prompt += "\n**STRUCTURE ANALYSIS:**\n"
            prompt += "Pre-analysis has identified the following column structures:\n\n"
            for col_idx, col_info in structure_analysis.get('columns', {}).items():
                if col_info.get('is_composite'):
                    prompt += f"Column {col_idx}: COMPOSITE FIELD (Internal delimiters: {col_info.get('internal_delimiters', [])})\n"
            prompt += "\n"

        prompt += "\n**Sample Data Rows:**\n"
        for i, row in enumerate(sample_rows[:3], 1):
            prompt += f"Row {i}: {row}\n"

        if not is_composite:
            prompt += """
**Task (Normal Tabular File):**
1. Map each column index to a semantic name.
2. **Property Extraction**: Identify ALL relevant properties from the file columns. 
   - Exclude the Source ID and Target ID columns from the properties dictionary.
   - Include all descriptive, quantitative, or categorical columns that provide additional context for the entity or relationship (e.g., scores, types, names, genomic positions).
   - Use meaningful property names (e.g., 'score' instead of 'column_24').
"""
        else:
            prompt += """
**Task (Composite/Complex File):**
1. Map each column index to a semantic name.
2. **Property Extraction**: Identify ALL relevant properties from the file columns. 
   - Exclude the Source ID and Target ID columns from the properties dictionary.
   - Include all descriptive, quantitative, or categorical columns that provide additional context for the entity or relationship.
   - Use meaningful property names.
3. **COMPOSITE FIELDS (Single Column)**: Use dot notation (e.g., "col.part") for property parts if needed.
4. **COMPOSITE KEYS (Multiple Columns)**: Use comma notation (e.g., "idx1,idx2,idx3") for identifiers that require multiple columns to be unique (e.g., genomic coordinates).
"""

        prompt += f"""
**CONSTRAINTS:**
- Use ONLY column indices from 0 to {len(headers) - 1}
- Do NOT invent column indices that don't exist
- **Numerical Prohibition**: NEVER use columns containing floats/decimals as IDs for biological entities.
- **No Hallucinated Parts**: If a column is simple, use a plain integer index.

**Output Format — return ONLY valid JSON:**
{{
  "column_definitions": {{
    "0": "semantic_name_for_column_0",
    "1": "semantic_name_for_column_1"
  }},
  "relationship_mappings": {{
    "primary_entity": {{
      "properties": {{
        "property_name": 2
      }},
      "confidence": 0.0-1.0,
      "reasoning": "brief explanation"
    }}
  }}
}}
"""

        prompt += "\nIMPORTANT:\n"
        prompt += "- All keys in column_definitions MUST be strings (e.g., \"0\", \"1\", not 0, 1)\n"
        if is_composite:
            prompt += "- For composite columns, property indices can be strings like \"0.0\", \"0.1\" to indicate parts\n"
        
        prompt += "\nGenerate the mapping now:\"\"\"\n"
        return prompt


    def call_llm_structured(self, prompt: str) -> Dict[str, Any]:
        """Call LLM and parse JSON response."""
        system = "You are a data mapping expert. You analyze data files and create precise column mappings. You always return valid JSON."
        response = self.llm(prompt, system=system)
        
        try:
            mapping = extract_json(response)
            
            if not mapping:
                raise ValueError(f"LLM returned invalid JSON. Response snippet: {response[:200]}")
                
            if "column_definitions" in mapping:
                col_defs = mapping["column_definitions"]
                mapping["column_definitions"] = {int(k) if k.isdigit() else k: v for k, v in col_defs.items()}
            
            # Handle composite column notation in properties
            if "relationship_mappings" in mapping:
                for entity_name, rel_data in mapping["relationship_mappings"].items():
                    if "properties" in rel_data:
                        for prop_name, val in rel_data["properties"].items():
                            if isinstance(val, str):
                                if '.' not in val and val.isdigit():
                                    rel_data["properties"][prop_name] = int(val)
                            elif isinstance(val, (int, float)):
                                rel_data["properties"][prop_name] = int(val)
            
            return mapping
        except Exception as e:
            raise ValueError(f"LLM returned invalid JSON: {e}\nResponse: {response}")




    def output_yaml(self, mapping: Dict[str, Any], output_path: str):
        """Write mappings to YAML file."""
        with open(output_path, 'w') as f:
            yaml.dump(mapping, f, default_flow_style=False, sort_keys=False)


    def generate_mappings(self, relationship_type: str = None, skip_structure_analysis: bool = False, 
                         source_entity_hint: str = None, target_entity_hint: str = None, 
                         source_id_hint: Any = None, target_id_hint: Any = None,
                         adapter_type: str = 'both') -> Dict[str, Any]:
        """Generate column definitions and relationship mappings.

        Args:
            relationship_type: Optional hint about the relationship type
            skip_structure_analysis: If True, skip the structure analysis phase
            source_entity_hint: Optional hint about source entity type (e.g., "gene", "enhancer")
            target_entity_hint: Optional hint about target entity type (e.g., "protein", "disease")
            source_id_hint: Optional index/name of the column used for Source ID
            target_id_hint: Optional index/name of the column used for Target ID

        Returns:
            Mapping dictionary
        """
        structure_analysis = {}
        
        if not skip_structure_analysis:
            print(f"Analyzing column structure (adapter_type={adapter_type})...")
            structure_analysis = self.analyze_column_structure(
                source_entity_hint, target_entity_hint, 
                source_id_hint=source_id_hint, target_id_hint=target_id_hint,
                adapter_type=adapter_type
            )
            
            if structure_analysis:
                print("Structure analysis complete")
                
                # Check inspector validation
                inspector_val = structure_analysis.get('inspector_validation', {})
                if inspector_val:
                    delimiter_ok = inspector_val.get('delimiter_correct', True)
                    header_ok = inspector_val.get('has_header_correct', True)
                    comment_ok = inspector_val.get('comment_lines_correct', True)
                    
                    if not delimiter_ok:
                        print(f"  Delimiter Issue: {inspector_val.get('delimiter_issues', 'Unknown')}")
                        if inspector_val.get('suggested_delimiter'):
                            print(f"    Suggested: {repr(inspector_val.get('suggested_delimiter'))}")
                    
                    if not header_ok:
                        print(f"  Header Detection Issue: {inspector_val.get('has_header_issues', 'Unknown')}")
                        if 'suggested_has_header' in inspector_val:
                            print(f"    Suggested: {inspector_val.get('suggested_has_header')}")
                    
                    if not comment_ok:
                        print(f"  Comment Lines Issue: {inspector_val.get('comment_lines_issues', 'Unknown')}")
                    
                    if delimiter_ok and header_ok and comment_ok:
                        print("  Source inspector output validated")
                
                if structure_analysis.get('parsing_required'):
                    print(" Composite fields detected - parsing will be required")
            else:
                print(" Structure analysis returned no results, proceeding without it")
        
        print("Building prompt...")
        prompt = self.build_prompt(relationship_type, structure_analysis, adapter_type=adapter_type)

        print("Calling LLM...")
        mapping = self.call_llm_structured(prompt)
        
        # Add structure analysis to output for reference
        if structure_analysis:
            mapping['structure_analysis'] = structure_analysis
            struct_reasoning = structure_analysis.get('reasoning_for_type', 'Structure analysis completed successfully.')
            print(f"\n[*] LLM File Structure Analysis:")
            print(f"  - Detected Type: {structure_analysis.get('file_structure_type', 'unknown')}")
            print(f"  - Reasoning: {struct_reasoning}")
            
            # Print normalization hints
            norm_hints = []
            for col_idx, col_data in structure_analysis.get('columns', {}).items():
                if col_data.get('normalization'):
                    norm_hints.append(f"    * Column {col_idx}: {col_data['normalization']}")
                for part in col_data.get('parts', []):
                    if part.get('normalization'):
                        part_type = part.get('type', 'part')
                        norm_hints.append(f"    * Column {col_idx} ({part_type}): {part['normalization']}")
            
            if norm_hints:
                print("  - Normalization Hints:")
                for hint in norm_hints:
                    print(hint)

        print("\n[*] LLM Semantic Mapping Decisions:")
        col_defs = mapping.get('column_definitions', {})
        if col_defs:
            print("  - Column Semantic Names:")
            for idx, name in col_defs.items():
                print(f"    * Column {idx} to {name}")

        rel_mappings = mapping.get('relationship_mappings', {})
        for ent, data in rel_mappings.items():
            conf = data.get('confidence', 1.0)
            reasoning = data.get('reasoning', 'No reasoning provided.')
            status_msg = "PASSED" if conf >= 0.7 else "WARNING"
            print(f"  [{status_msg}] Entity '{ent}' (Confidence: {conf}): {reasoning}")

        print("\nMappings generated successfully")
        return mapping


def main():
    """CLI entry point for LLM column mapper."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate column mappings using LLM (schema-free)"
    )
    parser.add_argument("--metadata", required=True,
                        help="Path to source metadata JSON file")
    parser.add_argument("--output", required=True,
                        help="Output path for generated mapping YAML")
    parser.add_argument("--relationship",
                        help="Optional relationship type hint")

    args = parser.parse_args()

    try:
        with open(args.metadata) as f:
            metadata = json.load(f)

        mapper = LLMColumnMapper(metadata)
        mapping = mapper.generate_mappings(args.relationship)
        mapper.output_yaml(mapping, args.output)
        print(f"Mapping written to {args.output}")
        sys.exit(0)

    except FileNotFoundError as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
