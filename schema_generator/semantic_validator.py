"""Semantic hallucination validator for generated BioCypher adapters.

Catches hallucinations that pass syntax checks but are biologically wrong:
- Wrong column used for source_id / target_id / node_id
- Structural mismatches between spec intent and generated logic

Two-layer approach:
    1. Column index static check  — does the code reference the columns the spec declared?
    2. LLM semantic review        — holistic review of spec intent vs generated logic
"""

import re
import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from schema_generator.llm_client import make_llm_client, find_project_root
from schema_generator.code_fixer import extract_json, extract_code, validate_syntax


def _extract_spec_id_info(spec: dict) -> List[Dict[str, Any]]:
    """Extract per-relationship column/type info from spec."""
    results = []
    for rel in spec.get("relationships", []):
        results.append({
            "name": rel.get("name", "unknown"),
            "source_type": str(rel.get("source", "")).lower(),
            "source_col": rel.get("source_column"),
            "target_type": str(rel.get("target", "")).lower() if rel.get("target") else None,
            "target_col": rel.get("target_column"),
        })
    return results


# ---------------------------------------------------------------------------
# Layer 1 — Static column-reference check
# ---------------------------------------------------------------------------

def _check_column_references(code: str, spec_ids: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    """
    Check that the code references the column indices declared in the spec.
    Returns (passed_messages, warning_messages).
    """
    passed, warnings = [], []
    for info in spec_ids:
        name = info["name"]
        for role, col in [("source", info["source_col"]), ("target", info.get("target_col"))]:
            if col is None:
                continue
            try:
                idx = int(col)
            except (ValueError, TypeError):
                continue  # composite or filename-based — skip static check
            pattern = rf"row\[{idx}\]"
            if re.search(pattern, code):
                passed.append(f"[{name}] Code references expected {role} column index [{idx}]")
            else:
                warnings.append(
                    f"[{name}] Code may not reference expected {role} column [{idx}] — "
                    f"verify that the correct column is used for {role}_id"
                )
    return passed, warnings


# ---------------------------------------------------------------------------
# Layer 2 — LLM semantic review (+ repair in same call)
# ---------------------------------------------------------------------------

def _build_spec_context(spec: dict) -> str:
    """Original specification context block for combined review+repair prompt."""
    purpose = spec.get("analysis", {}).get("purpose", "N/A")
    rel_summary = [
        {
            "name": r.get("name"),
            "source": r.get("source"),
            "source_column": r.get("source_column"),
            "target": r.get("target"),
            "target_column": r.get("target_column"),
        }
        for r in spec.get("relationships", [])
    ]
    steps = []
    for rel in spec.get("relationships", []):
        rel_steps = rel.get("implementation", {}).get("steps", [])
        steps.extend(rel_steps)
    if not steps:
        steps = spec.get("implementation_steps", [])

    spec_context = f"""
## Original Specification Context
Purpose: {purpose}
Relationships: {json.dumps(rel_summary, indent=2)}
Implementation Steps:
"""
    for step in steps:
        spec_context += f"- {step}\n"
    return spec_context


def _build_error_summary(static_warnings: List[str], static_errors: List[str]) -> str:
    """Semantic errors from static layers for the combined review+repair prompt."""
    lines = [f"- {e}" for e in static_errors] + [f"- {w}" for w in static_warnings]
    return "\n".join(lines) if lines else "(none identified by static checks)"


def _llm_semantic_review(
    code: str,
    spec: dict,
    inspection: dict,
    llm_fn,
    static_warnings: Optional[List[str]] = None,
    static_errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Ask a lightweight LLM to review whether the generated adapter correctly
    implements the spec's intent, focusing on column usage and ID extraction.
    """
    purpose = spec.get("analysis", {}).get("purpose", "N/A")
    rel_summary = [
        {
            "name": r.get("name"),
            "source": r.get("source"),
            "source_column": r.get("source_column"),
            "target": r.get("target"),
            "target_column": r.get("target_column"),
            "properties": list((r.get("properties") or {}).keys()),
        }
        for r in spec.get("relationships", [])
    ]

    sample_rows = []
    expected_delimiter = spec.get("data_format", {}).get("delimiter", "\\t")
    if inspection.get("main_file"):
        sample_rows = inspection["main_file"].get("metadata", {}).get("sample_rows", [])[:3]

    # Send only the first 100 lines of code to keep the prompt small
    code_preview = "\n".join(code.splitlines()[:100])
    revalidation_instructions = ""
    composite_instructions = ""

    spec_context = _build_spec_context(spec)
    error_summary = _build_error_summary(static_warnings or [], static_errors or [])
    has_static_issues = bool(static_warnings or static_errors)
    intro = (
        "You are an expert BioCypher adapter developer. The following adapter code has "
        "semantic issues identified by our validator (see below). Review it and repair if needed.\n\n"
        if has_static_issues
        else "You are reviewing a generated BioCypher adapter for semantic correctness.\n\n"
    )

    prompt = f"""{intro}## Spec Intent
Purpose: {purpose}

## Expected Relationships / Entities
{json.dumps(rel_summary, indent=2)}

## Data Format (from specification)
- Expected Delimiter: {repr(expected_delimiter)}
- CRITICAL: The code MUST use this delimiter. Do NOT suggest changing it based on sample data appearance.

## Sample Data Rows (from real file)
{json.dumps(sample_rows, indent=2)}{revalidation_instructions}{composite_instructions}

{spec_context}
## Semantic Errors Identified:
{error_summary}

## Generated Code (first 100 lines)
```python
{code_preview}
```

## Broken Code:
```python
{code}
```

## Review Checklist
1. Does the code extract source_id / node_id / target_id from the correct columns declared in the spec?
2. Do the sample data values at those columns look like valid biological identifiers for the declared entity type?

Return ONLY valid JSON — no markdown, no explanation outside the JSON:
{{
  "verdict": "pass" | "warning" | "fail",
  "column_mapping_correct": true | false,
  "id_extraction_correct": true | false,
  "issues": ["list any specific semantic issues found; empty list if none"],
  "summary": "one sentence"
}}

If verdict is "warning" or "fail", after the JSON object apply these repair instructions and output the fixed code:

## Instructions:
1. Analyze the semantic errors.
2. Fix the Python code so it correctly implements the expected extraction logic (e.g. use the correct column indices, extract correct IDs).
3. CRITICAL: Make sure you DO NOT accidentally remove other logic from the code that satisfies the original specification (such as stripping version numbers, adding properties, etc). Use the Original Specification Context above to ensure all requirements are met.
4. Return ONLY the complete fixed Python code inside a ```python block."""

    system = (
        "You are a bioinformatics code reviewer and BioCypher adapter developer. "
        "Be concise and precise. Return valid JSON first; if verdict is warning or fail, "
        "then return the complete fixed Python code in a ```python block."
    )
    try:
        response = llm_fn(prompt, system=system)
        result = extract_json(response)
        if not result:
            return {
                "verdict": "unknown", "issues": [], "summary": "LLM review returned no parseable JSON"
            }
        verdict = result.get("verdict", "unknown")
        if verdict in ("warning", "fail"):
            fixed_code = extract_code(response)
            if fixed_code:
                is_valid, _ = validate_syntax(fixed_code)
                if is_valid:
                    result["fixed_code"] = fixed_code
        return result
    except Exception as e:
        return {"verdict": "unknown", "issues": [str(e)], "summary": f"LLM review error: {e}"}


def apply_semantic_fix_from_report(semantic_report: dict) -> Optional[str]:
    """
    Return fixed adapter code from the combined review+repair LLM call, if present and valid.
    """
    fixed_code = (semantic_report.get("llm_review") or {}).get("fixed_code")
    if not fixed_code:
        return None
    is_valid, _ = validate_syntax(fixed_code)
    return fixed_code if is_valid else None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_semantic_correctness(
    code: str,
    spec: dict,
    inspection: dict,
    llm_fn=None,
    skip_llm: bool = False,
) -> Dict[str, Any]:
    """
    Run all three semantic validation layers and return a combined report.

    Args:
        code:       Generated adapter Python source
        spec:       Parsed adapter specification (YAML → dict)
        inspection: Output of inspect_adapter_files() — contains sample_rows
        llm_fn:     Optional LLM callable. If None, uses the lightweight fixer model.

    Returns:
        {
            "passed":          [str, ...],
            "warnings":        [str, ...],
            "errors":          [str, ...],
            "llm_review":      {verdict, issues, summary, ...},
            "overall_verdict": "pass" | "warning" | "fail"
        }
    """
    results: Dict[str, Any] = {
        "passed": [],
        "warnings": [],
        "errors": [],
        "llm_review": {},
        "overall_verdict": "pass",
    }

    spec_ids = _extract_spec_id_info(spec)

    # --- Layer 1: static column reference check ---
    p, w = _check_column_references(code, spec_ids)
    results["passed"].extend(p)
    results["warnings"].extend(w)

    # --- Layer 2: LLM semantic review ---
    if skip_llm:
        results["warnings"].append("LLM semantic review skipped (skip_llm=True)")
    else:
        if llm_fn is None:
            llm_fn = make_llm_client(
                find_project_root(Path(__file__))
            )

        print("[*] Running LLM semantic review...")
        llm_result = _llm_semantic_review(
            code,
            spec,
            inspection,
            llm_fn,
            static_warnings=list(results["warnings"]),
            static_errors=list(results["errors"]),
        )
        results["llm_review"] = llm_result

        verdict = llm_result.get("verdict", "unknown")
        issues = llm_result.get("issues") or []
        summary = llm_result.get("summary", "")

        if verdict == "pass":
            results["passed"].append(f"LLM semantic review PASSED — {summary}")
        elif verdict == "warning":
            results["warnings"].extend([f"LLM semantic issue: {i}" for i in issues])
            results["warnings"].append(f"LLM review: {summary}")
        elif verdict == "fail":
            results["errors"].extend([f"LLM semantic error: {i}" for i in issues])
            results["errors"].append(f"LLM review: {summary}")
        else:
            results["warnings"].append(f"LLM semantic review inconclusive — {summary}")

    # --- Final verdict ---
    if results["errors"]:
        results["overall_verdict"] = "fail"
    elif results["warnings"]:
        results["overall_verdict"] = "warning"

    return results


def print_semantic_report(report: Dict[str, Any]) -> None:
    """Pretty-print the semantic validation report."""
    verdict = report["overall_verdict"]
    symbol = {"pass": "[+]", "warning": "[~]", "fail": "[-]"}.get(verdict, "[?]")
    print(f"\n{symbol} Semantic Validation: {verdict.upper()}")

    for msg in report["passed"]:
        print(f"    ✓ {msg}")
    for msg in report["warnings"]:
        print(f"    ⚠ {msg}")
    for msg in report["errors"]:
        print(f"    ✗ {msg}")

    llm = report.get("llm_review", {})
    if llm:
        col_ok = llm.get("column_mapping_correct")
        id_ok = llm.get("id_extraction_correct")
        if col_ok is not None:
            print(f"    {'✓' if col_ok else '✗'} Column mapping correct: {col_ok}")
        if id_ok is not None:
            print(f"    {'✓' if id_ok else '✗'} ID extraction correct: {id_ok}")
    print()
