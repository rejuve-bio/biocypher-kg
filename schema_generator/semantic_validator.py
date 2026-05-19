"""Semantic hallucination validator for generated BioCypher adapters.

Catches hallucinations that pass syntax checks but are biologically wrong:
- Wrong column used for source_id / target_id / node_id
- ID values that don't match the expected biological identifier format
- Structural mismatches between spec intent and generated logic

Three-layer approach:
  1. Column index static check  — does the code reference the columns the spec declared?
  2. ID pattern check           — do sample-row values look like the right biological IDs?
  3. LLM semantic review        — holistic review of spec intent vs generated logic
"""

import re
import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from schema_generator.llm_client import make_llm_client, find_project_root
from schema_generator.code_fixer import extract_json


# Known biological identifier regex patterns keyed by entity type keyword
_ID_PATTERNS: Dict[str, List[str]] = {
    "gene":        [r"^ENSG\d+", r"^\d{4,}$", r"^hsa:\d+", r"^[A-Z][A-Z0-9]{1,}$"],
    "transcript":  [r"^ENST\d+", r"^ENSR\d+"],
    "exon":        [r"^ENSE\d+"],
    "protein":     [r"^[A-Z][0-9][A-Z0-9]{3}[0-9]$", r"^ENSP\d+"],
    "snp":         [r"^rs\d+"],
    "variant":     [r"^rs\d+", r"^chr[\dXYxy]+[_:]\d+"],
    "phenotype":   [r"^HP[_:]\d{5,}", r"^MP[_:]\d+"],
    "disease":     [r"^OMIM[_:]\d+", r"^MONDO[_:]\d+", r"^DOID[_:]\d+", r"^OMIM:\d+"],
    "pathway":     [r"^R-[A-Z]{2,4}-\d+", r"^hsa\d+", r"^WP\d+"],
    "go":          [r"^GO[_:]\d{7}"],
    "mirna":       [r"^hsa-mi[rR]-", r"^MI\d{7}"],
    "anatomy":     [r"^UBERON[_:]\d+", r"^CL[_:]\d+", r"^BTO[_:]\d+"],
    "rna":         [r"^URS[0-9A-F]+", r"^ENSR\d+", r"^RF\d+"],
    "enhancer":    [r"^chr[\dXY]+[_:]\d+[_:]\d+", r"^EH38E\d+"],
    "motif":       [r"^[A-Z0-9]+\.\d+$"],
}


def _match_patterns(value: str, entity_type: str) -> bool:
    """Return True if value matches any known pattern for the entity type."""
    entity_lower = entity_type.lower()
    for keyword, patterns in _ID_PATTERNS.items():
        if keyword in entity_lower or entity_lower in keyword:
            for pat in patterns:
                if re.match(pat, value, re.IGNORECASE):
                    return True
    return False


def _has_known_pattern(entity_type: str) -> bool:
    entity_lower = entity_type.lower()
    return any(k in entity_lower or entity_lower in k for k in _ID_PATTERNS)


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


def _sample_values(rows: List[List[str]], col) -> List[str]:
    """Extract non-empty values from rows at col index. col may be int or str."""
    if col is None:
        return []
    try:
        idx = int(col)
        return [r[idx].strip() for r in rows if len(r) > idx and r[idx].strip()]
    except (ValueError, TypeError):
        return []


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
# Layer 2 — Biological ID pattern check
# ---------------------------------------------------------------------------

def _check_id_patterns(
    rows: List[List[str]], spec_ids: List[Dict[str, Any]]
) -> Tuple[List[str], List[str]]:
    """
    For each relationship, extract sample values at the declared column and
    check them against known biological ID patterns.
    Returns (passed_messages, warning_messages).
    """
    passed, warnings = [], []
    for info in spec_ids:
        name = info["name"]
        for role, col, etype in [
            ("source", info["source_col"], info["source_type"]),
            ("target", info.get("target_col"), info.get("target_type") or ""),
        ]:
            if col is None or not etype:
                continue
            values = _sample_values(rows, col)
            if not values:
                warnings.append(
                    f"[{name}] Could not extract {role} IDs from column {col} in sample rows"
                )
                continue

            if not _has_known_pattern(etype):
                passed.append(
                    f"[{name}] No ID pattern rule for entity type '{etype}' — "
                    f"sample values look like: {values[:2]}"
                )
                continue

            matched = sum(1 for v in values[:5] if _match_patterns(v, etype))
            total = min(5, len(values))
            if matched > 0:
                passed.append(
                    f"[{name}] {role.capitalize()} ID pattern OK ({matched}/{total} "
                    f"sample values match '{etype}' format)"
                )
            else:
                warnings.append(
                    f"[{name}] {role.capitalize()} IDs {values[:3]} do not match "
                    f"expected '{etype}' identifier format — check column {col}"
                )
    return passed, warnings


# ---------------------------------------------------------------------------
# Layer 3 — LLM semantic review
# ---------------------------------------------------------------------------

def _llm_semantic_review(
    code: str, spec: dict, inspection: dict, llm_fn
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
    if inspection.get("main_file"):
        sample_rows = inspection["main_file"].get("sample_rows", [])[:3]

    # Send only the first 100 lines of code to keep the prompt small
    code_preview = "\n".join(code.splitlines()[:100])

    prompt = f"""You are reviewing a generated BioCypher adapter for semantic correctness.

## Spec Intent
Purpose: {purpose}

## Expected Relationships / Entities
{json.dumps(rel_summary, indent=2)}

## Sample Data Rows (from real file)
{json.dumps(sample_rows, indent=2)}

## Generated Code (first 100 lines)
```python
{code_preview}
```

## Review Checklist
1. Does the code extract source_id / node_id / target_id from the correct columns declared in the spec?
2. Do the sample data values at those columns look like valid biological identifiers for the declared entity type?
3. Is there any obvious mismatch between what the spec asks for and what the code does (wrong column, wrong label, wrong delimiter)?

Return ONLY valid JSON — no markdown, no explanation outside the JSON:
{{
  "verdict": "pass" | "warning" | "fail",
  "column_mapping_correct": true | false,
  "id_extraction_correct": true | false,
  "issues": ["list any specific semantic issues found; empty list if none"],
  "summary": "one sentence"
}}"""

    system = (
        "You are a bioinformatics code reviewer. "
        "Be concise and precise. Return only valid JSON."
    )
    try:
        response = llm_fn(prompt, system=system)
        result = extract_json(response)
        return result if result else {
            "verdict": "unknown", "issues": [], "summary": "LLM review returned no parseable JSON"
        }
    except Exception as e:
        return {"verdict": "unknown", "issues": [str(e)], "summary": f"LLM review error: {e}"}


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
    sample_rows: List[List[str]] = []
    if inspection.get("main_file"):
        sample_rows = inspection["main_file"].get("sample_rows", [])

    # --- Layer 1: static column reference check ---
    p, w = _check_column_references(code, spec_ids)
    results["passed"].extend(p)
    results["warnings"].extend(w)

    # --- Layer 2: biological ID pattern check ---
    if sample_rows:
        p, w = _check_id_patterns(sample_rows, spec_ids)
        results["passed"].extend(p)
        results["warnings"].extend(w)
    else:
        results["warnings"].append(
            "No sample rows available in inspection data — skipping ID pattern validation"
        )

    # --- Layer 3: LLM semantic review ---
    if skip_llm:
        results["warnings"].append("LLM semantic review skipped (skip_llm=True)")
    else:
        if llm_fn is None:
            llm_fn = make_llm_client(
                find_project_root(Path(__file__)), model_override="llama-3.1-8b-instant"
            )

        print("[*] Running LLM semantic review...")
        llm_result = _llm_semantic_review(code, spec, inspection, llm_fn)
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
