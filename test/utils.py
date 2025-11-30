import yaml
from pathlib import Path
from typing import List, Dict, Any, Tuple


def load_expected_records(adapter_name: str) -> List[Dict[str, Any]]:
    """Load expected-record fixture for a given adapter.

    Returns an empty list when no fixture exists or when `expected_records` is empty.
    """
    p = Path(__file__).parent / "fixtures" / "expected_records" / f"{adapter_name}.yaml"
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text()) or {}
    return data.get("expected_records", []) or []


def normalize_id(idv) -> Tuple:
    """Normalize node/edge ids to a tuple form (type, id) when possible."""
    if isinstance(idv, (list, tuple)) and len(idv) >= 2:
        return (str(idv[0]).lower(), str(idv[1]))
    s = str(idv)
    if ':' in s:
        parts = s.split(':', 1)
        return (parts[0].lower(), parts[1])
    return (None, s)


PROPERTY_ALIASES = {
    'symbol': ['gene_name', 'approved_symbol', 'symbol', 'name'],
    'pvalue': ['pvalue', 'p_value', 'pval'],
    'slope': ['slope', 'beta'],
}


def match_properties(expected_props: Dict[str, Any], actual_props: Dict[str, Any]) -> bool:
    if not expected_props:
        return True
    if not isinstance(actual_props, dict):
        return False
    actual_keys_lower = {str(k).lower(): k for k in actual_props.keys()}
    for k, v in expected_props.items():
        k_lower = str(k).lower()
        matched_key = None
        if k_lower in actual_keys_lower:
            matched_key = actual_keys_lower[k_lower]
        else:

            aliases = PROPERTY_ALIASES.get(k_lower, [])
            for a in aliases:
                if a.lower() in actual_keys_lower:
                    matched_key = actual_keys_lower[a.lower()]
                    break
        if matched_key is None:
            return False
        if str(actual_props[matched_key]) != str(v):
            return False
    return True


def record_in_nodes(expected: Dict[str, Any], actual_nodes: List[Tuple]) -> bool:
    """Check whether an expected node record is present in actual nodes.

    `actual_nodes` expected format: (node_id, node_label, node_props)
    """
    exp_id = expected.get("id")
    exp_label = expected.get("label")
    exp_props = expected.get("properties", {})

    for node in actual_nodes:
        try:
            node_id, node_label, node_props = node
        except Exception:
            continue
        nid = normalize_id(node_id)
        if exp_id:
            if isinstance(exp_id, (list, tuple)) and len(exp_id) >= 2:
                exp_local = str(exp_id[1])
                if exp_local not in nid[1]:
                    continue
            else:
                exp_local = str(exp_id)
                if exp_local not in nid[1]:
                    continue
        if exp_label:
            node_label_lower = str(node_label).lower()
            exp_label_lower = str(exp_label).lower()
            if exp_label_lower not in node_label_lower and node_label_lower not in exp_label_lower:
                continue
        if not match_properties(exp_props, node_props):
            continue
        return True
    return False


def record_in_edges(expected: Dict[str, Any], actual_edges: List[Tuple]) -> bool:
    """Check whether an expected edge record is present in actual edges.

    `actual_edges` expected format: (source_id, target_id, edge_label, edge_props)
    """
    exp_source = expected.get("source")
    exp_target = expected.get("target")
    exp_label = expected.get("label")
    exp_props = expected.get("properties", {})

    for edge in actual_edges:
        try:
            src, tgt, lbl, props = edge
        except Exception:
            continue
        src_n = normalize_id(src)
        tgt_n = normalize_id(tgt)

        # normalize label and properties for matching
        lbl_lower = str(lbl).lower()
        props_lower = {k.lower(): v for k, v in (props or {}).items()}

        label_match = False
        if exp_label:
            if lbl_lower == str(exp_label).lower():
                label_match = True
            else:
                for key in ('interaction_type', 'type', 'relationship'):
                    if props_lower.get(key) and str(props_lower.get(key)).lower() == str(exp_label).lower():
                        label_match = True
                        break
        else:
            label_match = True

        if not label_match:
            continue

        source_matches = True
        if exp_source:
            if isinstance(exp_source, (list, tuple)) and len(exp_source) >= 2:
                source_matches = (src_n[1] == str(exp_source[1]))
            else:
                source_matches = (src_n[1] == str(exp_source))

        target_matches = True
        if exp_target:
            if isinstance(exp_target, (list, tuple)) and len(exp_target) >= 2:
                target_matches = (tgt_n[1] == str(exp_target[1]))
            else:
                target_matches = (tgt_n[1] == str(exp_target))

        props_match = match_properties(exp_props, props or {})

        if source_matches and target_matches and props_match:
            return True

        if label_match and props_match:
            return True
   
        continue
    return False
