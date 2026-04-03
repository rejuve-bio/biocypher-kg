#!/usr/bin/env python3
"""
Detect if any heavy ontology adapter was affected by changes to the adapters config.

Instead of grepping diff text (which misses changes to nested fields like filepaths),
this script maps changed line numbers back to their top-level YAML key by scanning
upward for the nearest unindented key. That key is the adapter name.

Usage:
    python detect_heavy_adapter_changes.py <base_sha> <head_sha>

Prints:
    HEAVY_CHANGE:<adapter_name>  — if a heavy adapter block was modified
    NO_HEAVY_CHANGE              — otherwise

Must stay in sync with SMOKE_SKIP_MODULE_PATTERNS in test/test.py.
"""
import sys
import subprocess
import re

HEAVY_PATTERNS = [
    "ontologies_adapter",
    "gene_ontology_adapter",
    "uberon_adapter",
    "cell_ontology_adapter",
    "cell_line_ontology_adapter",
    "experimental_factor_ontology_adapter",
    "brenda_tissue_ontology_adapter",
    "human_phenotype_ontology_adapter",
    "chebi_ontology_adapter",
    "disease_ontology_adapter",
]

CONFIG_FILE = "config/hsa/hsa_adapters_config_sample.yaml"


def is_heavy_adapter(name):
    return any(p in name for p in HEAVY_PATTERNS)


def find_top_level_key(lines, line_num_1indexed):
    """Scan backward from line_num to find the nearest unindented YAML key."""
    for i in range(line_num_1indexed - 1, -1, -1):
        line = lines[i]
        if line and not line[0].isspace() and ":" in line and not line.startswith("#"):
            return line.split(":")[0].strip()
    return None


def get_file_lines(sha, path):
    """Get file content at a specific git commit. Returns [] if not found."""
    try:
        content = subprocess.check_output(
            ["git", "show", f"{sha}:{path}"], text=True, stderr=subprocess.DEVNULL
        )
        return content.splitlines()
    except subprocess.CalledProcessError:
        return []


def parse_diff_hunks(diff_text):
    """
    Parse @@ hunk headers from git diff --unified=0 output.
    Returns (old_ranges, new_ranges) where each is a list of (start, count) tuples.
    """
    old_ranges, new_ranges = [], []
    for line in diff_text.splitlines():
        m = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
        if m:
            old_start = int(m.group(1))
            old_count = int(m.group(2)) if m.group(2) is not None else 1
            new_start = int(m.group(3))
            new_count = int(m.group(4)) if m.group(4) is not None else 1
            if old_count > 0:
                old_ranges.append((old_start, old_count))
            if new_count > 0:
                new_ranges.append((new_start, new_count))
    return old_ranges, new_ranges


def check_ranges(lines, ranges):
    """Return the first heavy adapter key found in the given line ranges, or None."""
    for start, count in ranges:
        for line_num in range(start, start + count):
            key = find_top_level_key(lines, line_num)
            if key and is_heavy_adapter(key):
                return key
    return None


def main():
    if len(sys.argv) != 3:
        print("Usage: detect_heavy_adapter_changes.py <base_sha> <head_sha>", file=sys.stderr)
        sys.exit(1)

    base_sha, head_sha = sys.argv[1], sys.argv[2]

    try:
        diff_text = subprocess.check_output(
            ["git", "diff", "--unified=0", base_sha, head_sha, "--", CONFIG_FILE],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        # Cannot diff — be conservative and treat as a heavy change
        print("HEAVY_CHANGE:unknown (git diff failed — conservative fallback)")
        return

    if not diff_text.strip():
        print("NO_HEAVY_CHANGE")
        return

    old_ranges, new_ranges = parse_diff_hunks(diff_text)

    # Check added/modified lines using the new file content
    new_lines = get_file_lines(head_sha, CONFIG_FILE)
    if new_lines:
        hit = check_ranges(new_lines, new_ranges)
        if hit:
            print(f"HEAVY_CHANGE:{hit}")
            return

    # Check removed lines using the old file content
    old_lines = get_file_lines(base_sha, CONFIG_FILE)
    if old_lines:
        hit = check_ranges(old_lines, old_ranges)
        if hit:
            print(f"HEAVY_CHANGE:{hit}")
            return

    print("NO_HEAVY_CHANGE")


if __name__ == "__main__":
    main()
