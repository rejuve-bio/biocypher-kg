"""Read-only introspection of the pipeline's YAML configuration."""
from __future__ import annotations

import functools
import sys
from pathlib import Path
from typing import Any, Optional

from backend.core.config import settings


@functools.lru_cache(maxsize=1)
def _load_yaml_with_includes():
    """Import repo-root ``config.yaml_loader.load_yaml_with_includes`` (needs repo root on sys.path)."""
    repo_root = str(settings.repo_root_path)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from config.yaml_loader import load_yaml_with_includes  # type: ignore

    return load_yaml_with_includes


# Mirror of create_knowledge_graph.py::get_writer (keep in sync).
WRITER_TYPES = ["metta", "prolog", "neo4j", "parquet", "networkx", "kgx"]

# Toggleable build flags for the UI; defaults match create_knowledge_graph.py::main.
BUILD_FLAGS = [
    {"name": "write_properties", "default": True,
     "help": "Write properties onto nodes and edges."},
    {"name": "add_provenance", "default": True,
     "help": "Attach dataset provenance (version/source/citation) to nodes and edges."},
    {"name": "include_taxon_id", "default": True,
     "help": "Include the taxon_id property. Turn off for single-species graphs."},
    {"name": "include_curie", "default": False,
     "help": "Keep CURIE namespace prefixes in node/edge IDs."},
    {"name": "skip_preflight", "default": False,
     "help": "Skip the file-path pre-flight check before running adapters."},
    {"name": "generate_data_source_schemas", "default": True,
     "help": "Generate per-source schema YAML files for the adapters in this run."},
]


class ConfigError(Exception):
    """Raised when a requested species/dataset/config cannot be resolved."""


def _resolve(path_str: str) -> Path:
    """Resolve a repo-relative (or absolute) config path against the repo root."""
    p = Path(path_str)
    return p if p.is_absolute() else (settings.repo_root_path / p)


def _is_path_like(value: str) -> bool:
    """True only for explicitly-rooted strings ("/", "./", "../"); mirrors the CLI's path check."""
    return value.startswith(("/", "./", "../"))


def _declared_paths(args: dict) -> list[str]:
    """Extract declared path-like args from an adapter's ``args`` dict."""
    paths: list[str] = []
    for arg_name, value in (args or {}).items():
        if arg_name == "feature_files" and isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and isinstance(item.get("path"), str):
                    paths.append(item["path"])
            continue
        if isinstance(value, list):
            paths.extend(v for v in value if isinstance(v, str) and _is_path_like(v))
            continue
        if isinstance(value, str) and _is_path_like(value):
            paths.append(value)
    return paths


def load_species_config() -> dict:
    """Load and return the parsed ``config/species_config.yaml`` registry."""
    load = _load_yaml_with_includes()
    path = settings.repo_root_path / "config" / "species_config.yaml"
    if not path.exists():
        raise ConfigError(f"species_config.yaml not found at {path}")
    return load(str(path)) or {}


def list_species_and_datasets() -> list[dict]:
    """List every species and its available datasets, with config existence flags."""
    cfg = load_species_config()
    result: list[dict] = []
    for species, datasets in cfg.items():
        entry: dict[str, Any] = {"species": species, "datasets": []}
        for ds_name, ds in (datasets or {}).items():
            adapters_cfg = ds.get("adapters_config", "")
            schema_cfg = ds.get("schema_config", "")
            entry["datasets"].append({
                "name": ds_name,
                "adapters_config": adapters_cfg,
                "schema_config": schema_cfg,
                "dbsnp_cache_root": ds.get("dbsnp_cache_root", ""),
                "dbsnp_variant": ds.get("dbsnp_variant", ""),
                "adapters_config_exists": bool(adapters_cfg) and _resolve(adapters_cfg).exists(),
                "schema_config_exists": bool(schema_cfg) and _resolve(schema_cfg).exists(),
            })
        result.append(entry)
    return result


def _dataset_entry(species: str, dataset: str) -> dict:
    cfg = load_species_config()
    if species not in cfg:
        raise ConfigError(f"Unknown species '{species}'. Known: {sorted(cfg)}")
    datasets = cfg[species] or {}
    if dataset not in datasets:
        raise ConfigError(
            f"Dataset '{dataset}' not available for species '{species}'. "
            f"Available: {sorted(datasets)}"
        )
    return datasets[dataset]


def resolve_adapters_config_path(species: str, dataset: str) -> Path:
    """Absolute path to the adapters config for a species/dataset."""
    entry = _dataset_entry(species, dataset)
    rel = entry.get("adapters_config")
    if not rel:
        raise ConfigError(f"No adapters_config set for {species}/{dataset}")
    path = _resolve(rel)
    if not path.exists():
        raise ConfigError(f"adapters_config not found: {path}")
    return path


def resolve_schema_config_path(species: str, dataset: str) -> Path:
    entry = _dataset_entry(species, dataset)
    rel = entry.get("schema_config")
    if not rel:
        raise ConfigError(f"No schema_config set for {species}/{dataset}")
    path = _resolve(rel)
    if not path.exists():
        raise ConfigError(f"schema_config not found: {path}")
    return path


def load_adapters_dict(species: str, dataset: str) -> tuple[dict, Optional[str]]:
    """Return (adapters_dict, input_dir) with ``input_dir`` popped out (mirrors the CLI)."""
    load = _load_yaml_with_includes()
    path = resolve_adapters_config_path(species, dataset)
    adapters = load(str(path)) or {}
    input_dir = adapters.pop("input_dir", None)
    return adapters, input_dir


def list_adapters(species: str, dataset: str) -> dict:
    """List adapters declared in a species/dataset adapters config."""
    adapters, input_dir = load_adapters_dict(species, dataset)
    out: list[dict] = []
    for name, entry in adapters.items():
        if not isinstance(entry, dict):
            continue
        adapter = entry.get("adapter") or {}
        args = adapter.get("args") or {}
        out.append({
            "name": name,
            "module": adapter.get("module"),
            "cls": adapter.get("cls"),
            "nodes": bool(entry.get("nodes", False)),
            "edges": bool(entry.get("edges", False)),
            "outdir": entry.get("outdir"),
            "source_id": entry.get("source_id"),
            "provenance": entry.get("provenance"),
            "args": args,
            "declared_paths": _declared_paths(args),
        })
    return {
        "species": species,
        "dataset": dataset,
        "input_dir": input_dir,
        "adapters_config": str(resolve_adapters_config_path(species, dataset)),
        "count": len(out),
        "adapters": out,
    }


def list_schema(species: str, dataset: str) -> dict:
    """Shallow view of the BioCypher schema: node/edge type names + per-source schemas."""
    load = _load_yaml_with_includes()
    path = resolve_schema_config_path(species, dataset)
    species_raw = load(str(path)) or {}

    primer_path = settings.repo_root_path / "config" / "primer_schema_config.yaml"
    primer_raw = load(str(primer_path)) if primer_path.exists() else {}

    # Species entries override primer entries of the same name (matches merge_schemas).
    merged = {**primer_raw, **species_raw}

    node_types: list[str] = []
    edge_types: list[str] = []
    for key, val in merged.items():
        if key == "Title" or not isinstance(val, dict):
            continue
        represented = str(val.get("represented_as", "")).lower()
        if represented == "edge":
            edge_types.append(key)
        else:
            node_types.append(key)

    ds_schema_dir = settings.repo_root_path / "data_source_schemas" / species
    data_source_schemas = (
        sorted(p.name for p in ds_schema_dir.glob("*.yaml"))
        if ds_schema_dir.is_dir() else []
    )
    return {
        "species": species,
        "dataset": dataset,
        "schema_config": str(path),
        "node_types": sorted(node_types),
        "edge_types": sorted(edge_types),
        "data_source_schemas": data_source_schemas,
    }


def _config_path(species: str, dataset: str, kind: str) -> Path:
    if kind == "adapters":
        return resolve_adapters_config_path(species, dataset)
    if kind == "schema":
        return resolve_schema_config_path(species, dataset)
    raise ConfigError(f"Unknown config kind '{kind}'. Use 'adapters' or 'schema'.")


def read_config_text(species: str, dataset: str, kind: str) -> dict:
    """Return the raw text of a species/dataset config file for editing."""
    path = _config_path(species, dataset, kind)
    return {"species": species, "dataset": dataset, "kind": kind,
            "path": str(path), "content": path.read_text()}


_BIOLINK_RE = None  # compiled lazily in _validate_schema_doc


def _validate_schema_doc(parsed: dict) -> tuple[list[str], list[str]]:
    """Fast, network-free schema/biolink checks. Returns (errors, warnings).

    Errors block the save (clear mistakes); warnings are surfaced but allowed
    (the authoritative biolink check still runs in BioCypher at build time).
    """
    import re
    global _BIOLINK_RE
    if _BIOLINK_RE is None:
        # Classes are PascalCase, but slots/mixins/predicates are lowercase
        # (e.g. biolink:association, biolink:related_to), so accept either case;
        # only genuinely broken terms (spaces, bad chars) are malformed.
        _BIOLINK_RE = re.compile(r"^biolink:[A-Za-z][A-Za-z0-9_]*$")

    errors: list[str] = []
    warnings: list[str] = []

    # is_a parents may live in this file or in the primer schema it's merged with.
    known_types = {k for k, v in parsed.items() if isinstance(v, dict)}
    primer = settings.repo_root_path / "config" / "primer_schema_config.yaml"
    if primer.exists():
        try:
            pdoc = _load_yaml_with_includes()(str(primer)) or {}
            known_types |= {k for k, v in pdoc.items() if isinstance(v, dict)}
        except Exception:  # noqa: BLE001
            pass
    known_biolink = set(list_biolink_categories())

    def check_term(name, field, v):
        if isinstance(v, str) and v.startswith("biolink:"):
            if not _BIOLINK_RE.match(v):
                errors.append(f"{name}: malformed biolink term in {field}: '{v}'")
            elif v not in known_biolink:
                warnings.append(f"{name}: {field} '{v}' is not used elsewhere in this project — verify it is a valid biolink class")

    for name, val in parsed.items():
        if name == "Title" or not isinstance(val, dict):
            continue
        rep = val.get("represented_as")
        if rep is not None and str(rep) not in ("node", "edge"):
            errors.append(f"{name}: represented_as must be 'node' or 'edge' (got '{rep}')")
        if not val.get("input_label"):
            warnings.append(f"{name}: no input_label set")
        check_term(name, "biolink_category", val.get("biolink_category"))
        mixins = val.get("mixins")
        if isinstance(mixins, list):
            for m in mixins:
                check_term(name, "mixins", m)
        elif mixins is not None:
            warnings.append(f"{name}: mixins should be a list")
        parents = val.get("is_a")
        plist = parents if isinstance(parents, list) else ([parents] if parents else [])
        for p in plist:
            if isinstance(p, str) and not p.startswith("biolink:") and p not in known_types:
                warnings.append(f"{name}: parent '{p}' (is_a) not found among schema types")
        props = val.get("properties")
        if props is not None and not isinstance(props, dict):
            errors.append(f"{name}: properties must be a mapping")
    return errors, warnings


def save_config_text(species: str, dataset: str, kind: str, content: str) -> dict:
    """Validate (via the project YAML loader + schema checks) and atomically write.

    Validation happens on a temp file in the same directory so relative ``!include``s
    resolve. Schema configs also get biolink/structure validation: errors block the
    save, warnings are returned. History/revert is git's job (config/ is tracked).
    """
    load = _load_yaml_with_includes()
    path = _config_path(species, dataset, kind)
    tmp = path.with_name(path.name + ".console-tmp")
    tmp.write_text(content)
    try:
        parsed = load(str(tmp))
    except Exception as exc:  # noqa: BLE001 - surface any loader error as a 400
        tmp.unlink(missing_ok=True)
        raise ConfigError(f"Config does not parse: {exc}")
    if not isinstance(parsed, dict):
        tmp.unlink(missing_ok=True)
        raise ConfigError("Top-level config must be a YAML mapping.")

    warnings: list[str] = []
    if kind == "schema":
        errors, warnings = _validate_schema_doc(parsed)
        if errors:
            tmp.unlink(missing_ok=True)
            raise ConfigError("Schema validation failed:\n- " + "\n- ".join(errors))

    tmp.replace(path)
    return {"species": species, "dataset": dataset, "kind": kind,
            "path": str(path), "content": content, "warnings": warnings}


# ===== Guided (structured) config editing =====
# A config is a mapping of named entries. The guided editor reads/writes one entry
# at a time. Saves splice only the edited entry's text block back into the raw
# file, so untouched entries stay byte-for-byte identical (see save_config_entry).

def _load_config_dict(path: Path) -> dict:
    """Load a config to a plain dict via the project loader (handles !include and,
    like the build loader, keeps the last of any duplicate keys)."""
    load = _load_yaml_with_includes()
    return load(str(path)) or {}


def _to_plain(obj):
    """Deep-convert to plain JSON-serializable Python (defensive; the loader
    already returns plain types)."""
    if isinstance(obj, dict):
        return {str(k): _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_plain(v) for v in obj]
    if isinstance(obj, bool):
        return bool(obj)
    if isinstance(obj, int):
        return int(obj)
    if isinstance(obj, float):
        return float(obj)
    if obj is None:
        return None
    return str(obj)


def _entry_summary(kind: str, name: str, val: dict) -> dict:
    if kind == "adapters":
        adapter = val.get("adapter") or {}
        return {
            "name": name,
            "nodes": bool(val.get("nodes", False)),
            "edges": bool(val.get("edges", False)),
            "module": adapter.get("module"),
            "cls": adapter.get("cls"),
        }
    # schema
    return {
        "name": name,
        "represented_as": str(val.get("represented_as", "node")),
        "input_label": val.get("input_label"),
        "biolink_category": val.get("biolink_category"),
        "is_a": val.get("is_a"),  # str or list; drives the hierarchy tree
    }


def list_config_entries(species: str, dataset: str, kind: str) -> dict:
    """Ordered list of the config's named entries + a light summary of each."""
    path = _config_path(species, dataset, kind)
    doc = _load_config_dict(path)
    entries = [
        _entry_summary(kind, str(name), val)
        for name, val in doc.items()
        if isinstance(val, dict)  # skip scalar keys like Title / input_dir
    ]
    return {"species": species, "dataset": dataset, "kind": kind,
            "path": str(path), "count": len(entries), "entries": entries}


def get_config_entry(species: str, dataset: str, kind: str, name: str) -> dict:
    """Full structured data for a single entry."""
    path = _config_path(species, dataset, kind)
    doc = _load_config_dict(path)
    if name not in doc:
        raise ConfigError(f"No entry '{name}' in the {kind} config.")
    return {"name": name, "data": _to_plain(doc[name])}


def _top_level_key(line: str):
    """The mapping key on a column-0 line, else None (skips indented lines,
    blanks, comments, and list items)."""
    if not line or line[0].isspace():
        return None
    s = line.lstrip()
    if s.startswith("#") or s.startswith("-") or ":" not in line:
        return None
    key = line.split(":", 1)[0].strip()
    if len(key) >= 2 and key[0] in "\"'" and key[-1] == key[0]:
        key = key[1:-1]
    return key or None


def _entry_span(lines: list[str], name: str):
    """[start, end) line indices of the top-level entry ``name`` (the last
    occurrence, matching the build loader's last-wins), or None."""
    keys = [(i, k) for i, ln in enumerate(lines) if (k := _top_level_key(ln)) is not None]
    match = None
    for pos, (_i, k) in enumerate(keys):
        if k == name:
            match = pos
    if match is None:
        return None
    start = keys[match][0]
    end = keys[match + 1][0] if match + 1 < len(keys) else len(lines)
    return start, end


def _dump_entry(name: str, data: dict) -> str:
    import yaml
    return yaml.safe_dump(
        {name: data},
        default_flow_style=False,
        sort_keys=False,  # preserve the field order the form sent
        allow_unicode=True,
        width=4096,  # don't wrap long scalars (e.g. urls)
        indent=2,
    )


def save_config_entry(species: str, dataset: str, kind: str, name: str, data: dict) -> dict:
    """Add or replace one entry by splicing just its block into the raw file, so
    every other entry stays byte-for-byte identical, then validate + atomic write."""
    if not isinstance(data, dict):
        raise ConfigError("Entry data must be a mapping.")
    path = _config_path(species, dataset, kind)
    lines = path.read_text().splitlines(keepends=True)
    new_lines = _dump_entry(name, data).splitlines(keepends=True)
    span = _entry_span(lines, name)
    if span is None:
        # New entry: append, ensuring a separating newline.
        sep = "" if (not lines or lines[-1].endswith("\n")) else "\n"
        result = "".join(lines) + sep + "".join(new_lines)
    else:
        start, end = span
        # Keep the blank lines that separated this entry from the next.
        trailing, j = [], end - 1
        while j >= start and lines[j].strip() == "":
            trailing.insert(0, lines[j])
            j -= 1
        result = "".join(lines[:start] + new_lines + trailing + lines[end:])
    saved = save_config_text(species, dataset, kind, result)  # validates + atomic write
    out = get_config_entry(species, dataset, kind, name)
    # Only surface warnings about the entry the user just edited.
    out["warnings"] = [w for w in saved.get("warnings", []) if w.startswith(f"{name}:")]
    return out


def delete_config_entry(species: str, dataset: str, kind: str, name: str) -> dict:
    """Remove one entry's block from the raw file, then validate + atomic write."""
    path = _config_path(species, dataset, kind)
    lines = path.read_text().splitlines(keepends=True)
    span = _entry_span(lines, name)
    if span is None:
        raise ConfigError(f"No entry '{name}' in the {kind} config.")
    start, end = span
    save_config_text(species, dataset, kind, "".join(lines[:start] + lines[end:]))
    return {"deleted": name}


def list_biolink_categories() -> list[str]:
    """Distinct ``biolink:...`` identifiers used across the schema configs, for
    the guided schema editor's type pickers (the project's own vocabulary)."""
    import re
    paths = [settings.repo_root_path / "config" / "primer_schema_config.yaml"]
    for entry in list_species_and_datasets():
        for ds in entry.get("datasets", []):
            sc = ds.get("schema_config")
            if sc:
                p = _resolve(sc)
                if p.exists():
                    paths.append(p)
    cats: set[str] = set()
    for p in paths:
        try:
            cats.update(re.findall(r"biolink:[A-Za-z0-9_]+", p.read_text()))
        except Exception:  # noqa: BLE001
            continue
    return sorted(cats)


def list_writers() -> list[str]:
    return list(WRITER_TYPES)


def list_flags() -> list[dict]:
    return [dict(f) for f in BUILD_FLAGS]
