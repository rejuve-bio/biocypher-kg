"""Read-only introspection of the pipeline's YAML configuration.

Everything here is cheap and pure: it parses committed config files and returns
plain dicts. It never instantiates BioCypher or a writer, and never runs a build.

Paths are always resolved against ``settings.repo_root_path`` and opened by
absolute path so the ``!include`` directive (resolved relative to the including
file's own directory) keeps working.
"""
from __future__ import annotations

import functools
import sys
from pathlib import Path
from typing import Any, Optional

from backend.core.config import settings


@functools.lru_cache(maxsize=1)
def _load_yaml_with_includes():
    """Import the repo-root ``config.yaml_loader.load_yaml_with_includes``.

    The build CLI lives at the repo root, not under ``kg-service``, so we put the
    repo root on ``sys.path`` to reuse its pure YAML loader (which understands the
    custom ``!include`` directive). Safe because this app only ever imports config
    via the fully-qualified ``backend.core.config`` — never bare ``import config``.
    """
    repo_root = str(settings.repo_root_path)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from config.yaml_loader import load_yaml_with_includes  # type: ignore

    return load_yaml_with_includes


# Writer types accepted by the CLI. Mirror of create_knowledge_graph.py::get_writer
# (keep in sync if a writer is added/removed there).
WRITER_TYPES = ["metta", "prolog", "neo4j", "parquet", "networkx", "kgx"]

# Toggleable build flags surfaced to the UI. `default` matches the CLI defaults in
# create_knowledge_graph.py::main.
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
    """Path-arg heuristic, mirrors create_knowledge_graph.py::_check_adapter_file_paths.

    A string arg is treated as a filesystem path only when it is explicitly
    rooted ("/", "./", "../"); bare names (labels, types) are ignored.
    """
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
    """Return (adapters_dict, input_dir) with ``input_dir`` popped out.

    Mirrors create_knowledge_graph.py::_load_adapters_config's ``pop('input_dir')``.
    """
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
    """Shallow view of the BioCypher schema: node/edge type names + per-source schemas.

    A full parse would require instantiating BioCypher (heavy, side effects), so we
    only read the top-level type keys and split them by ``represented_as``.

    A real build merges ``primer_schema_config.yaml`` with the species schema
    (create_knowledge_graph.py::merge_schemas), so we do the same here — otherwise
    species whose file is mostly empty (cel, mmu) would report zero types.
    """
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

    # Per-source auto-generated schemas (informational, read-only).
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


def list_writers() -> list[str]:
    return list(WRITER_TYPES)


def list_flags() -> list[dict]:
    return [dict(f) for f in BUILD_FLAGS]
