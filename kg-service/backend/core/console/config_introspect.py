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


# Data-source config fields the downloader acts on (see DownloadManager.process_download).
_DOWNLOAD_FIELDS = {"url", "directories", "zip_extract", "bed_url", "ep_base_url", "bucket"}


def _flat_urls(val) -> list:
    """Every link in a source's ``url`` value, whatever its shape: a string, a list,
    a {filename: url} map or a {sub_folder: links} map."""
    if isinstance(val, str):
        return [val]
    if isinstance(val, dict):
        val = list(val.values())
    if isinstance(val, list):
        return [u for v in val for u in _flat_urls(v)]
    return []


def list_data_sources(species: str) -> dict:
    """List the external data sources a species draws from (name + url).

    Reads ``config/<species>/<species>_data_source_config.yaml``. The ``version``
    field there is a resolution strategy, not a concrete version, so only name and
    url(s) are surfaced here; the actual version used appears per build in the
    graph-info datasets. Gives users scope ("what/where is this data from") before
    they build.
    """
    import yaml

    path = settings.repo_root_path / "config" / species / f"{species}_data_source_config.yaml"
    if not path.exists():
        raise ConfigError(f"No data source config for species '{species}' at {path}")
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except Exception as exc:  # noqa: BLE001 - surface a parse error as 404/400
        raise ConfigError(f"Could not parse {path.name}: {exc}")

    sources: list[dict] = []
    for key, entry in data.items():
        if key == "input_dir" or not isinstance(entry, dict):
            continue
        urls = _flat_urls(entry.get("url"))
        local_paths = [
            lf.get("stable_name") or lf.get("pattern")
            for lf in (entry.get("local_files") or {}).values()
            if isinstance(lf, dict) and (lf.get("stable_name") or lf.get("pattern"))
        ]
        sources.append({
            "key": key, "name": entry.get("name") or key, "urls": urls,
            # Whether the downloader can fetch it; local-only sources (e.g. roadmap_dhs)
            # declare files that must be placed in the input dir by hand.
            "downloadable": key == "roadmap" or bool(_DOWNLOAD_FIELDS & set(entry)),
            # Paths (relative to the input dir) this source's files land at.
            "local_paths": local_paths,
        })
    sources.sort(key=lambda s: str(s["name"]).lower())
    return {"species": species, "count": len(sources), "sources": sources}


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


# (what, species, dataset) -> (sources_stamp, value): lists rebuilt only when a config
# or the schema folder changes (sources_stamp), not on every page switch.
_by_stamp: dict = {}


def _until_configs_change(what: str, species: str, dataset: str, compute):
    import copy

    stamp = sources_stamp(species, dataset)
    hit = _by_stamp.get((what, species, dataset))
    if not (hit and hit[0] == stamp):
        hit = (stamp, compute())
        _by_stamp[(what, species, dataset)] = hit
    return copy.deepcopy(hit[1])  # callers may change what they get


def list_adapters(species: str, dataset: str) -> dict:
    """List adapters declared in a species/dataset adapters config."""
    return _until_configs_change("adapters", species, dataset,
                                 lambda: _list_adapters(species, dataset))


def _list_adapters(species: str, dataset: str) -> dict:
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


# ── Source-based grouping ─────────────────────────────────────────────────────
# The build engine is adapter-based, but the UI groups adapters by their data
# source. A source is the first path segment of an adapter's input file arg (the
# downloader writes each source into input_dir/<source>/...). Adapters whose source
# can't be derived (ontologies with "." paths, no-path adapters) fall into a shared
# group, and a per-species override file can pin any adapter to a source explicitly.

SHARED_SOURCE = "_shared"


def _match_source(seg: str, source_keys: set) -> Optional[str]:
    """Resolve a path segment to a data-source key.

    Handles exact matches and versioned directories, where the download dir differs
    from the source key by a version suffix (e.g. "catlasv1" -> "catlas"). Prefers
    the longest key that prefixes the segment so a short key never shadows a longer.
    """
    seg = seg.lstrip("./").lower()
    if not seg:
        return None
    if seg in source_keys:
        return seg
    # Versioned dir longer than the key: "catlasv1" -> "catlas" (longest wins).
    best = None
    for sk in source_keys:
        if seg.startswith(sk) and (best is None or len(sk) > len(best)):
            best = sk
    if best:
        return best
    # Dir shorter than the key, but uniquely identifying it: "gtex" -> "gtex_eqtl".
    pref = [sk for sk in source_keys if sk.startswith(seg)]
    return pref[0] if len(pref) == 1 else None


def _source_candidates(entry: dict, source_keys: set) -> list:
    """Source keys referenced by an adapter's file-path args, in first-seen order.

    Scans any string arg (or list of them) that looks like a path (contains "/"),
    plus a slash-less ``filepath`` whose whole value is the source dir
    (e.g. ``filepath: coxpressdb``).
    """
    args = (entry.get("adapter") or {}).get("args") or {}
    cands: list = []
    for key, val in args.items():
        # A list of paths (filepaths: [...]) counts like single ones, in order.
        for v in (val if isinstance(val, list) else [val]):
            if not isinstance(v, str) or not v:
                continue
            if "/" in v:
                seg = v.split("/", 1)[0]
            elif key == "filepath":
                seg = v
            else:
                continue
            m = _match_source(seg, source_keys)
            if m and m not in cands:
                cands.append(m)
    return cands


def _derive_source(name: str, entry: dict, source_keys: set) -> Optional[str]:
    """Best-effort source for an adapter from its file-path args.

    Prefers a candidate matching the adapter's name prefix (so a multi-source
    adapter like ``eva_snp_in_gene`` lands under ``eva``), else the sole candidate.
    """
    cands = _source_candidates(entry, source_keys)
    if not cands:
        return None
    for key in cands:
        if name == key or name.startswith(key + "_"):
            return key
    return cands[0] if len(cands) == 1 else sorted(cands)[0]


def _sources_in_full(species: str, source_keys: set) -> dict:
    """adapter name -> the source the full dataset gives it. Sample data is bundled
    under other paths (GENCODE's under ensembl/, say), so a sample adapter's path can
    miss its source; the same adapter in the full config names it."""
    try:
        full, _ = load_adapters_dict(species, "full")
    except Exception:  # noqa: BLE001 - a species without a full dataset
        return {}
    out = {}
    for name, entry in full.items():
        if isinstance(entry, dict):
            src = _derive_source(name, entry, source_keys)
            if src:
                out[name] = src
    return out


def load_adapter_source_overrides(species: str) -> dict:
    """Optional config/<species>/adapter_source_map.yaml mapping adapter -> source."""
    import yaml
    path = settings.repo_root_path / "config" / species / "adapter_source_map.yaml"
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except Exception:  # noqa: BLE001 - a broken override file must not break the list
        return {}
    return {str(k): str(v) for k, v in data.items() if isinstance(k, str) and v}


@functools.lru_cache(maxsize=1)
def _schema_generator():
    """The data-source schema generator (repo-root schema_generator/), set up only to
    read which source each adapter declares."""
    _load_yaml_with_includes()  # puts the repo root on sys.path
    from schema_generator.generate_data_source_schemas import SchemaGenerator  # type: ignore

    return SchemaGenerator, SchemaGenerator.for_analysis(
        settings.repo_root_path / "biocypher_metta" / "adapters")


# module -> (adapter file mtime, (source name, source url) or None)
_declared: dict = {}


def _declared_source(module: str) -> Optional[tuple]:
    """(source name, source url) an adapter's code declares, as the schema generator
    reads it; re-read only when the adapter file changes."""
    sg, gen = _schema_generator()
    path = gen.adapters_dir / gen.get_adapter_file_from_module(module)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    hit = _declared.get(module)
    if hit and hit[0] == mtime:
        return hit[1]
    gen.adapter_cache.pop(str(path.relative_to(gen.adapters_dir)), None)
    import warnings

    try:
        # Parsing an adapter warns about its own odd strings (e.g. "\G" escapes).
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", (DeprecationWarning, SyntaxWarning))
            found = gen.declared_source(module)
    except Exception:  # noqa: BLE001 - an adapter the generator can't parse
        found = None
    _declared[module] = (mtime, found)
    return found


def _origin(url) -> Optional[str]:
    from urllib.parse import urlparse

    p = urlparse(url) if isinstance(url, str) else None
    return f"{p.scheme}://{p.netloc}" if p and p.scheme in ("http", "https") and p.netloc else None


def schema_sources(species: str, adapters: dict) -> dict:
    """adapter name -> {"key", "name", "website"}: the data source the adapter's code
    declares, exactly as data_source_schemas/<species>/ lists sources (one file per
    source, named by the generator). Name and website come from that file when it is
    there (a build regenerates it), else from what the adapter declares."""
    import yaml

    sg, _ = _schema_generator()
    folder = _schema_folder(species)
    by_file: dict = {}
    out: dict = {}
    for name, entry in adapters.items():
        module = ((entry.get("adapter") or {}).get("module")) if isinstance(entry, dict) else None
        found = _declared_source(module) if module else None
        if not found:
            continue
        source_name, source_url = found
        filename = sg.source_filename(source_name)
        if filename not in by_file:
            meta: dict = {}
            try:
                meta = yaml.safe_load((folder / filename).read_text()) or {}
            except (OSError, yaml.YAMLError):
                pass
            by_file[filename] = {
                "key": filename[: -len(".yaml")],
                "name": meta.get("name") or source_name,
                "website": meta.get("website") or _origin(source_url),
            }
        out[name] = by_file[filename]
    return out


def _schema_folder(species: str) -> Path:
    return settings.repo_root_path / "data_source_schemas" / species


def _schema_files(species: str) -> dict:
    """{file stem: parsed schema file} for data_source_schemas/<species>/, read fresh
    each time so the list follows the folder as builds rewrite it."""
    import yaml

    out: dict = {}
    folder = _schema_folder(species)
    for path in sorted(folder.glob("*.yaml")) if folder.is_dir() else []:
        try:
            data = yaml.safe_load(path.read_text()) or {}
        except (OSError, yaml.YAMLError):
            continue
        if isinstance(data, dict):
            out[path.stem] = data
    return out


def sources_stamp(species: str, dataset: str) -> str:
    """Changes whenever anything the source list is made from changes: the schema
    folder (a build rewrites it), the species' configs, the shared schema."""
    import hashlib

    paths = list(_schema_folder(species).glob("*.yaml"))
    paths += list((settings.repo_root_path / "config" / species).glob("*.yaml"))
    paths += [settings.repo_root_path / "config" / "species_config.yaml",
              settings.repo_root_path / "config" / "primer_schema_config.yaml"]
    h = hashlib.sha1(f"{species}/{dataset}".encode())
    for path in sorted(paths):
        try:
            st = path.stat()
        except OSError:
            continue
        h.update(f"{path.name}:{st.st_mtime_ns}:{st.st_size};".encode())
    return h.hexdigest()[:16]


# (species, dataset) -> (stamp, merged schema config, {adapter: [{"name", "kind"}]})
_types_cache: dict = {}


def _adapter_types(species: str, dataset: str, adapters: dict) -> tuple:
    """(merged schema config, {adapter name: [{"name", "kind"}]}): the schema types each
    adapter writes, resolved the way the schema generator resolves them when it writes
    data_source_schemas/<species>/. Cached until a config or an adapter file changes."""
    import contextlib
    import hashlib
    import io
    import json
    import warnings

    sg, base = _schema_generator()
    schema_path = resolve_schema_config_path(species, dataset)
    files = {schema_path, settings.repo_root_path / "config" / "primer_schema_config.yaml"}
    for entry in adapters.values():
        module = ((entry.get("adapter") or {}).get("module")) if isinstance(entry, dict) else None
        if module:
            files.add(base.adapters_dir / base.get_adapter_file_from_module(module))
    h = hashlib.sha1(json.dumps(adapters, sort_keys=True, default=str).encode())
    for f in sorted(files, key=str):
        try:
            h.update(f"{f}:{f.stat().st_mtime_ns};".encode())
        except OSError:
            pass
    stamp = h.hexdigest()
    hit = _types_cache.get((species, dataset))
    if hit and hit[0] == stamp:
        return hit[1], hit[2]

    schema = sg.load_schema_config(str(schema_path), include_primer=True)
    gen = sg.for_analysis(base.adapters_dir, schema_config=schema)
    types: dict = {}
    # The generator reports what it can't resolve on stdout and warns about odd
    # strings in adapter files; neither belongs in the console's log.
    with contextlib.redirect_stdout(io.StringIO()), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for name, entry in adapters.items():
            if not isinstance(entry, dict):
                continue
            try:
                types[name] = gen.adapter_types(name, entry)
            except Exception:  # noqa: BLE001 - one unreadable adapter must not break the list
                types[name] = []
    _types_cache[(species, dataset)] = (stamp, schema, types)
    return schema, types


def warm_source_caches() -> None:
    """Work out, ahead of time, what every species' source list needs that is slow the
    first time (each adapter's declared source and schema types), so switching
    species in the build page is quick from the start. Runs in a background thread."""
    import logging
    import threading

    def run():
        try:
            species_cfg = load_species_config()
        except Exception:  # noqa: BLE001
            return
        for species, datasets in species_cfg.items():
            for dataset in (datasets or {}) if isinstance(datasets, dict) else ():
                try:
                    list_adapters(species, dataset)
                    _until_configs_change("sources", species, dataset,
                                          lambda: _static_source_groups(species, dataset))
                except Exception as exc:  # noqa: BLE001 - a species without configs
                    logging.getLogger(__name__).debug("warming %s/%s: %s", species, dataset, exc)

    threading.Thread(target=run, name="warm-source-caches", daemon=True).start()


def _type_detail(name: str, kind: str, file_entry: Optional[dict], schema_entry: Optional[dict]) -> dict:
    """What the wizard shows for one node or edge type: from the source's schema file
    when it lists the type, else from the schema config."""
    e = file_entry if isinstance(file_entry, dict) else (schema_entry if isinstance(schema_entry, dict) else {})
    props = e.get("properties")
    out = {
        "name": name,
        "kind": kind,
        "description": e.get("description"),
        "properties": sorted(props) if isinstance(props, dict) else [],
    }
    if kind == "edge":
        src = e.get("source") or (schema_entry or {}).get("source")
        tgt = e.get("target") or (schema_entry or {}).get("target")
        out["source"] = src if isinstance(src, str) else (src[0] if isinstance(src, list) and src else None)
        out["target"] = tgt if isinstance(tgt, str) else (tgt[0] if isinstance(tgt, list) and tgt else None)
        # The relation the graph gets: output_label, or, without one, the input_label
        # (what the writers name the edge then: e.g. is_marker_for).
        verb = (e.get("output_label") or (schema_entry or {}).get("output_label")
                or e.get("input_label") or (schema_entry or {}).get("input_label"))
        if isinstance(verb, list):
            verb = verb[0] if verb else None
        out["verb"] = verb if isinstance(verb, str) else None
    return out


def _group_types(adapter_names: list, types: dict, schema: dict, file: Optional[dict]) -> list:
    """A source's node and edge types, each with the adapters that write it. ``split``:
    one of those adapters writes other types of the same kind too, so its known
    count can't be put on this type alone."""
    order: list = []
    by_name: dict = {}
    for a in adapter_names:
        mine = types.get(a) or []
        for t in mine:
            key = (t["kind"], t["name"])
            if key not in by_name:
                section = (file or {}).get("nodes" if t["kind"] == "node" else "relationships") or {}
                by_name[key] = {**_type_detail(t["name"], t["kind"], section.get(t["name"]),
                                               schema.get(t["name"])),
                                "adapters": [], "split": False}
                order.append(key)
            by_name[key]["adapters"].append(a)
            if sum(1 for x in mine if x["kind"] == t["kind"]) > 1:
                by_name[key]["split"] = True
    return [by_name[k] for k in order]


def _file_types(file: dict) -> list:
    """Types a schema file lists, for a source no adapter in this config writes."""
    out = []
    for section, kind in (("nodes", "node"), ("relationships", "edge")):
        for name, e in ((file or {}).get(section) or {}).items():
            out.append({**_type_detail(name, kind, e, None), "adapters": [], "split": False})
    return out


def adapter_needs_dbsnp(entry: dict) -> bool:
    """True if this adapter takes the dbSNP lookup maps (same rule as the pipeline's
    _adapters_need_dbsnp): only these force the dbSNP cache to be loaded."""
    args = (entry.get("adapter") or {}).get("args") or {}
    return "dbsnp_rsid_map" in args or "dbsnp_pos_map" in args


def group_adapters_by_source(species: str, dataset: str) -> dict:
    """Adapters for a species/dataset grouped by data source, for the build wizard.

    The sources are the ones data_source_schemas/<species>/ lists: each adapter goes
    under the source its code declares (GTEx, CATLAS, OBO Foundry, ...), the same rule
    the schema generator uses to write that folder.

    Each source also lists the downloads its adapters read (``downloads``: entries of
    the species' data source config, with whether each is on disk). A source can need
    several (ENCODE: cCRE and tfbs), several sources can share one (fly GOA reads the
    FlyBase download), and some need none (OBO Foundry ontologies are fetched by the
    build). Downloads no adapter reads are still listed, on their own.
    """
    from backend.core.console import job_runner  # local import avoids an import cycle

    static = _until_configs_change("sources", species, dataset,
                                   lambda: _static_source_groups(species, dataset))
    presence: dict = {}
    try:
        for s in job_runner.data_status(species).get("sources", []):
            presence[s["key"]] = s
    except Exception:  # noqa: BLE001 - presence is a nicety, not required
        pass

    for g in static["groups"]:
        dls = g["downloads"]
        for d in dls:
            here = presence.get(d["key"], {})
            d["present"] = here.get("present")
            d["bytes"] = here.get("bytes")
            # "yours" (your read-only folder) or "console" (downloaded), when present
            d["location"] = here.get("location")
        on_disk = [d for d in dls if d["present"]]
        g["present"] = bool(dls) and len(on_disk) == len(dls)
        g["bytes"] = sum(d["bytes"] or 0 for d in on_disk) or None
        g["location"] = (None if not on_disk else
                         "yours" if all(d["location"] == "yours" for d in on_disk) else "console")
        # Where to put what can't be downloaded and isn't there yet.
        g["local_paths"] = [p for d in dls if not d["downloadable"] and not d["present"]
                            for p in d["local_paths"]]
    static["stamp"] = sources_stamp(species, dataset)
    return static


def _static_source_groups(species: str, dataset: str) -> dict:
    """Everything in the source list but what's on disk: rebuilt only when a config or
    the schema folder changes."""
    adapters, input_dir = load_adapters_dict(species, dataset)
    try:
        source_meta = {s["key"]: s for s in list_data_sources(species).get("sources", [])}
    except ConfigError:
        source_meta = {}
    # Download keys can be mixed-case (e.g. "cCRE"); match case-insensitively but
    # report the real key so presence/metadata still resolve.
    key_by_lower = {k.lower(): k for k in source_meta}
    lower_keys = set(key_by_lower)
    overrides = load_adapter_source_overrides(species)
    in_full = _sources_in_full(species, lower_keys) if dataset != "full" else {}
    declared = schema_sources(species, adapters)
    schema, types = _adapter_types(species, dataset, adapters)
    folder = _schema_files(species)

    def download(key: str) -> dict:
        meta = source_meta.get(key, {})
        return {
            "key": key,
            "name": meta.get("name") or key,
            "downloadable": bool(meta.get("downloadable", True)),
            "local_paths": meta.get("local_paths", []),
            "urls": meta.get("urls", []),
        }

    groups: dict = {}
    for name, entry in adapters.items():
        if not isinstance(entry, dict):
            continue
        dl = overrides.get(name) or _derive_source(name, entry, lower_keys) or in_full.get(name)
        dl = key_by_lower.get(dl.lower(), dl) if dl else None
        src = declared.get(name)
        if src:
            key, gname, website = src["key"], src["name"], src["website"]
        elif dl:  # an adapter the generator can't read: fall back to its download
            key, gname, website = dl, source_meta.get(dl, {}).get("name") or dl, None
        else:
            key, gname, website = SHARED_SOURCE, "Ontologies / shared", None
        g = groups.setdefault(key, {"source": key, "name": gname, "website": website,
                                    "downloads": [], "adapters": []})
        if dl and dl not in g["downloads"]:
            g["downloads"].append(dl)
        g["adapters"].append({
            "name": name,
            "nodes": bool(entry.get("nodes", False)),
            "edges": bool(entry.get("edges", False)),
            "needs_dbsnp": adapter_needs_dbsnp(entry),
        })

    # Sources the folder lists that no adapter in this config writes (their adapters are
    # off, or in another dataset's config): listed, not buildable.
    for stem, data in folder.items():
        if stem not in groups:
            groups[stem] = {"source": stem, "name": data.get("name") or stem,
                            "website": data.get("website"), "downloads": [], "adapters": []}

    # Downloads no adapter here reads (e.g. fly fca2), so they can still be fetched.
    used = {k for g in groups.values() for k in g["downloads"]}
    for key in sorted(set(source_meta) - used):
        groups["download:" + key] = {"source": "download:" + key,
                                     "name": source_meta[key].get("name") or key,
                                     "website": None, "downloads": [key], "adapters": []}

    out = []
    for g in groups.values():
        dls = [download(k) for k in g["downloads"]]
        g["downloads"] = dls
        g["downloadable"] = any(d["downloadable"] for d in dls)
        g["urls"] = [u for d in dls for u in d["urls"]]
        g["website"] = g["website"] or next((_origin(u) for u in g["urls"] if _origin(u)), None)
        g["adapter_count"] = len(g["adapters"])
        g["node_count"] = sum(1 for a in g["adapters"] if a["nodes"])
        g["edge_count"] = sum(1 for a in g["adapters"] if a["edges"])
        g["needs_dbsnp"] = any(a["needs_dbsnp"] for a in g["adapters"])
        # Whether data_source_schemas/<species>/ has this source's file (a build writes it).
        g["in_folder"] = g["source"] in folder
        g["types"] = (_group_types([a["name"] for a in g["adapters"]], types, schema, folder.get(g["source"]))
                      if g["adapters"] else _file_types(folder.get(g["source"])))
        out.append(g)
    # Sources with adapters first, alphabetical; then sources only the folder lists;
    # then downloads no adapter reads.
    out.sort(key=lambda x: (not x["adapters"], not x["in_folder"], x["source"] == SHARED_SOURCE,
                            str(x["name"]).lower()))
    return {"species": species, "dataset": dataset, "input_dir": input_dir, "groups": out}


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


# Pseudo-species that targets the shared primer schema (config/primer_schema_config.yaml).
# Every species inherits this base schema via merge_schemas() at build time; this lets the
# console edit it in one place and show it (read-only) inside each species' schema view.
BASE_SPECIES = "base"


def _primer_schema_path() -> Path:
    p = settings.repo_root_path / "config" / "primer_schema_config.yaml"
    if not p.exists():
        raise ConfigError(f"base (primer) schema not found: {p}")
    return p


def _config_path(species: str, dataset: str, kind: str) -> Path:
    if species == BASE_SPECIES:
        if kind != "schema":
            raise ConfigError("The base (shared) config is a schema only; it has no adapters.")
        return _primer_schema_path()
    if kind == "adapters":
        return resolve_adapters_config_path(species, dataset)
    if kind == "schema":
        return resolve_schema_config_path(species, dataset)
    raise ConfigError(f"Unknown config kind '{kind}'. Use 'adapters' or 'schema'.")


def read_config_text(species: str, dataset: str, kind: str) -> dict:
    """Return the raw text of a species/dataset config file for editing, and whether
    this instance edited it (config_store.status)."""
    from backend.core.console import config_store

    path = _config_path(species, dataset, kind)
    return {"species": species, "dataset": dataset, "kind": kind,
            "path": str(path), "content": path.read_text(), **config_store.status(path)}


def reset_config(species: str, dataset: str, kind: str) -> dict:
    """Put the shipped version of a config file back (this instance's edit is dropped)."""
    from backend.core.console import config_store

    path = _config_path(species, dataset, kind)
    try:
        config_store.reset(path)
    except ValueError as exc:
        raise ConfigError(str(exc))
    return read_config_text(species, dataset, kind)


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
    save, warnings are returned. The edit stays with this console instance
    (config_store), never the shipped config.
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
    from backend.core.console import config_store

    config_store.mark_edited(path)
    return {"species": species, "dataset": dataset, "kind": kind,
            "path": str(path), "content": content, "warnings": warnings,
            **config_store.status(path)}


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
    """Ordered list of the config's named entries + a light summary of each.

    For a species schema, also returns ``inherited``: the primer (base) entries that
    this species does not override. They are shared and read-only here (edit them in
    the base view), but surfaced so the full type set is visible, not hidden.
    """
    path = _config_path(species, dataset, kind)
    doc = _load_config_dict(path)
    entries = [
        _entry_summary(kind, str(name), val)
        for name, val in doc.items()
        if isinstance(val, dict)  # skip scalar keys like Title / input_dir
    ]
    inherited: list[dict] = []
    if kind == "schema" and species != BASE_SPECIES:
        own = {e["name"] for e in entries}
        primer = _load_config_dict(_primer_schema_path())
        for name, val in primer.items():
            if isinstance(val, dict) and str(name) not in own:
                summary = _entry_summary("schema", str(name), val)
                summary["inherited"] = True
                inherited.append(summary)
    return {"species": species, "dataset": dataset, "kind": kind,
            "path": str(path), "count": len(entries),
            "entries": entries, "inherited": inherited}


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
