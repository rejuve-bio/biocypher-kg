"""
Knowledge graph generation through BioCypher script
"""

import importlib
import json
import os
import tempfile
import time
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import List, Optional

import typer
import yaml
from biocypher import BioCypher
from biocypher._logger import logger
from biocypher_metta.kgx_writer import *
from biocypher_metta.metta_writer import *
from biocypher_metta.neo4j_csv_writer import *
from biocypher_metta.networkx_writer import NetworkXWriter
from biocypher_metta.parquet_writer import ParquetWriter
from biocypher_metta.processors import DBSNPProcessor
from biocypher_metta.prolog_writer import PrologWriter
from checkpoint_manager import CheckpointManager, prompt_resume_or_restart
from config.yaml_loader import load_yaml_with_includes


app = typer.Typer()


def load_species_config(config_path: str = "config/species_config.yaml") -> dict:
    """Load species configuration from YAML file."""
    try:
        with open(config_path, "r") as fp:
            species_config = load_yaml_with_includes(fp)
            logger.info(f"Loaded species configuration from {config_path}")
            return species_config
    except FileNotFoundError:
        logger.error(f"Species config file not found: {config_path}")
        logger.error("Please create config/species_config.yaml with species configurations")
        raise typer.Exit(1)
    except yaml.YAMLError as exc:
        logger.error(f"Error parsing species config file: {config_path}")
        logger.error(exc)
        raise typer.Exit(1)


def get_writer(
    writer_type: str,
    output_dir: Path,
    schema_config_path: Path,
    include_curie: bool = False,
):
    writer_type = writer_type.lower()

    if writer_type == "metta":
        return MeTTaWriter(
            schema_config=str(schema_config_path),
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir,
            include_curie=include_curie,
        )
    if writer_type == "prolog":
        return PrologWriter(
            schema_config=str(schema_config_path),
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir,
            include_curie=include_curie,
        )
    if writer_type == "neo4j":
        return Neo4jCSVWriter(
            schema_config=str(schema_config_path),
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir,
            include_curie=include_curie,
        )
    if writer_type == "parquet":
        return ParquetWriter(
            schema_config=str(schema_config_path),
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir,
            buffer_size=10000,
            overwrite=True,
            include_curie=include_curie,
        )
    if writer_type == "kgx":
        return KGXWriter(
            schema_config=str(schema_config_path),
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir,
            include_curie=include_curie,
        )
    if writer_type == "networkx":
        return NetworkXWriter(
            schema_config=str(schema_config_path),
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir,
            include_curie=include_curie,
        )

    raise ValueError(f"Unknown writer type: {writer_type}")


def preprocess_schema(schema_config_path: Path):
    def convert_input_labels(label, replace_char="_"):
        if isinstance(label, list):
            return [item.replace(" ", replace_char) for item in label]
        return label.replace(" ", replace_char)

    bcy = BioCypher(
        schema_config_path=str(schema_config_path),
        biocypher_config_path="config/biocypher_config.yaml",
    )
    schema = bcy._get_ontology_mapping()._extend_schema()
    edge_node_types = {}

    for _, value in schema.items():
        if value.get("abstract", False) or value.get("represented_as") != "edge":
            continue

        source_type = value.get("source", None)
        target_type = value.get("target", None)

        if source_type is not None and target_type is not None:
            input_label = value["input_label"]
            if isinstance(input_label, list):
                label = convert_input_labels(input_label[0])
            else:
                label = convert_input_labels(input_label)

            if isinstance(source_type, list):
                processed_source = [convert_input_labels(item).lower() for item in source_type]
            else:
                processed_source = convert_input_labels(source_type).lower()

            if isinstance(target_type, list):
                processed_target = [convert_input_labels(item).lower() for item in target_type]
            else:
                processed_target = convert_input_labels(target_type).lower()

            output_label = value.get("output_label", None)
            if output_label:
                if isinstance(output_label, list):
                    processed_output_label = convert_input_labels(output_label[0]).lower()
                else:
                    processed_output_label = convert_input_labels(output_label).lower()
            else:
                processed_output_label = None

            edge_node_types[label.lower()] = {
                "source": processed_source,
                "target": processed_target,
                "output_label": processed_output_label,
            }

    return edge_node_types


def gather_graph_info(nodes_count, nodes_props, edges_count, schema_dict, output_dir):
    graph_info = {
        "node_count": sum(nodes_count.values()),
        "edge_count": sum(edges_count.values()),
        "dataset_count": 0,
        "data_size": "",
        "top_entities": [{"name": node, "count": count} for node, count in nodes_count.items()],
        "top_connections": [],
        "frequent_relationships": [],
        "schema": {"nodes": [], "edges": []},
        "datasets": [],
    }

    predicate_count = Counter()
    relations_frequency = Counter()
    possible_connections = defaultdict(set)

    for edge_key, count in edges_count.items():
        parts = edge_key.split("|")
        if len(parts) == 3:
            edge_type, source_type, target_type = parts
        else:
            edge_type = edge_key
            if edge_type.lower() in schema_dict:
                source_type = schema_dict[edge_type.lower()]["source"]
                target_type = schema_dict[edge_type.lower()]["target"]
            else:
                continue

        if edge_type.lower() in schema_dict:
            label = schema_dict[edge_type.lower()]["output_label"] or edge_type
            predicate_count[label] += count

            if isinstance(source_type, list):
                for src in source_type:
                    relations_frequency[f"{src}|{target_type}"] += count
                    possible_connections[f"{src}|{target_type}"].add(label)
            elif isinstance(target_type, list):
                for tgt in target_type:
                    relations_frequency[f"{source_type}|{tgt}"] += count
                    possible_connections[f"{source_type}|{tgt}"].add(label)
            else:
                relations_frequency[f"{source_type}|{target_type}"] += count
                possible_connections[f"{source_type}|{target_type}"].add(label)

    graph_info["top_connections"] = [
        {"name": predicate, "count": count}
        for predicate, count in predicate_count.items()
    ]
    graph_info["frequent_relationships"] = [
        {"entities": rel.split("|"), "count": count}
        for rel, count in relations_frequency.items()
    ]

    for node, props in nodes_props.items():
        graph_info["schema"]["nodes"].append(
            {"data": {"name": node, "properties": list(props)}}
        )

    for conn, pos_connections in possible_connections.items():
        source, target = conn.split("|")
        graph_info["schema"]["edges"].append(
            {
                "data": {
                    "source": source,
                    "target": target,
                    "possible_connections": list(pos_connections),
                }
            }
        )

    total_size = sum(
        file.stat().st_size for file in Path(output_dir).rglob("*") if file.is_file()
    )
    total_size_gb = total_size / (1024**3)
    graph_info["data_size"] = f"{total_size_gb:.2f} GB"

    return graph_info


def _fmt_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {seconds:.1f}s"

    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.1f}s"


def _load_adapters_config(config_path: Path, context: str) -> dict:
    with open(config_path, "r") as fp:
        try:
            return load_yaml_with_includes(fp)
        except yaml.YAMLError as exc:
            logger.error(f"Error loading adapter config for {context}")
            logger.error(exc)
            raise typer.Exit(1)


_PREFLIGHT_SKIP_ARGS = {"cache_dir"}


def _check_adapter_file_paths(adapters_dict: dict) -> dict:
    """Return {adapter_name: {arg_name: path}} for every declared path that does not exist."""
    missing = {}
    for adapter_name, adapter_entry in adapters_dict.items():
        args = (adapter_entry.get("adapter") or {}).get("args") or {}
        adapter_missing = {}
        for arg_name, value in args.items():
            if arg_name in _PREFLIGHT_SKIP_ARGS:
                continue
            if arg_name == "feature_files" and isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        path = item.get("path")
                        if isinstance(path, str) and not Path(path).exists():
                            adapter_missing[f"feature_files[{i}].path"] = path
                continue
            if not isinstance(value, str):
                continue
            if not (value.startswith("/") or value.startswith("./") or value.startswith("../")):
                continue
            if not Path(value).exists():
                adapter_missing[arg_name] = value
        if adapter_missing:
            missing[adapter_name] = adapter_missing
    return missing


def process_adapters(
    adapters_dict,
    dbsnp_rsids_dict,
    dbsnp_pos_dict,
    writer,
    write_properties,
    add_provenance,
    schema_dict,
    checkpoint_manager: Optional[CheckpointManager] = None,
    skip_preflight: bool = False,
):
    """
    Iterate over all adapters, write nodes/edges, and accumulate statistics.

    When a CheckpointManager is provided:
    - Adapters that appear in checkpoint_manager.completed_adapters are skipped.
    - After each successful adapter the checkpoint is updated with the latest
      accumulated counts so a subsequent resume can pick up from that point.
    - If an adapter raises an exception the checkpoint is saved with the
      failing adapter name before re-raising, so the user can fix the data
      and resume without losing prior progress.
    """
    if not skip_preflight:
        missing = _check_adapter_file_paths(adapters_dict)
        if missing:
            logger.error(
                f"Pre-flight check failed — {len(missing)} adapter(s) have missing file paths:"
            )
            for adapter_name, bad_args in missing.items():
                logger.error(f"\n  [{adapter_name}]")
                for arg_name, path in bad_args.items():
                    logger.error(f"    {arg_name}: {path}")
            logger.error(
                "\nFix the paths above or run with --skip-preflight to bypass this check."
            )
            raise typer.Exit(1)
        logger.info("Pre-flight path check passed.")

    if checkpoint_manager is not None and checkpoint_manager.completed_adapters:
        nodes_count, nodes_props, edges_count, datasets_dict = (
            checkpoint_manager.restore_accumulators()
        )
        logger.info(
            f"Restored accumulators: "
            f"{sum(nodes_count.values())} nodes, "
            f"{sum(edges_count.values())} edges."
        )
    else:
        nodes_count = Counter()
        nodes_props = defaultdict(set)
        edges_count = Counter()
        datasets_dict = {}

    completed_adapters = list(
        checkpoint_manager.completed_adapters if checkpoint_manager else []
    )
    total_start = time.time()

    for adapter_name in adapters_dict:
        if adapter_name in completed_adapters:
            logger.info(f"Skipping adapter (already completed): {adapter_name}")
            continue

        adapter_start = time.time()
        writer.clear_counts()
        logger.info(f"Running adapter: {adapter_name}")

        adapter_config = adapters_dict[adapter_name]["adapter"]
        adapter_module = importlib.import_module(adapter_config["module"])
        adapter_cls = getattr(adapter_module, adapter_config["cls"])
        ctr_args = adapter_config["args"]

        if "dbsnp_rsid_map" in ctr_args:
            ctr_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
        if "dbsnp_pos_map" in ctr_args:
            ctr_args["dbsnp_pos_map"] = dbsnp_pos_dict
        ctr_args["write_properties"] = write_properties
        ctr_args["add_provenance"] = add_provenance

        adapter = adapter_cls(**ctr_args)
        write_nodes = adapters_dict[adapter_name]["nodes"]
        write_edges = adapters_dict[adapter_name]["edges"]
        outdir = adapters_dict[adapter_name]["outdir"]

        dataset_name = getattr(adapter, "source", None)
        version = getattr(adapter, "version", None)
        source_url = getattr(adapter, "source_url", None)

        if dataset_name is None:
            logger.warning(
                f"Dataset name is None for adapter: {adapter_name}. "
                "Ensure 'source' is defined in the adapter constructor."
            )
        elif dataset_name not in datasets_dict:
            datasets_dict[dataset_name] = {
                "name": dataset_name,
                "version": version,
                "url": source_url,
                "nodes": set(),
                "edges": set(),
                "imported_on": str(date.today()),
            }

        try:
            if write_nodes:
                nodes = adapter.get_nodes()
                freq, props = writer.write_nodes(nodes, path_prefix=outdir)
                for node_label, node_count in freq.items():
                    nodes_count[node_label] += node_count
                    if dataset_name is not None:
                        datasets_dict[dataset_name]["nodes"].add(node_label)
                for node_label, node_props in props.items():
                    nodes_props[node_label] = nodes_props[node_label].union(node_props)

            if write_edges:
                edges = adapter.get_edges()
                freq = writer.write_edges(edges, path_prefix=outdir)
                for edge_label_key, edge_count in freq.items():
                    edges_count[edge_label_key] += edge_count

                    parts = edge_label_key.split("|")
                    edge_type = parts[0]

                    if edge_type.lower() in schema_dict:
                        output_label = (
                            schema_dict[edge_type.lower()]["output_label"] or edge_type
                        )
                    else:
                        output_label = edge_type

                    if dataset_name is not None:
                        datasets_dict[dataset_name]["edges"].add(output_label)

        except Exception as exc:
            elapsed = _fmt_elapsed(time.time() - adapter_start)
            logger.error(f"Adapter '{adapter_name}' failed after {elapsed}: {exc}")
            if checkpoint_manager is not None:
                checkpoint_manager.save(
                    completed_adapters=completed_adapters,
                    nodes_count=nodes_count,
                    nodes_props=nodes_props,
                    edges_count=edges_count,
                    datasets_dict=datasets_dict,
                    failed_adapter=adapter_name,
                )
                logger.info(
                    f"Checkpoint saved. Re-run the pipeline to resume from adapter '{adapter_name}'."
                )
            raise

        completed_adapters.append(adapter_name)
        logger.info(
            f"Adapter '{adapter_name}' completed in {_fmt_elapsed(time.time() - adapter_start)}"
        )
        if checkpoint_manager is not None:
            checkpoint_manager.save(
                completed_adapters=completed_adapters,
                nodes_count=nodes_count,
                nodes_props=nodes_props,
                edges_count=edges_count,
                datasets_dict=datasets_dict,
                failed_adapter=None,
            )
            logger.info(f"Checkpoint updated after adapter: {adapter_name}")

    logger.info(f"All adapters completed in {_fmt_elapsed(time.time() - total_start)}")
    return nodes_count, nodes_props, edges_count, datasets_dict


def _write_graph_info(
    nodes_count, nodes_props, edges_count, schema_dict, output_dir, datasets_dict
):
    """Build and write graph_info.json."""
    graph_info = gather_graph_info(
        nodes_count, nodes_props, edges_count, schema_dict, output_dir
    )
    for dataset_name in datasets_dict:
        datasets_dict[dataset_name]["nodes"] = list(datasets_dict[dataset_name]["nodes"])
        datasets_dict[dataset_name]["edges"] = list(datasets_dict[dataset_name]["edges"])
        graph_info["datasets"].append(datasets_dict[dataset_name])

    graph_info["dataset_count"] = len(graph_info["datasets"])

    file_path = Path(output_dir) / "graph_info.json"
    with open(file_path, "w") as fp:
        json.dump(graph_info, fp, indent=2)

    logger.info(f"graph_info.json written to {file_path}")
    return graph_info


def _load_dbsnp(cache_root: str, variant: Optional[str], is_sample: bool = False) -> tuple:
    """Load dbSNP mappings using DBSNPProcessor."""
    if not cache_root:
        if is_sample:
            logger.info("No dbSNP cache directory specified, continuing without rsID mappings")
            return {}, {}
        logger.error("=" * 80)
        logger.error("ERROR: dbSNP cache root not set for a full dataset run.")
        logger.error("")
        logger.error("Solutions:")
        logger.error("  1. Set `dbsnp_cache_root` in config/species_config.yaml, OR")
        logger.error("  2. Pass --dbsnp-cache-root <path> on the command line")
        logger.error("")
        logger.error("Generate the cache with:")
        logger.error("  python3 scripts/update_dbsnp.py --cache-dir <root>/common --common-only")
        logger.error("  python3 scripts/update_dbsnp.py --cache-dir <root>/full")
        logger.error("=" * 80)
        raise typer.Exit(1)

    if is_sample:
        cache_path = Path(cache_root)
    else:
        if not variant:
            logger.error("=" * 80)
            logger.error("ERROR: dbSNP variant not specified for a full dataset run.")
            logger.error("")
            logger.error("Pass --dbsnp-variant common|full on the command line,")
            logger.error("or set `dbsnp_variant` in config/species_config.yaml.")
            logger.error("=" * 80)
            raise typer.Exit(1)
        if variant not in ("common", "full"):
            logger.error(
                f"ERROR: invalid --dbsnp-variant '{variant}'. Expected 'common' or 'full'."
            )
            raise typer.Exit(1)
        cache_path = Path(cache_root) / variant

    if not cache_path.exists() or not cache_path.is_dir():
        if is_sample:
            logger.warning(
                f"dbSNP cache directory not found at {cache_path}, continuing without rsID mappings"
            )
            return {}, {}
        logger.error("=" * 80)
        logger.error(f"ERROR: dbSNP cache directory not found: {cache_path}")
        logger.error("")
        logger.error("Generate the cache with:")
        logger.error(
            f"  python3 scripts/update_dbsnp.py --cache-dir {cache_path}"
            + (" --common-only" if variant == "common" else "")
        )
        logger.error("=" * 80)
        raise typer.Exit(1)

    db_file = cache_path / "dbsnp_mapping.db"
    pkl_file = cache_path / "dbsnp_mapping.pkl"
    if not db_file.exists() and not pkl_file.exists():
        if is_sample:
            logger.warning(f"No dbSNP mapping file at {cache_path}, continuing without rsID mappings")
            return {}, {}
        logger.error("=" * 80)
        logger.error(f"ERROR: No dbSNP mapping file in {cache_path}")
        logger.error(f"Expected {db_file} or {pkl_file}")
        logger.error("")
        logger.error("Generate the cache with:")
        logger.error(
            f"  python3 scripts/update_dbsnp.py --cache-dir {cache_path}"
            + (" --common-only" if variant == "common" else "")
        )
        logger.error("=" * 80)
        raise typer.Exit(1)

    try:
        variant_label = "sample" if is_sample else variant
        logger.info(f"Preparing dbSNP cache (variant={variant_label}) from {cache_path}")
        dbsnp_proc = DBSNPProcessor(cache_dir=str(cache_path))
        logger.info("Opening dbSNP mapping backend...")
        dbsnp_proc.load_mapping()
        logger.info("Creating dbSNP lookup wrappers...")
        rsids_dict, pos_dict = dbsnp_proc.get_dict_wrappers()
        logger.info(
            f"Loaded {len(rsids_dict):,} rsID mappings (variant={variant_label}) from {cache_path}"
        )

        if not is_sample:
            actual_common = dbsnp_proc.is_common_only()
            if actual_common is True and variant == "full":
                logger.warning(
                    f"Requested variant=full but cache at {cache_path} was built with --common-only"
                )
            elif actual_common is False and variant == "common":
                logger.warning(
                    f"Requested variant=common but cache at {cache_path} is the full dataset"
                )

        return rsids_dict, pos_dict
    except Exception as exc:
        if is_sample:
            logger.warning(f"Failed to load dbSNP mappings from {cache_path}: {exc}")
            return {}, {}
        logger.error("=" * 80)
        logger.error(f"ERROR: Failed to load dbSNP mappings: {exc}")
        logger.error("=" * 80)
        raise typer.Exit(1)


def merge_schemas(primer_schema_path, species_schema_path):
    """
    Merge two BioCypher schema YAML files into a temporary file.
    """
    primer_schema_path = Path(primer_schema_path)
    species_schema_path = Path(species_schema_path)

    with open(primer_schema_path, "r") as fp:
        primer_schema = yaml.safe_load(fp)

    with open(species_schema_path, "r") as fp:
        species_schema = yaml.safe_load(fp)

    merged_schema = {**primer_schema, **species_schema}

    temp_fd, temp_path = tempfile.mkstemp(
        suffix=".yaml",
        dir=primer_schema_path.parent,
    )

    try:
        with os.fdopen(temp_fd, "w") as fp:
            items = list(merged_schema.items())
            for index, (schema_name, schema_content) in enumerate(items):
                yaml.dump(
                    {schema_name: schema_content},
                    fp,
                    default_flow_style=False,
                    sort_keys=False,
                )
                if index < len(items) - 1:
                    fp.write("\n")
    except Exception as exc:
        Path(temp_path).unlink(missing_ok=True)
        raise RuntimeError(f"Error writing merged schema: {exc}")

    return Path(temp_path)


def delete_temp_schema(temp_path):
    """Delete the temporary file created by merge_schemas."""
    try:
        Path(temp_path).unlink(missing_ok=True)
    except OSError:
        pass


@app.command()
def main(
    species: Optional[str] = typer.Option(
        None,
        help="Species to generate KG for: hsa, dmel, cel, mmo, rno, or 'all'",
    ),
    dataset: str = typer.Option(
        "full",
        help="Dataset size: 'sample' or 'full' (default: full)",
    ),
    species_config_path: str = typer.Option(
        "config/species_config.yaml",
        help="Path to species configuration YAML file",
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        file_okay=False,
        dir_okay=True,
        help="Output directory (required)",
    ),
    adapters_config: Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Adapters config path (manual mode only)",
    ),
    dbsnp_cache_root: Optional[str] = typer.Option(
        None,
        "--dbsnp-cache-root",
        help="dbSNP cache root directory. For non-sample runs, must contain "
        "'common/' and/or 'full/' subdirectories produced by scripts/update_dbsnp.py.",
    ),
    dbsnp_variant: Optional[str] = typer.Option(
        None,
        "--dbsnp-variant",
        help="Which dbSNP variant subset to load: 'common' or 'full'. "
        "Required for non-sample runs (unless set via species_config.yaml).",
    ),
    schema_config: Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Schema config path (manual mode only)",
    ),
    writer_type: str = typer.Option(
        default="metta",
        help="Choose writer type: metta, prolog, neo4j, parquet, networkx, KGX",
    ),
    write_properties: bool = typer.Option(
        True, help="Write properties to nodes and edges"
    ),
    add_provenance: bool = typer.Option(
        True, help="Add provenance to nodes and edges"
    ),
    include_curie: bool = typer.Option(
        False,
        "--include-curie/--no-curie",
        help="Keep CURIE namespace prefixes in node/edge IDs; by default these IDs "
        "are written without prefixes, though some ontology labels may still retain them.",
    ),
    buffer_size: int = typer.Option(10000, help="Buffer size for Parquet writer"),
    overwrite: bool = typer.Option(True, help="Overwrite existing Parquet files"),
    include_adapters: Optional[List[str]] = typer.Option(
        None,
        help="Specific adapters to include (space-separated, default: all)",
        case_sensitive=False,
    ),
    no_checkpoint: bool = typer.Option(
        False,
        "--no-checkpoint",
        help="Disable checkpointing entirely (always start fresh, never write checkpoint).",
    ),
    resume: Optional[bool] = typer.Option(
        None,
        "--resume/--restart",
        help=(
            "When a checkpoint exists: --resume continues from it, "
            "--restart deletes it and starts over. "
            "If omitted you will be prompted interactively."
        ),
    ),
    skip_preflight: bool = typer.Option(
        False,
        "--skip-preflight",
        help="Skip pre-flight file path validation before running adapters.",
    ),
    check_only: bool = typer.Option(
        False,
        "--check-only",
        help=(
            "Validate file paths declared in --adapters-config and exit without running any adapters. "
            "Only --adapters-config is required in this mode."
        ),
    ),
):
    """
    Main function. Call individual adapters to download and process data. Build
    via BioCypher from node and edge data.
    """
    if check_only:
        if adapters_config is None:
            logger.error("--adapters-config is required with --check-only")
            raise typer.Exit(1)
        adapters_dict = _load_adapters_config(adapters_config, str(adapters_config))
        if include_adapters:
            include_lower = [a.lower() for a in include_adapters]
            adapters_dict = {k: v for k, v in adapters_dict.items() if k.lower() in include_lower}
        missing = _check_adapter_file_paths(adapters_dict)
        if missing:
            logger.error(
                f"Pre-flight check failed — {len(missing)} adapter(s) have missing file paths:"
            )
            for adapter_name, bad_args in missing.items():
                logger.error(f"\n  [{adapter_name}]")
                for arg_name, path in bad_args.items():
                    logger.error(f"    {arg_name}: {path}")
            raise typer.Exit(1)
        logger.info(
            f"Pre-flight check passed — all {len(adapters_dict)} adapter(s) have valid file paths."
        )
        raise typer.Exit(0)

    manual_mode = all([adapters_config, schema_config])
    species_mode = species is not None

    if not manual_mode and not species_mode:
        logger.error("You must either:")
        logger.error(
            "  1. Use --species flag with --output-dir "
            "(e.g., --species hsa --dataset sample --output-dir output_hsa)"
        )
        logger.error(
            "  2. Provide all manual parameters (--output-dir, --adapters-config, --schema-config)"
        )
        raise typer.Exit(1)

    if output_dir is None:
        logger.error("--output-dir is required")
        raise typer.Exit(1)

    is_merged_schema = False
    temp_schema_to_cleanup = None

    try:
        if species_mode:
            species_config = load_species_config(species_config_path)
            if species.lower() == "all":
                logger.info("Generating KG for all species")
                logger.info(f"Base output directory: {output_dir}")
                available_species = list(species_config.keys())

                for sp in available_species:
                    if dataset not in species_config[sp]:
                        logger.warning(
                            f"Dataset '{dataset}' not available for species '{sp}', skipping..."
                        )
                        continue

                    sp_output_dir = output_dir / sp
                    logger.info(f"\n{'=' * 60}")
                    logger.info(f"Processing {sp} - {dataset}")
                    logger.info(f"Output: {sp_output_dir}")
                    logger.info(f"{'=' * 60}\n")

                    sp_output_dir.mkdir(parents=True, exist_ok=True)
                    config = species_config[sp][dataset]

                    sp_adapters_config = Path(config["adapters_config"])
                    sp_schema_config = merge_schemas(
                        "config/primer_schema_config.yaml",
                        Path(config["schema_config"]),
                    )
                    sp_is_sample = dataset == "sample"
                    sp_cache_root = dbsnp_cache_root or config.get("dbsnp_cache_root", "")
                    if not sp_cache_root and sp_is_sample:
                        sp_cache_root = "aux_files/hsa/sample_dbsnp"
                    sp_variant = dbsnp_variant or config.get("dbsnp_variant") or None

                    sp_dbsnp_rsids_dict, sp_dbsnp_pos_dict = _load_dbsnp(
                        sp_cache_root, sp_variant, is_sample=sp_is_sample
                    )

                    bc = get_writer(
                        writer_type,
                        sp_output_dir,
                        sp_schema_config,
                        include_curie=include_curie,
                    )
                    logger.info(f"Using {writer_type} writer for {sp}")

                    if writer_type == "parquet":
                        bc.buffer_size = buffer_size
                        bc.overwrite = overwrite

                    schema_dict = preprocess_schema(sp_schema_config)
                    sp_adapters_dict = _load_adapters_config(sp_adapters_config, sp)

                    if include_adapters:
                        original_count = len(sp_adapters_dict)
                        include_lower = [adapter.lower() for adapter in include_adapters]
                        sp_adapters_dict = {
                            key: value
                            for key, value in sp_adapters_dict.items()
                            if key.lower() in include_lower
                        }
                        if not sp_adapters_dict:
                            logger.error(f"No matching adapters found for {sp}.")
                            continue
                        logger.info(
                            f"Filtered to {len(sp_adapters_dict)}/{original_count} adapters for {sp}"
                        )

                    ckpt = _setup_checkpoint(
                        sp_output_dir,
                        pipeline_id=f"{sp_output_dir}::{sp_adapters_config}",
                        no_checkpoint=no_checkpoint,
                        resume=resume,
                    )

                    nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
                        sp_adapters_dict,
                        sp_dbsnp_rsids_dict,
                        sp_dbsnp_pos_dict,
                        bc,
                        write_properties,
                        add_provenance,
                        schema_dict,
                        checkpoint_manager=ckpt,
                        skip_preflight=skip_preflight,
                    )

                    if writer_type == "networkx":
                        bc.write_graph()
                        logger.info(f"NetworkX graph saved for {sp}")

                    if hasattr(bc, "finalize"):
                        bc.finalize()

                    _write_graph_info(
                        nodes_count,
                        nodes_props,
                        edges_count,
                        schema_dict,
                        sp_output_dir,
                        datasets_dict,
                    )

                    if ckpt is not None:
                        ckpt.delete()

                    delete_temp_schema(sp_schema_config)

                    logger.info(f"Done with {sp}")
                    logger.info(f"Total nodes processed for {sp}: {sum(nodes_count.values())}")
                    logger.info(f"Total edges processed for {sp}: {sum(edges_count.values())}")

                logger.info("\n" + "=" * 60)
                logger.info("All species processed successfully!")
                logger.info("=" * 60)
                return

            if species not in species_config:
                logger.error(f"Unknown species: {species}")
                logger.error(f"Available: {', '.join(species_config.keys())}")
                raise typer.Exit(1)

            if dataset not in species_config[species]:
                logger.error(f"Dataset '{dataset}' not available for species '{species}'")
                logger.error(
                    f"Available datasets: {', '.join(species_config[species].keys())}"
                )
                raise typer.Exit(1)

            config = species_config[species][dataset]
            logger.info(f"Generating KG for {species} using {dataset} dataset")
            logger.info(f"Output directory: {output_dir}")
            logger.info(f"Write properties: {write_properties}")
            logger.info(f"Add provenance: {add_provenance}")

            output_dir.mkdir(parents=True, exist_ok=True)

            adapters_config = Path(config["adapters_config"])
            schema_config = merge_schemas(
                "config/primer_schema_config.yaml",
                Path(config["schema_config"]),
            )
            is_merged_schema = True
            temp_schema_to_cleanup = schema_config
            cfg_cache_root = config.get("dbsnp_cache_root", "")
            cfg_variant = config.get("dbsnp_variant") or None
        else:
            cfg_cache_root = ""
            cfg_variant = None

        is_sample_config = dataset == "sample" if species_mode else "sample" in str(adapters_config).lower()

        resolved_cache_root = dbsnp_cache_root or cfg_cache_root
        if not resolved_cache_root and is_sample_config:
            resolved_cache_root = "aux_files/hsa/sample_dbsnp"
        resolved_variant = dbsnp_variant or cfg_variant

        dbsnp_rsids_dict, dbsnp_pos_dict = _load_dbsnp(
            resolved_cache_root,
            resolved_variant,
            is_sample=is_sample_config,
        )

        if not species_mode and str(schema_config) == "config/primer_schema_config.yaml":
            potential_species = None
            for part in Path(adapters_config).parts:
                if part in ["hsa", "dmel", "cel", "mmo", "rno"]:
                    potential_species = part
                    break

            if potential_species:
                species_schema_path = Path(
                    f"config/{potential_species}/{potential_species}_schema_config.yaml"
                )
                if species_schema_path.exists():
                    logger.info(
                        f"Manual mode: detected species '{potential_species}' from adapters config."
                    )
                    logger.info(
                        f"Automatically merging primer schema with {species_schema_path}"
                    )
                    schema_config = merge_schemas(
                        "config/primer_schema_config.yaml",
                        species_schema_path,
                    )
                    is_merged_schema = True
                    temp_schema_to_cleanup = schema_config

        bc = get_writer(
            writer_type,
            output_dir,
            schema_config,
            include_curie=include_curie,
        )
        logger.info(f"Using {writer_type} writer")

        if writer_type == "parquet":
            bc.buffer_size = buffer_size
            bc.overwrite = overwrite

        schema_dict = preprocess_schema(schema_config)
        adapters_dict = _load_adapters_config(adapters_config, str(adapters_config))

        if include_adapters:
            original_count = len(adapters_dict)
            include_lower = [adapter.lower() for adapter in include_adapters]
            adapters_dict = {
                key: value
                for key, value in adapters_dict.items()
                if key.lower() in include_lower
            }
            if not adapters_dict:
                available = "\n".join(f" - {adapter}" for adapter in adapters_dict.keys())
                logger.error(f"No matching adapters found. Available adapters:\n{available}")
                raise typer.Exit(1)
            logger.info(f"Filtered to {len(adapters_dict)}/{original_count} adapters")

        output_dir.mkdir(parents=True, exist_ok=True)
        ckpt = _setup_checkpoint(
            output_dir,
            pipeline_id=f"{output_dir}::{adapters_config}",
            no_checkpoint=no_checkpoint,
            resume=resume,
        )

        nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
            adapters_dict,
            dbsnp_rsids_dict,
            dbsnp_pos_dict,
            bc,
            write_properties,
            add_provenance,
            schema_dict,
            checkpoint_manager=ckpt,
            skip_preflight=skip_preflight,
        )

        if writer_type == "networkx":
            bc.write_graph()
            logger.info("NetworkX graph saved successfully")

        if hasattr(bc, "finalize"):
            bc.finalize()

        _write_graph_info(
            nodes_count,
            nodes_props,
            edges_count,
            schema_dict,
            output_dir,
            datasets_dict,
        )

        if ckpt is not None:
            ckpt.delete()

        logger.info("Done")
        logger.info(f"Total nodes processed: {sum(nodes_count.values())}")
        logger.info(f"Total edges processed: {sum(edges_count.values())}")
    finally:
        if is_merged_schema and temp_schema_to_cleanup is not None:
            delete_temp_schema(temp_schema_to_cleanup)


def _setup_checkpoint(
    output_dir: Path,
    pipeline_id: str,
    no_checkpoint: bool,
    resume: Optional[bool],
) -> Optional[CheckpointManager]:
    """
    Return a configured CheckpointManager, or None if checkpointing is disabled.
    """
    if no_checkpoint:
        logger.info("Checkpointing disabled (--no-checkpoint).")
        return None

    ckpt = CheckpointManager(output_dir=output_dir, pipeline_id=pipeline_id)

    if not ckpt.exists():
        logger.info("No existing checkpoint — starting fresh.")
        return ckpt

    loaded = ckpt.load()
    if not loaded:
        return ckpt

    if resume is True:
        logger.info("--resume flag set: resuming from checkpoint.")
        return ckpt

    if resume is False:
        logger.info("--restart flag set: deleting checkpoint and starting over.")
        ckpt.delete()
        return CheckpointManager(output_dir=output_dir, pipeline_id=pipeline_id)

    should_resume = prompt_resume_or_restart(ckpt)
    if should_resume:
        return ckpt

    return CheckpointManager(output_dir=output_dir, pipeline_id=pipeline_id)


if __name__ == "__main__":
    app()
