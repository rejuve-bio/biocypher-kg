"""
Knowledge graph generation through BioCypher script
"""

from datetime import date
from pathlib import Path

from biocypher import BioCypher
from biocypher_metta.metta_writer import *
from biocypher_metta.prolog_writer import PrologWriter
from biocypher_metta.neo4j_csv_writer import *
from biocypher_metta.kgx_writer import *
from biocypher_metta.parquet_writer import ParquetWriter
from biocypher_metta.networkx_writer import NetworkXWriter
from biocypher_metta.processors import DBSNPProcessor
from biocypher._logger import logger
import typer
import yaml
from config.yaml_loader import load_yaml_with_includes
import importlib  # for reflection
from typing_extensions import Annotated
import json
from collections import Counter, defaultdict
from typing import Union, List, Optional

# ── NEW: import the checkpoint manager ──────────────────────────────────────
from checkpoint_manager import CheckpointManager, prompt_resume_or_restart
# ────────────────────────────────────────────────────────────────────────────


app = typer.Typer()


# Load species configuration from YAML
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
    except yaml.YAMLError as e:
        logger.error(f"Error parsing species config file: {config_path}")
        logger.error(e)
        raise typer.Exit(1)


# Function to choose the writer class based on user input
def get_writer(writer_type: str, output_dir: Path, schema_config_path: Path):
    if writer_type.lower() == 'metta':
        return MeTTaWriter(schema_config=str(schema_config_path),
                           biocypher_config="config/biocypher_config.yaml",
                           output_dir=output_dir)
    elif writer_type.lower() == 'prolog':
        return PrologWriter(schema_config=str(schema_config_path), 
                            biocypher_config="config/biocypher_config.yaml",
                            output_dir=output_dir)
    elif writer_type.lower() == 'neo4j':
        return Neo4jCSVWriter(schema_config=str(schema_config_path), 
                               biocypher_config="config/biocypher_config.yaml",
                               output_dir=output_dir)
    elif writer_type.lower() == 'parquet':
        return ParquetWriter(
            schema_config=str(schema_config_path), 
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir,
            buffer_size=10000,
            overwrite=True
        )
    elif writer_type.lower() == 'kgx':
        return KGXWriter(schema_config=str(schema_config_path),
                          biocypher_config="config/biocypher_config.yaml",
                          output_dir=output_dir)
    elif writer_type.lower() == 'networkx':
        return NetworkXWriter(
            schema_config=str(schema_config_path),
            biocypher_config="config/biocypher_config.yaml",
            output_dir=output_dir
        )
    else:
        raise ValueError(f"Unknown writer type: {writer_type}")

def preprocess_schema(schema_config_path: Path):
    def convert_input_labels(label, replace_char="_"):
        if isinstance(label, list):
            return [item.replace(" ", replace_char) for item in label]
        return label.replace(" ", replace_char)

    bcy = BioCypher(
        schema_config_path=str(schema_config_path),
        biocypher_config_path="config/biocypher_config.yaml"
    )
    schema = bcy._get_ontology_mapping()._extend_schema()
    edge_node_types = {}

    for k, v in schema.items():
        if v.get('abstract', False) or v.get('represented_as') != 'edge':
            continue

        source_type = v.get("source", None)
        target_type = v.get("target", None)

        if source_type is not None and target_type is not None:
            input_label = v["input_label"]
            if isinstance(input_label, list):
                label = convert_input_labels(input_label[0])
            else:
                label = convert_input_labels(input_label)

            if isinstance(source_type, list):
                processed_source = [convert_input_labels(s).lower() for s in source_type]
            else:
                processed_source = convert_input_labels(source_type).lower()

            if isinstance(target_type, list):
                processed_target = [convert_input_labels(t).lower() for t in target_type]
            else:
                processed_target = convert_input_labels(target_type).lower()

            output_label = v.get("output_label", None)
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
        'node_count': sum(nodes_count.values()),
        'edge_count': sum(edges_count.values()),
        'dataset_count': 0,
        'data_size': '',
        'top_entities': [{'name': node, 'count': count} for node, count in nodes_count.items()],
        'top_connections': [],
        'frequent_relationships': [],
        'schema': {'nodes': [], 'edges': []},
        'datasets': []
    }

    predicate_count = Counter()
    relations_frequency = Counter()
    possible_connections = defaultdict(set)

    for edge_key, count in edges_count.items():
        parts = edge_key.split('|')
        if len(parts) == 3:
            edge_type, source_type, target_type = parts
        else:
            edge_type = edge_key
            if edge_type.lower() in schema_dict:
                source_type = schema_dict[edge_type.lower()]['source']
                target_type = schema_dict[edge_type.lower()]['target']
            else:
                continue

        if edge_type.lower() in schema_dict:
            label = schema_dict[edge_type.lower()]['output_label'] or edge_type
            predicate_count[label] += count

            if isinstance(source_type, list):
                for src in source_type:
                    relations_frequency[f'{src}|{target_type}'] += count
                    possible_connections[f'{src}|{target_type}'].add(label)
            elif isinstance(target_type, list):
                for tgt in target_type:
                    relations_frequency[f'{source_type}|{tgt}'] += count
                    possible_connections[f'{source_type}|{tgt}'].add(label)
            else:
                relations_frequency[f'{source_type}|{target_type}'] += count
                possible_connections[f'{source_type}|{target_type}'].add(label)

    graph_info['top_connections'] = [{'name': predicate, 'count': count} for predicate, count in predicate_count.items()]
    graph_info['frequent_relationships'] = [{'entities': rel.split('|'), 'count': count} for rel, count in relations_frequency.items()]

    for node, props in nodes_props.items():
        graph_info['schema']['nodes'].append({'data': {'name': node, 'properties': list(props)}})

    for conn, pos_connections in possible_connections.items():
        source, target = conn.split('|')
        graph_info['schema']['edges'].append({'data': {'source': source, 'target': target, 'possible_connections': list(pos_connections)}})

    total_size = sum(file.stat().st_size for file in Path(output_dir).rglob('*') if file.is_file())
    total_size_gb = total_size / (1024 ** 3)  # 1GB == 1024^3
    graph_info['data_size'] = f"{total_size_gb:.2f} GB"

    return graph_info


# ── MODIFIED: process_adapters now accepts and updates a CheckpointManager ──
def process_adapters(
    adapters_dict,
    dbsnp_rsids_dict,
    dbsnp_pos_dict,
    writer,
    write_properties,
    add_provenance,
    schema_dict,
    checkpoint_manager: Optional[CheckpointManager] = None,
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
    # ------------------------------------------------------------------
    # Restore accumulators from a previous partial run (if any)
    # ------------------------------------------------------------------
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

    completed_adapters: list = list(
        checkpoint_manager.completed_adapters if checkpoint_manager else []
    )

    for c in adapters_dict:
        # ── Skip already-completed adapters ─────────────────────────
        if c in completed_adapters:
            logger.info(f"Skipping adapter (already completed): {c}")
            continue

        writer.clear_counts()
        logger.info(f"Running adapter: {c}")

        adapter_config = adapters_dict[c]["adapter"]
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
        write_nodes = adapters_dict[c]["nodes"]
        write_edges = adapters_dict[c]["edges"]
        outdir = adapters_dict[c]["outdir"]

        dataset_name = getattr(adapter, 'source', None)
        version = getattr(adapter, 'version', None)
        source_url = getattr(adapter, 'source_url', None)

        if dataset_name is None:
            logger.warning(
                f"Dataset name is None for adapter: {c}. "
                "Ensure 'source' is defined in the adapter constructor."
            )
        else:
            if dataset_name not in datasets_dict:
                datasets_dict[dataset_name] = {
                    "name": dataset_name,
                    "version": version,
                    "url": source_url,
                    "nodes": set(),
                    "edges": set(),
                    "imported_on": str(date.today())
                }

        try:
            if write_nodes:
                nodes = adapter.get_nodes()
                freq, props = writer.write_nodes(nodes, path_prefix=outdir)
                for node_label in freq:
                    nodes_count[node_label] += freq[node_label]
                    if dataset_name is not None:
                        datasets_dict[dataset_name]['nodes'].add(node_label)
                for node_label in props:
                    nodes_props[node_label] = nodes_props[node_label].union(props[node_label])

            if write_edges:
                edges = adapter.get_edges()
                freq = writer.write_edges(edges, path_prefix=outdir)
                for edge_label_key in freq:
                    edges_count[edge_label_key] += freq[edge_label_key]

                    parts = edge_label_key.split('|')
                    edge_type = parts[0]

                    if edge_type.lower() in schema_dict:
                        output_label = schema_dict[edge_type.lower()]['output_label'] or edge_type
                    else:
                        output_label = edge_type

                    if dataset_name is not None:
                        datasets_dict[dataset_name]['edges'].add(output_label)

        except Exception as exc:
            logger.error(f"Adapter '{c}' failed: {exc}")
            # ── Save checkpoint with the failed adapter name ─────────
            if checkpoint_manager is not None:
                checkpoint_manager.save(
                    completed_adapters=completed_adapters,
                    nodes_count=nodes_count,
                    nodes_props=nodes_props,
                    edges_count=edges_count,
                    datasets_dict=datasets_dict,
                    failed_adapter=c,
                )
                logger.info(
                    f"Checkpoint saved. Re-run the pipeline to resume from adapter '{c}'."
                )
            raise  # re-raise so the caller can handle / exit

        # ── Mark adapter as completed and save checkpoint ────────────
        completed_adapters.append(c)
        if checkpoint_manager is not None:
            checkpoint_manager.save(
                completed_adapters=completed_adapters,
                nodes_count=nodes_count,
                nodes_props=nodes_props,
                edges_count=edges_count,
                datasets_dict=datasets_dict,
                failed_adapter=None,
            )
            logger.info(f"Checkpoint updated after adapter: {c}")

    return nodes_count, nodes_props, edges_count, datasets_dict
# ────────────────────────────────────────────────────────────────────────────


def _write_graph_info(
    nodes_count, nodes_props, edges_count, schema_dict, output_dir, datasets_dict
):
    """Build and write graph_info.json — extracted to avoid repetition."""
    graph_info = gather_graph_info(
        nodes_count, nodes_props, edges_count, schema_dict, output_dir
    )
    for ds in datasets_dict:
        datasets_dict[ds]["nodes"] = list(datasets_dict[ds]["nodes"])
        datasets_dict[ds]["edges"] = list(datasets_dict[ds]["edges"])
        graph_info['datasets'].append(datasets_dict[ds])

    graph_info["dataset_count"] = len(graph_info['datasets'])

    file_path = Path(output_dir) / "graph_info.json"
    with open(file_path, "w") as f:
        json.dump(graph_info, f, indent=2)

    logger.info(f"graph_info.json written to {file_path}")
    return graph_info


def _load_dbsnp(cache_dir: str, is_sample: bool = False) -> tuple:
    """Load dbSNP mappings using DBSNPProcessor.

    Args:
        cache_dir: Path to directory containing dbsnp_mapping.pkl.
                   If empty string, returns empty dicts.
        is_sample: Whether this is a sample config. For full configs,
                   missing cache is treated as an error.

    Returns:
        Tuple of (rsid_to_pos_dict, pos_to_rsid_dict)
    """
    if not cache_dir:
        logger.info("No dbSNP cache directory specified, continuing without rsID mappings")
        return {}, {}

    cache_path = Path(cache_dir)

    if not cache_path.exists() or not cache_path.is_dir():
        if is_sample:
            logger.warning(f"dbSNP cache directory not found at {cache_path}, continuing without rsID mappings")
            return {}, {}
        else:
            logger.error("=" * 80)
            logger.error("ERROR: Full config requires server dbSNP cache directory")
            logger.error(f"Expected location: {cache_path}")
            logger.error("Directory not found!")
            logger.error("")
            logger.error("Solutions:")
            logger.error("  1. Run on the bizon server where cache exists")
            logger.error("  2. Use sample config instead: --dataset sample")
            logger.error("  3. Create cache by running: python update_dbsnp.py")
            logger.error("=" * 80)
            raise typer.Exit(1)

    mapping_file = cache_path / 'dbsnp_mapping.pkl'
    if not mapping_file.exists():
        if is_sample:
            logger.warning(f"dbSNP mapping file not found at {mapping_file}, continuing without rsID mappings")
            return {}, {}
        else:
            logger.error("=" * 80)
            logger.error(f"ERROR: dbSNP mapping file not found at {mapping_file}")
            logger.error("")
            logger.error("Solutions:")
            logger.error("  1. If cache doesn't exist, run: python update_dbsnp.py")
            logger.error("  2. Use sample config instead: --dataset sample")
            logger.error("=" * 80)
            raise typer.Exit(1)

    try:
        dbsnp_proc = DBSNPProcessor(cache_dir=str(cache_path))
        dbsnp_proc.load_mapping()
        rsids_dict, pos_dict = dbsnp_proc.get_dict_wrappers()
        logger.info(f"Loaded {len(rsids_dict):,} rsID mappings from {cache_path}")
        return rsids_dict, pos_dict
    except Exception as e:
        if is_sample:
            logger.warning(f"Failed to load dbSNP mappings from {cache_path}: {e}")
            return {}, {}
        else:
            logger.error("=" * 80)
            logger.error(f"ERROR: Failed to load dbSNP mappings: {e}")
            logger.error("")
            logger.error("Solutions:")
            logger.error("  1. If cache doesn't exist, run: python update_dbsnp.py")
            logger.error("  2. Use sample config instead: --dataset sample")
            logger.error("=" * 80)
            raise typer.Exit(1)


# Run build
@app.command()
def main(
    # Species selection options
    species: Optional[str] = typer.Option(
        None,
        help="Species to generate KG for: hsa, dmel, cel, mmo, rno, or 'all'"
    ),
    dataset: str = typer.Option(
        "full",
        help="Dataset size: 'sample' or 'full' (default: full)"
    ),
    species_config_path: str = typer.Option(
        "config/species_config.yaml",
        help="Path to species configuration YAML file"
    ),

    # Output directory
    output_dir: Optional[Path] = typer.Option(
        None,
        file_okay=False,
        dir_okay=True,
        help="Output directory (required)"
    ),

    # Manual mode only parameters
    adapters_config: Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Adapters config path (manual mode only)"
    ),
    dbsnp_cache_dir: Optional[str] = typer.Option(
        None,
        help="dbSNP cache directory containing dbsnp_mapping.pkl (manual mode only, optional - defaults to aux_files/hsa/sample_dbsnp)"
    ),
    schema_config: Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Schema config path (manual mode only)"
    ),

    # Common options
    writer_type: str = typer.Option(default="metta", help="Choose writer type: metta, prolog, neo4j, parquet, networkx, KGX"),
    write_properties: bool = typer.Option(True, help="Write properties to nodes and edges"),
    add_provenance: bool = typer.Option(True, help="Add provenance to nodes and edges"),
    buffer_size: int = typer.Option(10000, help="Buffer size for Parquet writer"),
    overwrite: bool = typer.Option(True, help="Overwrite existing Parquet files"),
    include_adapters: Optional[List[str]] = typer.Option(
        None,
        help="Specific adapters to include (space-separated, default: all)",
        case_sensitive=False,
    ),

    # ── NEW: checkpoint options ─────────────────────────────────────────
    no_checkpoint: bool = typer.Option(
        False,
        "--no-checkpoint",
        help="Disable checkpointing entirely (always start fresh, never write checkpoint)."
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
    # ────────────────────────────────────────────────────────────────────
):
    """
    Main function. Call individual adapters to download and process data. Build
    via BioCypher from node and edge data.

    Two modes:
    1. Species mode: Use --species, --dataset, and --output-dir flags
       Example: --species hsa --dataset sample --output-dir output_hsa

    2. Manual mode: Specify all paths manually (backward compatible)
       Example: --output-dir output_neo4j --adapters-config config/...

    Checkpointing
    -------------
    After each adapter completes, a checkpoint is saved to
    <output_dir>/kg_checkpoint.json.  If the run is interrupted you can
    re-run the exact same command and choose to resume or start over.

    Flags:
      --no-checkpoint   Disable checkpointing (original behaviour).
      --resume          Resume automatically without prompting.
      --restart         Delete any checkpoint and start over without prompting.
    """

    # Determine which mode we're in
    manual_mode = all([adapters_config, schema_config])
    species_mode = species is not None

    if not manual_mode and not species_mode:
        logger.error("You must either:")
        logger.error("  1. Use --species flag with --output-dir (e.g., --species hsa --dataset sample --output-dir output_hsa)")
        logger.error("  2. Provide all manual parameters (--output-dir, --adapters-config, --schema-config)")
        raise typer.Exit(1)

    if output_dir is None:
        logger.error("--output-dir is required")
        raise typer.Exit(1)

    # ── Species mode ────────────────────────────────────────────────────
    if species_mode:
        SPECIES_CONFIG = load_species_config(species_config_path)

        if species.lower() == 'all':
            logger.info("Generating KG for all species")
            logger.info(f"Base output directory: {output_dir}")
            available_species = list(SPECIES_CONFIG.keys())

            for sp in available_species:
                if dataset not in SPECIES_CONFIG[sp]:
                    logger.warning(f"Dataset '{dataset}' not available for species '{sp}', skipping...")
                    continue

                sp_output_dir = output_dir / sp
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing {sp} - {dataset}")
                logger.info(f"Output: {sp_output_dir}")
                logger.info(f"{'='*60}\n")

                sp_output_dir.mkdir(parents=True, exist_ok=True)
                config = SPECIES_CONFIG[sp][dataset]

                sp_adapters_config = Path(config['adapters_config'])
                sp_schema_config = Path(config['schema_config'])
                sp_is_sample = (dataset == 'sample')
                sp_dbsnp_cache_dir = config.get('dbsnp_cache_dir', '')
                if not sp_dbsnp_cache_dir:
                    if sp_is_sample:
                        sp_dbsnp_cache_dir = 'aux_files/hsa/sample_dbsnp'
                    else:
                        sp_dbsnp_cache_dir = '/mnt/hdd_2/kedist/rsids_map'

                # Load dbSNP mappings via DBSNPProcessor
                sp_dbsnp_rsids_dict, sp_dbsnp_pos_dict = _load_dbsnp(sp_dbsnp_cache_dir, is_sample=sp_is_sample)

                bc = get_writer(writer_type, sp_output_dir, sp_schema_config)
                logger.info(f"Using {writer_type} writer for {sp}")

                if writer_type == 'parquet':
                    bc.buffer_size = buffer_size
                    bc.overwrite = overwrite

                schema_dict = preprocess_schema(sp_schema_config)

                with open(sp_adapters_config, "r") as fp:
                    try:
                        sp_adapters_dict = load_yaml_with_includes(fp)
                    except yaml.YAMLError as e:
                        logger.error(f"Error loading adapter config for {sp}")
                        logger.error(e)
                        continue

                if include_adapters:
                    original_count = len(sp_adapters_dict)
                    include_lower = [a.lower() for a in include_adapters]
                    sp_adapters_dict = {
                        k: v for k, v in sp_adapters_dict.items()
                        if k.lower() in include_lower
                    }
                    if not sp_adapters_dict:
                        logger.error(f"No matching adapters found for {sp}.")
                        continue
                    logger.info(f"Filtered to {len(sp_adapters_dict)}/{original_count} adapters for {sp}")

                # ── Checkpoint setup per-species ─────────────────────
                ckpt = _setup_checkpoint(
                    sp_output_dir,
                    pipeline_id=f"{sp_output_dir}::{sp_adapters_config}",
                    no_checkpoint=no_checkpoint,
                    resume=resume,
                )

                nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
                    sp_adapters_dict, sp_dbsnp_rsids_dict, sp_dbsnp_pos_dict, bc,
                    write_properties, add_provenance, schema_dict,
                    checkpoint_manager=ckpt,
                )

                if writer_type == 'networkx':
                    bc.write_graph()
                    logger.info(f"NetworkX graph saved for {sp}")

                if hasattr(bc, 'finalize'):
                    bc.finalize()

                _write_graph_info(
                    nodes_count, nodes_props, edges_count,
                    schema_dict, sp_output_dir, datasets_dict
                )

                # ── Delete checkpoint after successful completion ─────
                if ckpt is not None:
                    ckpt.delete()

                logger.info(f"Done with {sp}")
                logger.info(f"Total nodes processed for {sp}: {sum(nodes_count.values())}")
                logger.info(f"Total edges processed for {sp}: {sum(edges_count.values())}")

            logger.info("\n" + "=" * 60)
            logger.info("All species processed successfully!")
            logger.info("=" * 60)
            return

        else:
            # Single species
            if species not in SPECIES_CONFIG:
                logger.error(f"Unknown species: {species}")
                logger.error(f"Available: {', '.join(SPECIES_CONFIG.keys())}")
                raise typer.Exit(1)

            if dataset not in SPECIES_CONFIG[species]:
                logger.error(f"Dataset '{dataset}' not available for species '{species}'")
                logger.error(f"Available datasets: {', '.join(SPECIES_CONFIG[species].keys())}")
                raise typer.Exit(1)

            config = SPECIES_CONFIG[species][dataset]
            logger.info(f"Generating KG for {species} using {dataset} dataset")
            logger.info(f"Output directory: {output_dir}")
            logger.info(f"Write properties: {write_properties}")
            logger.info(f"Add provenance: {add_provenance}")

            output_dir.mkdir(parents=True, exist_ok=True)

            adapters_config = Path(config['adapters_config'])
            schema_config = Path(config['schema_config'])
            dbsnp_cache_dir = config.get('dbsnp_cache_dir', '')

    # Load dbSNP mappings via DBSNPProcessor
    # Determine sample vs full, and resolve dbsnp_cache_dir if not set
    if not species_mode:
        is_sample_config = 'sample' in str(adapters_config).lower()
    else:
        is_sample_config = (dataset == 'sample')

    if not dbsnp_cache_dir:
        if is_sample_config:
            dbsnp_cache_dir = 'aux_files/hsa/sample_dbsnp'
        else:
            # Full config: use server cache
            dbsnp_cache_dir = '/mnt/hdd_2/kedist/rsids_map'
    dbsnp_rsids_dict, dbsnp_pos_dict = _load_dbsnp(dbsnp_cache_dir, is_sample=is_sample_config)

    bc = get_writer(writer_type, output_dir, schema_config)
    logger.info(f"Using {writer_type} writer")

    if writer_type == 'parquet':
        bc.buffer_size = buffer_size
        bc.overwrite = overwrite

    schema_dict = preprocess_schema(schema_config)

    with open(adapters_config, "r") as fp:
        try:
            adapters_dict = load_yaml_with_includes(fp)
        except yaml.YAMLError as e:
            logger.error("Error while trying to load adapter config")
            logger.error(e)

    if include_adapters:
        original_count = len(adapters_dict)
        include_lower = [a.lower() for a in include_adapters]
        adapters_dict = {
            k: v for k, v in adapters_dict.items()
            if k.lower() in include_lower
        }
        if not adapters_dict:
            available = "\n".join(f" - {a}" for a in adapters_dict.keys())
            logger.error(f"No matching adapters found. Available adapters:\n{available}")
            raise typer.Exit(1)

        logger.info(f"Filtered to {len(adapters_dict)}/{original_count} adapters")

    # ── Checkpoint setup ─────────────────────────────────────────────────
    output_dir.mkdir(parents=True, exist_ok=True)
    ckpt = _setup_checkpoint(
        output_dir,
        pipeline_id=f"{output_dir}::{adapters_config}",
        no_checkpoint=no_checkpoint,
        resume=resume,
    )
    # ────────────────────────────────────────────────────────────────────

    nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
        adapters_dict, dbsnp_rsids_dict, dbsnp_pos_dict, bc,
        write_properties, add_provenance, schema_dict,
        checkpoint_manager=ckpt,
    )

    if writer_type == 'networkx':
        bc.write_graph()
        logger.info("NetworkX graph saved successfully")

    if hasattr(bc, 'finalize'):
        bc.finalize()

    _write_graph_info(
        nodes_count, nodes_props, edges_count,
        schema_dict, output_dir, datasets_dict
    )

    # ── Delete checkpoint after successful completion ────────────────────
    if ckpt is not None:
        ckpt.delete()
    # ────────────────────────────────────────────────────────────────────

    logger.info("Done")
    logger.info(f"Total nodes processed: {sum(nodes_count.values())}")
    logger.info(f"Total edges processed: {sum(edges_count.values())}")


# ── Helper: create and configure a CheckpointManager ────────────────────────
def _setup_checkpoint(
    output_dir: Path,
    pipeline_id: str,
    no_checkpoint: bool,
    resume: Optional[bool],
) -> Optional[CheckpointManager]:
    """
    Return a configured CheckpointManager, or None if checkpointing is disabled.

    Decision matrix
    ---------------
    no_checkpoint=True          → return None (no checkpointing at all)
    no_checkpoint=False + no existing checkpoint → create fresh CheckpointManager
    no_checkpoint=False + existing checkpoint:
        resume=True  → load and return checkpoint
        resume=False → delete checkpoint, return fresh CheckpointManager
        resume=None  → ask the user interactively
    """
    if no_checkpoint:
        logger.info("Checkpointing disabled (--no-checkpoint).")
        return None

    ckpt = CheckpointManager(output_dir=output_dir, pipeline_id=pipeline_id)

    if not ckpt.exists():
        logger.info("No existing checkpoint — starting fresh.")
        return ckpt

    # A checkpoint exists — resolve resume/restart
    loaded = ckpt.load()
    if not loaded:
        # Checkpoint was stale / unreadable; start fresh
        return ckpt

    if resume is True:
        logger.info("--resume flag set: resuming from checkpoint.")
        return ckpt

    if resume is False:
        logger.info("--restart flag set: deleting checkpoint and starting over.")
        ckpt.delete()
        return CheckpointManager(output_dir=output_dir, pipeline_id=pipeline_id)

    # resume is None → interactive prompt
    should_resume = prompt_resume_or_restart(ckpt)
    if should_resume:
        return ckpt
    # User chose restart — checkpoint already deleted by the prompt helper
    return CheckpointManager(output_dir=output_dir, pipeline_id=pipeline_id)
# ────────────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    app()