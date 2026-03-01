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
from biocypher._logger import logger
import typer
import yaml
from config.yaml_loader import load_yaml_with_includes
import importlib  # for reflection
from typing_extensions import Annotated
import pickle
import json
from collections import Counter, defaultdict
from typing import Union, List, Optional


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
# Added schema_config_path parameter
def get_writer(writer_type: str, output_dir: Path, schema_config_path: Path):
    if writer_type.lower() == 'metta':
        return MeTTaWriter(schema_config=str(schema_config_path), # Replaced hardcoded path
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
        return KGXWriter(
            schema_config=str(schema_config_path), 
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

#  Added schema_config_path parameter
def preprocess_schema(schema_config_path: Path):
    def convert_input_labels(label, replace_char="_"):
        if isinstance(label, list):
            return [item.replace(" ", replace_char) for item in label]
        return label.replace(" ", replace_char)

    bcy = BioCypher(
        schema_config_path=str(schema_config_path), # Replaced hardcoded path
        biocypher_config_path="config/biocypher_config.yaml"
    )
    schema = bcy._get_ontology_mapping()._extend_schema()
    edge_node_types = {}

    for k, v in schema.items():
        # Skip abstract types and non-edge types
        if v.get('abstract', False) or v.get('represented_as') != 'edge':
            continue
            
        source_type = v.get("source", None)
        target_type = v.get("target", None)

        if source_type is not None and target_type is not None:
            # Handle both single labels and lists of labels
            input_label = v["input_label"]
            if isinstance(input_label, list):
                label = convert_input_labels(input_label[0])
            else:
                label = convert_input_labels(input_label)
            
            # Handle source_type which can be a string or list
            if isinstance(source_type, list):
                processed_source = [convert_input_labels(s).lower() for s in source_type]
            else:
                processed_source = convert_input_labels(source_type).lower()
            
            # Handle target_type which can be a string or list  
            if isinstance(target_type, list):
                processed_target = [convert_input_labels(t).lower() for t in target_type]
            else:
                processed_target = convert_input_labels(target_type).lower()
            
            # Handle output_label
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


def process_adapters(adapters_dict, dbsnp_rsids_dict, dbsnp_pos_dict, writer, write_properties, add_provenance, schema_dict):
    nodes_count = Counter()
    nodes_props = defaultdict(set)
    edges_count = Counter()
    datasets_dict = {}

    for c in adapters_dict:
        writer.clear_counts() # Reset counter for this adapter
        logger.info(f"Running adapter: {c}")
        adapter_config = adapters_dict[c]["adapter"]
        adapter_module = importlib.import_module(adapter_config["module"])
        adapter_cls = getattr(adapter_module, adapter_config["cls"])
        ctr_args = adapter_config["args"]

        if "dbsnp_rsid_map" in ctr_args: #this for dbs that use grch37 assembly and to map grch37 to grch38
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
            logger.warning(f"Dataset name is None for adapter: {c}. Ensure 'source' is defined in the adapter constructor.")
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

    return nodes_count, nodes_props, edges_count, datasets_dict

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
    dbsnp_rsids: Optional[Path] = typer.Option(
        None,
        exists=True, 
        file_okay=True, 
        dir_okay=False,
        help="dbSNP rsids file (manual mode only)"
    ),
    dbsnp_pos: Optional[Path] = typer.Option(
        None,
        exists=True, 
        file_okay=True, 
        dir_okay=False,
        help="dbSNP positions file (manual mode only)"
    ),
    schema_config: Optional[Path] = typer.Option(
        None,
        exists=True, 
        file_okay=True, 
        dir_okay=False,
        help="Schema config path (manual mode only)"
    ),
    
    # Common options
         writer_type: str = typer.Option(default="metta", help="Choose writer type: metta, prolog, neo4j, parquet, networkx,KGX"),
         write_properties: bool = typer.Option(True, help="Write properties to nodes and edges"),
         add_provenance: bool = typer.Option(True, help="Add provenance to nodes and edges"),
         buffer_size: int = typer.Option(10000, help="Buffer size for Parquet writer"),
         overwrite: bool = typer.Option(True, help="Overwrite existing Parquet files"),
         include_adapters: Optional[List[str]] = typer.Option(
              None,
              help="Specific adapters to include (space-separated, default: all)",
              case_sensitive=False,
          )):
    """
    Main function. Call individual adapters to download and process data. Build
    via BioCypher from node and edge data.
    Two modes:
    1. Species mode: Use --species, --dataset, and --output-dir flags
       Example: --species hsa --dataset sample --output-dir output_hsa
    
    2. Manual mode: Specify all paths manually (backward compatible)
       Example: --output-dir output_neo4j --adapters-config config/...
    """
    
    # Determine which mode we're in
    manual_mode = all([adapters_config, dbsnp_rsids, dbsnp_pos, schema_config])
    species_mode = species is not None
    
    if not manual_mode and not species_mode:
        logger.error("You must either:")
        logger.error("  1. Use --species flag with --output-dir (e.g., --species hsa --dataset sample --output-dir output_hsa)")
        logger.error("  2. Provide all manual parameters (--output-dir, --adapters-config, --dbsnp-rsids, --dbsnp-pos, --schema-config)")
        raise typer.Exit(1)
    
    # Validate that output_dir is provided
    if output_dir is None:
        logger.error("--output-dir is required")
        raise typer.Exit(1)
    
    # Handle species mode
    if species_mode:
        # Load species configuration
        SPECIES_CONFIG = load_species_config(species_config_path)
        
        if species.lower() == 'all':
            logger.info("Generating KG for all species")
            logger.info(f"Base output directory: {output_dir}")
            logger.info("Species-specific subdirectories will be created")
            available_species = list(SPECIES_CONFIG.keys())
            
            for sp in available_species:
                if dataset not in SPECIES_CONFIG[sp]:
                    logger.warning(f"Dataset '{dataset}' not available for species '{sp}', skipping...")
                    continue
                
                # Create species-specific subdirectory
                sp_output_dir = output_dir / sp
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing {sp} - {dataset}")
                logger.info(f"Output: {sp_output_dir}")
                logger.info(f"{'='*60}\n")
                
                sp_output_dir.mkdir(parents=True, exist_ok=True)
                
                config = SPECIES_CONFIG[sp][dataset]
                
                # Convert config paths to Path objects
                sp_adapters_config = Path(config['adapters_config'])
                sp_dbsnp_rsids = Path(config['dbsnp_rsids'])
                sp_dbsnp_pos = Path(config['dbsnp_pos'])
                sp_schema_config = Path(config['schema_config'])
                
                # Process this species
                logger.info(f"Loading dbsnp rsids map for {sp}")
                sp_dbsnp_rsids_dict = pickle.load(open(sp_dbsnp_rsids, 'rb'))
                logger.info(f"Loading dbsnp pos map for {sp}")
                # sp_dbsnp_pos_dict = pickle.load(open(sp_dbsnp_pos, 'rb'))
                sp_dbsnp_pos_dict = sp_dbsnp_rsids_dict
                
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
                
                # Filter adapters if specific ones are requested
                if include_adapters:
                    original_count = len(sp_adapters_dict)
                    include_lower = [a.lower() for a in include_adapters]
                    sp_adapters_dict = {
                        k: v for k, v in sp_adapters_dict.items()
                        if k.lower() in include_lower
                    }
                    if not sp_adapters_dict:
                        available = "\n".join(f" - {a}" for a in sp_adapters_dict.keys())
                        logger.error(f"No matching adapters found for {sp}. Available adapters:\n{available}")
                        continue
                    logger.info(f"Filtered to {len(sp_adapters_dict)}/{original_count} adapters for {sp}")
                
                # Run adapters for this species
                nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
                    sp_adapters_dict, sp_dbsnp_rsids_dict, sp_dbsnp_pos_dict, bc, 
                    write_properties, add_provenance, schema_dict
                )
                
                if writer_type == 'networkx':
                    bc.write_graph()
                    logger.info(f"NetworkX graph saved for {sp}")
                
                if hasattr(bc, 'finalize'):
                    bc.finalize()
                
                # Gather graph info for this species
                graph_info = gather_graph_info(nodes_count, nodes_props, edges_count, schema_dict, sp_output_dir)
                
                for dataset_name in datasets_dict:
                    datasets_dict[dataset_name]["nodes"] = list(datasets_dict[dataset_name]["nodes"])
                    datasets_dict[dataset_name]["edges"] = list(datasets_dict[dataset_name]["edges"])
                    graph_info['datasets'].append(datasets_dict[dataset_name])
                
                graph_info["dataset_count"] = len(graph_info['datasets'])
                
                # Write graph info to JSON
                graph_info_json = json.dumps(graph_info, indent=2)
                file_path = f"{sp_output_dir}/graph_info.json"
                with open(file_path, "w") as f:
                    f.write(graph_info_json)
                
                logger.info(f"Done with {sp}")
                logger.info(f"Total nodes processed for {sp}: {sum(nodes_count.values())}")
                logger.info(f"Total edges processed for {sp}: {sum(edges_count.values())}")
            
            logger.info("\n" + "="*60)
            logger.info("All species processed successfully!")
            logger.info("="*60)
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
            
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Convert config paths to Path objects
            adapters_config = Path(config['adapters_config'])
            dbsnp_rsids = Path(config['dbsnp_rsids'])
            dbsnp_pos = Path(config['dbsnp_pos'])
            schema_config = Path(config['schema_config'])
    
    # run the actual processing (same for both manual and species mode)
    logger.info("Loading dbsnp rsids map")
    dbsnp_rsids_dict = pickle.load(open(dbsnp_rsids, 'rb'))
    logger.info("Loading dbsnp pos map")
    dbsnp_pos_dict = pickle.load(open(dbsnp_pos, 'rb'))

    # Choose the writer based on user input or default to 'metta'
    #  Passed schema_config to get_writer
    bc = get_writer(writer_type, output_dir, schema_config)
    logger.info(f"Using {writer_type} writer")

    if writer_type == 'parquet':
        bc.buffer_size = buffer_size
        bc.overwrite = overwrite

    #  Passed schema_config to preprocess_schema
    schema_dict = preprocess_schema(schema_config)

    with open(adapters_config, "r") as fp:
        try:
            adapters_dict = load_yaml_with_includes(fp)
        except yaml.YAMLError as e:
            logger.error("Error while trying to load adapter config")
            logger.error(e)

    # Filter adapters if specific ones are requested
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
    # Run adapters
    nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
        adapters_dict, dbsnp_rsids_dict, dbsnp_pos_dict, bc, write_properties, add_provenance, schema_dict
    )

    # For NetworkX writer, save the graph after processing all adapters
    if writer_type == 'networkx':
        bc.write_graph()
        logger.info("NetworkX graph saved successfully")

    if hasattr(bc, 'finalize'):
        bc.finalize()

    # Gather graph info
    graph_info = gather_graph_info(nodes_count, nodes_props, edges_count, schema_dict, output_dir)

    for dataset in datasets_dict:
        datasets_dict[dataset]["nodes"] = list(datasets_dict[dataset]["nodes"])
        datasets_dict[dataset]["edges"] = list(datasets_dict[dataset]["edges"])
        graph_info['datasets'].append(datasets_dict[dataset])

    graph_info["dataset_count"] = len(graph_info['datasets'])

    # Write the graph info to JSON
    graph_info_json = json.dumps(graph_info, indent=2)
    file_path = f"{output_dir}/graph_info.json"
    with open(file_path, "w") as f:
        f.write(graph_info_json)

    logger.info("Done")
    logger.info(f"Total nodes processed: {sum(nodes_count.values())}")
    logger.info(f"Total edges processed: {sum(edges_count.values())}")

if __name__ == "__main__":
    app()