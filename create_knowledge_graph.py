"""
Knowledge graph generation through BioCypher script with enhanced adapter selection
"""
from typing import Union, List
from datetime import date
from pathlib import Path
from typing import List, Optional
from collections import Counter, defaultdict
import importlib
import json
import pickle
import yaml

from biocypher import BioCypher
from biocypher._logger import logger
from biocypher_metta.metta_writer import *
from biocypher_metta.prolog_writer import PrologWriter
from biocypher_metta.neo4j_csv_writer import *
import typer
from typing_extensions import Annotated

app = typer.Typer()

def get_writer(writer_type: str, output_dir: Path):
    """Choose the writer class based on user input"""
    writers = {
        'metta': MeTTaWriter,
        'prolog': PrologWriter,
        'neo4j': Neo4jCSVWriter
    }
    if writer_type not in writers:
        raise ValueError(f"Unknown writer type: {writer_type}. Choose from: {list(writers.keys())}")
    
    return writers[writer_type](
        schema_config="config/schema_config.yaml",
        biocypher_config="config/biocypher_config.yaml",
        output_dir=output_dir
    )

def preprocess_schema():
    """Process schema to extract edge-node relationships"""
    def normalize_label(label: Union[str, List[str]], replace_char: str = "_") -> Union[str, List[str]]:
        """Normalizes label(s) to lowercase with replaced spaces."""
        if isinstance(label, list):
            return [item.replace(" ", replace_char).lower() for item in label]
        return label.replace(" ", replace_char).lower()

    bcy = BioCypher(
        schema_config_path="config/schema_config.yaml",
        biocypher_config_path="config/biocypher_config.yaml"
    )
    schema = bcy._get_ontology_mapping()._extend_schema()
    edge_node_types = {}

    for k, v in schema.items():
        if v["represented_as"] == "edge":
            source = v.get("source")
            target = v.get("target")
            if source and target:
                edge_node_types[normalize_label(v["input_label"])] = {
                    "source": normalize_label(source),
                    "target": normalize_label(target),
                    "output_label": normalize_label(v["output_label"]) if v.get("output_label") else None,
                }

    return edge_node_types

def gather_graph_info(nodes_count, nodes_props, edges_count, schema_dict, output_dir):
    """Collect and format graph statistics"""
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

    # Process edge statistics
    predicate_count = Counter()
    relations_frequency = Counter()
    possible_connections = defaultdict(set)

    for edge, count in edges_count.items():
        label = schema_dict[edge]['output_label'] or edge
        predicate_count[label] += count
        source = schema_dict[edge]['source']
        target = schema_dict[edge]['target']
        relations_frequency[f'{source}|{target}'] += count
        possible_connections[f'{source}|{target}'].add(label)

    # Populate graph info
    graph_info['top_connections'] = [{'name': p, 'count': c} for p, c in predicate_count.items()]
    graph_info['frequent_relationships'] = [
        {'entities': rel.split('|'), 'count': count} 
        for rel, count in relations_frequency.items()
    ]

    for node, props in nodes_props.items():
        graph_info['schema']['nodes'].append({
            'data': {'name': node, 'properties': list(props)}
        })

    for conn, pos_connections in possible_connections.items():
        source, target = conn.split('|')
        graph_info['schema']['edges'].append({
            'data': {
                'source': source,
                'target': target,
                'possible_connections': list(pos_connections)
            }
        })

    # Calculate output size
    total_size = sum(f.stat().st_size for f in Path(output_dir).rglob('*') if f.is_file())
    graph_info['data_size'] = f"{total_size / (1024 ** 3):.2f} GB"

    return graph_info

def process_adapters(adapters_dict, dbsnp_rsids_dict, dbsnp_pos_dict, writer, 
                    write_properties, add_provenance, schema_dict):
    """Process selected adapters and generate graph data"""
    nodes_count = Counter()
    nodes_props = defaultdict(set)
    edges_count = Counter()
    datasets_dict = {}

    for adapter_name, config in adapters_dict.items():
        writer.clear_counts()
        logger.info(f"Processing adapter: {adapter_name}")

        # Initialize adapter
        adapter_config = config["adapter"]
        adapter_module = importlib.import_module(adapter_config["module"])
        adapter_cls = getattr(adapter_module, adapter_config["cls"])
        
        # Prepare constructor args
        ctr_args = adapter_config["args"].copy()
        if "dbsnp_rsid_map" in ctr_args:
            ctr_args["dbsnp_rsid_map"] = dbsnp_rsids_dict
        if "dbsnp_pos_map" in ctr_args:
            ctr_args["dbsnp_pos_map"] = dbsnp_pos_dict
        ctr_args.update({
            "write_properties": write_properties,
            "add_provenance": add_provenance
        })

        adapter = adapter_cls(**ctr_args)
        
        # Track dataset metadata
        dataset_name = getattr(adapter, 'source', adapter_name)
        if dataset_name not in datasets_dict:
            datasets_dict[dataset_name] = {
                "name": dataset_name,
                "version": getattr(adapter, 'version', "unknown"),
                "url": getattr(adapter, 'source_url', ""),
                "nodes": set(),
                "edges": set(),
                "imported_on": str(date.today())
            }

        # Process nodes if configured
        if config["nodes"]:
            nodes = adapter.get_nodes()
            freq, props = writer.write_nodes(nodes, path_prefix=config["outdir"])
            for label, count in freq.items():
                nodes_count[label] += count
                datasets_dict[dataset_name]['nodes'].add(label)
            for label, properties in props.items():
                nodes_props[label].update(properties)

        # Process edges if configured
        if config["edges"]:
            edges = adapter.get_edges()
            freq = writer.write_edges(edges, path_prefix=config["outdir"])
            for label, count in freq.items():
                edges_count[label] += count
                output_label = schema_dict[label]['output_label'] or label
                datasets_dict[dataset_name]['edges'].add(output_label)

    return nodes_count, nodes_props, edges_count, datasets_dict

@app.command()
def main(
    output_dir: Annotated[Path, typer.Option(exists=True, file_okay=False, dir_okay=True)],
    adapters_config: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
    dbsnp_rsids: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
    dbsnp_pos: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
    writer_type: str = typer.Option("metta", help="Output format: metta, prolog, or neo4j"),
    write_properties: bool = typer.Option(True, help="Include node/edge properties"),
    add_provenance: bool = typer.Option(True, help="Add provenance information"),
    include_adapters: Optional[List[str]] = typer.Option(
            None, 
            help="Specific adapters to include (space-separated, default: all)",
            case_sensitive=False,
        )
):
    """Generate knowledge graph from selected biological data adapters"""
    # Load required mappings
    logger.info("Loading dbSNP mappings")
    dbsnp_rsids_dict = pickle.load(open(dbsnp_rsids, 'rb'))
    dbsnp_pos_dict = pickle.load(open(dbsnp_pos, 'rb'))

    # Initialize writer and schema
    writer = get_writer(writer_type, output_dir)
    schema_dict = preprocess_schema()

    # Load and filter adapters
    with open(adapters_config, "r") as f:
        adapters_dict = yaml.safe_load(f)
        
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

    # Process adapters and generate output
    nodes_count, nodes_props, edges_count, datasets_dict = process_adapters(
        adapters_dict, dbsnp_rsids_dict, dbsnp_pos_dict, 
        writer, write_properties, add_provenance, schema_dict
    )

    # Generate and save summary
    graph_info = gather_graph_info(nodes_count, nodes_props, edges_count, schema_dict, output_dir)
    graph_info['datasets'] = [
        {**ds, 'nodes': list(ds['nodes']), 'edges': list(ds['edges'])} 
        for ds in datasets_dict.values()
    ]
    graph_info['dataset_count'] = len(graph_info['datasets'])

    with open(output_dir / "graph_info.json", "w") as f:
        json.dump(graph_info, f, indent=2)

    logger.info("Knowledge graph generation complete")

if __name__ == "__main__":
    app()
