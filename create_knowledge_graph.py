"""
Knowledge graph generation through BioCypher script
"""
from datetime import date
from pathlib import Path
from biocypher_metta.metta_writer import *
from biocypher_metta.prolog_writer import PrologWriter
from biocypher_metta.neo4j_csv_writer import *
from biocypher._logger import logger
import typer
import yaml
import importlib  #for reflection
from typing_extensions import Annotated
import pickle
import json
from collections import Counter, defaultdict

app = typer.Typer()

# Function to choose the writer class based on user input
def get_writer(writer_type: str, output_dir: Path):
    if writer_type == 'metta':
        return MeTTaWriter(schema_config="config/schema_config.yaml",
                           biocypher_config="config/biocypher_config.yaml",
                           output_dir=output_dir)
    elif writer_type == 'prolog':
        return PrologWriter(schema_config="config/schema_config.yaml",
                            biocypher_config="config/biocypher_config.yaml",
                            output_dir=output_dir)
    elif writer_type == 'neo4j':
        return Neo4jCSVWriter(schema_config="config/schema_config.yaml",
                               biocypher_config="config/biocypher_config.yaml",
                               output_dir=output_dir)
    else:
        raise ValueError(f"Unknown writer type: {writer_type}")

def preprocess_schema():
    def convert_input_labels(label, replace_char="_"):
        return label.replace(" ", replace_char)

    bcy = BioCypher(
        schema_config_path="config/schema_config.yaml", biocypher_config_path="config/biocypher_config.yaml"
    )
    schema = bcy._get_ontology_mapping()._extend_schema()
    edge_node_types = {}

    for k, v in schema.items():
        if v["represented_as"] == "edge":
            source_type = v.get("source", None)
            target_type = v.get("target", None)

            if source_type is not None and target_type is not None:
                label = convert_input_labels(v["input_label"])
                source_type = convert_input_labels(source_type)
                target_type = convert_input_labels(target_type)
                output_label = v.get("output_label", None)

                edge_node_types[label.lower()] = {
                    "source": source_type.lower(),
                    "target": target_type.lower(),
                    "output_label": output_label.lower() if output_label else None,
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

    for edge, count in edges_count.items():
        label = schema_dict[edge]['output_label'] or edge
        predicate_count[label] += count
        source = schema_dict[edge]['source']
        target = schema_dict[edge]['target']
        relations_frequency[f'{source}|{target}'] += count
        possible_connections[f'{source}|{target}'].add(label)

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

# Run build
@app.command()
def main(output_dir: Annotated[Path, typer.Option(exists=True, file_okay=False, dir_okay=True)],
         adapters_config: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
         dbsnp_rsids: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
         dbsnp_pos: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
         writer_type: str = typer.Option(default="metta", help="Choose writer type: metta, prolog, neo4j"),
         write_properties: bool = typer.Option(True, help="Write properties to nodes and edges"),
         add_provenance: bool = typer.Option(True, help="Add provenance to nodes and edges")):
    """
    Main function. Call individual adapters to download and process data. Build
    via BioCypher from node and edge data.
    """

    # Start biocypher
    logger.info("Loading dbsnp rsids map")
    dbsnp_rsids_dict = pickle.load(open(dbsnp_rsids, 'rb'))
    logger.info("Loading dbsnp pos map")
    dbsnp_pos_dict = pickle.load(open(dbsnp_pos, 'rb'))

    # Choose the writer based on user input or default to 'metta'
    bc = get_writer(writer_type, output_dir)
    logger.info(f"Using {writer_type} writer")

    schema_dict = preprocess_schema()

    # Run adapters
    with open(adapters_config, "r") as fp:
        try:
            adapters_dict = yaml.safe_load(fp)
        except yaml.YAMLError as e:
            logger.error("Error while trying to load adapter config")
            logger.error(e)

    nodes_count = Counter()
    nodes_props = defaultdict(set)
    edges_count = Counter()
    datasets_dict = {}

    for c in adapters_dict:
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
            freq, props = bc.write_nodes(nodes, path_prefix=outdir)
            for node_label in freq:
                nodes_count[node_label] += freq[node_label]
                datasets_dict[dataset_name]['nodes'].add(node_label)
            for node_label in props:
                nodes_props[node_label] = nodes_props[node_label].union(props[node_label])

        if write_edges:
            edges = adapter.get_edges()
            freq = bc.write_edges(edges, path_prefix=outdir)
            for edge_label in freq:
                edges_count[edge_label] += freq[edge_label]
                label = schema_dict[edge_label]['output_label'] or edge_label
                datasets_dict[dataset_name]['edges'].add(label)

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

if __name__ == "__main__":
    app()
