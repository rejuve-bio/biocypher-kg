"""
Knowledge graph generation through BioCypher script
"""
from biocypher_metta.metta_writer import *
from biocypher_metta.prolog_writer import PrologWriter
from biocypher_metta.neo4j_csv_writer import *
from biocypher._logger import logger
import typer
import yaml
import importlib #for reflection
from typing_extensions import Annotated
import pickle

app = typer.Typer()

# Function to choose the writer class based on user input
def get_writer(writer_type: str, output_dir: pathlib.Path):
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
                if isinstance(v["input_label"], list):
                    label = convert_input_labels(v["input_label"][0])
                    source_type = convert_input_labels(source_type[0])
                    target_type = convert_input_labels(target_type[0])
                else:
                    label = convert_input_labels(v["input_label"])
                    source_type = convert_input_labels(source_type)
                    target_type = convert_input_labels(target_type)
                output_label = v.get("output_label", None)

                edge_node_types[label.lower()] = {
                    "source": source_type.lower(),
                    "target": target_type.lower(),
                    "output_label": (
                        output_label.lower() if output_label is not None else None
                    ),
                }
    
    return edge_node_types

# Run build
@app.command()
def main(output_dir: Annotated[pathlib.Path, typer.Option(exists=True, file_okay=False, dir_okay=True)],
         adapters_config: Annotated[pathlib.Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
         dbsnp_rsids: Annotated[pathlib.Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
         dbsnp_pos: Annotated[pathlib.Path, typer.Option(exists=True, file_okay=True, dir_okay=False)],
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

    # bc.show_ontology_structure()
    schema_dict = preprocess_schema()

    # Run adapters

    with open(adapters_config, "r") as fp:
        try:
            adapters_dict = yaml.safe_load(fp)
        except yaml.YAMLError as e:
            logger.error(f"Error while trying to load adapter config")
            logger.error(e)

    nodes_count = Counter()
    nodes_props = defaultdict(set)
    edges_count = Counter()
    predicate_count = Counter()
    relations_frequency = Counter()
    possible_connections = defaultdict(set)
    schema_json = {'nodes': [], 'edges': []}
    graph_info = {'node_count': 0, 
                  'edge_count': 0,
                  'top_entities': [],
                  'top_connections': [],
                  'frequent_relationships': []}
    
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

        if write_nodes:
            nodes = adapter.get_nodes()
            freq, props = bc.write_nodes(nodes, path_prefix=outdir)
            for node_label in freq:
                nodes_count[node_label] += freq[node_label]
            for node_label in props:
                nodes_props[node_label] = nodes_props[node_label].union(props[node_label])

        if write_edges:
            edges = adapter.get_edges()
            freq = bc.write_edges(edges, path_prefix=outdir)
            for edge_label in freq:
                edges_count[edge_label] += freq[edge_label]
    
    
    for node, count in nodes_count.items():
        graph_info['node_count'] += count
        graph_info['top_entities'].append({'name': node, 'count': count})
        
    
    for edge, count in edges_count.items():
        graph_info['edge_count'] += count
        label = schema_dict[edge]['output_label'] or edge
        source = schema_dict[edge]['source']
        target = schema_dict[edge]['target']
        predicate_count[label] += count
        relations_frequency[f'{source}|{target}'] += count
        possible_connections[f'{source}|{target}'].add(label)
    
    for predicate, count in predicate_count.items():
        graph_info['top_connections'].append({'name': predicate, 'count': count})
    
    for rel, count in relations_frequency.items():
        s, t = rel.split('|')
        graph_info['frequent_relationships'].append({'entities':[s, t], 'count': count})
    
    
    for node, props in nodes_props.items():
        schema_json['nodes'].append({'data': {'name': node, 'properties': list(props)}})
    
    for conn, pos_connections in possible_connections.items():
        source, target = conn.split('|')
        schema_json['edges'].append({'data': {'source': source, 'target': target, 'possible_connections': list(pos_connections)}})
    
    graph_info['schema'] = schema_json
    graph_info_json = json.dumps(graph_info, indent=2)
    file_path = f"{output_dir}/graph_info.json"
    with open(file_path, "w") as f:
        f.write(graph_info_json)


    logger.info("Done")

if __name__ == "__main__":
    app()