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

    # Run adapters

    with open(adapters_config, "r") as fp:
        try:
            adapters_dict = yaml.safe_load(fp)
        except yaml.YAMLError as e:
            logger.error(f"Error while trying to load adapter config")
            logger.error(e)

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
            bc.write_nodes(nodes, path_prefix=outdir)

        if write_edges:
            edges = adapter.get_edges()
            bc.write_edges(edges, path_prefix=outdir)


    logger.info("Done")

if __name__ == "__main__":
    app()