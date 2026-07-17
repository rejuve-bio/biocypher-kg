import typer
import logging
from pathlib import Path
from typing_extensions import Annotated
from biocypher_dataset_downloader.download_manager import DownloadManager
from biocypher_dataset_downloader.reactome_exporter import export_reactome_reactions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = typer.Typer()


@app.command()
def download_data(
    output_dir: Annotated[Path, typer.Option(exists=False, file_okay=False, dir_okay=True)],
    config_file: str = "config/hsa/hsa_data_source_config.yaml",
    source: str = None,
    sample_fraction: float = 0.01,
    no_checksum: Annotated[bool, typer.Option("--no-checksum", help="Skip sha256 checksums in the provenance manifest (faster on huge files).")] = False,
):
    """Download data sources defined in a species config YAML.

    Writes a provenance manifest (download_manifest.json) and append-only version
    history (versions.json) under the output directory.

    Examples:
        python download_data.py --output-dir data/hsa
        python download_data.py --output-dir data/dmel --config-file config/dmel/dmel_data_source_config.yaml
        python download_data.py --output-dir data/hsa --source reactome
        python download_data.py --output-dir data/hsa --no-checksum
    """
    try:
        manager = DownloadManager(config_file, output_dir, sample_fraction=sample_fraction,
                                  compute_checksums=not no_checksum)
        if source:
            manager.download_source(source)
        else:
            manager.download_all()
    except Exception as e:
        logging.error(f"Download failed: {e}")
        raise


@app.command()
def export_reactome(
    output_dir: Annotated[Path, typer.Option(dir_okay=True)],
    neo4j_uri: str = typer.Option("bolt://localhost:7687", help="Neo4j URI"),
    neo4j_user: str = typer.Option("neo4j", help="Neo4j username"),
    neo4j_password: str = typer.Option("neo4j", help="Neo4j password"),
    ref_db: str = typer.Option("UniProt", help="Reference database display name")
):
    """
    Connect to a Reactome Neo4j graph database and generate the reactome_reaction_exporter_All_species.txt file.
    """
    output_file = output_dir / "reactome_reaction_exporter_All_species.txt"
    try:
        export_reactome_reactions(output_file, neo4j_uri, neo4j_user, neo4j_password, ref_db)
    except Exception as e:
        logging.error(f"Reactome export failed: {e}")
        raise


if __name__ == "__main__":
    app()

# import typer
# from pathlib import Path
# from typing_extensions import Annotated
# from biocypher_dataset_downloader.download_manager import DownloadManager
# import logging

# logging.basicConfig(level=logging.INFO)
# app = typer.Typer()

# @app.command()
# def download_data(
#     output_dir: Annotated[Path, typer.Option(exists=False, file_okay=False, dir_okay=True)],
#     config_file: str = "config/hsa/hsa_data_source_config.yaml",
#     source: str = None
# ):
#     """Download data sources"""
#     try:
#         manager = DownloadManager(config_file, output_dir)
#         if source:
#             manager.download_source(source)
#         else:
#             manager.download_all()
#     except Exception as e:
#         logging.error(f"Download failed: {str(e)}")
#         raise

# if __name__ == "__main__":
#     app()