import typer
import logging
from pathlib import Path
from typing_extensions import Annotated
from biocypher_dataset_downloader.download_manager import DownloadManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = typer.Typer()


@app.command()
def download_data(
    output_dir: Annotated[Path, typer.Option(exists=False, file_okay=False, dir_okay=True)],
    config_file: str = "config/hsa/hsa_data_source_config.yaml",
    source: str = None,
    sample_fraction: float = 0.01,
):
    """Download data sources defined in a species config YAML.

    Examples:
        python download_data.py --output-dir data/hsa
        python download_data.py --output-dir data/dmel --config-file config/dmel/dmel_data_source_config.yaml
        python download_data.py --output-dir data/hsa --source reactome
    """
    try:
        manager = DownloadManager(config_file, output_dir, sample_fraction=sample_fraction)
        if source:
            manager.download_source(source)
        else:
            manager.download_all()
    except Exception as e:
        logging.error(f"Download failed: {e}")
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