import typer
from pathlib import Path
from typing_extensions import Annotated
from downloader import DownloadManager
import logging

logging.basicConfig(level=logging.INFO)
app = typer.Typer()

@app.command()
def download_data(
    output_dir: Annotated[Path, typer.Option(exists=False, file_okay=False, dir_okay=True)],
    config_file: str = "config/data_source_config.yaml",
    source: str = None
):
    """Download data sources"""
    try:
        manager = DownloadManager(config_file, output_dir)
        if source:
            manager.download_source(source)
        else:
            manager.download_all()
    except Exception as e:
        logging.error(f"Download failed: {str(e)}")
        raise

if __name__ == "__main__":
    app()