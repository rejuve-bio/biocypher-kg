import sys
import typer
import logging
from pathlib import Path
from typing_extensions import Annotated
from biocypher_dataset_downloader.download_manager import DownloadManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = typer.Typer()

REPO_ROOT = Path(__file__).resolve().parent.parent


def _generate_connected_sample(config_file: str, output_dir: Path, size_budget: int):
    """Run scripts/generate_connected_sample.py against this download's output_dir,
    writing the filtered connected sample to samples/<species>/. Species is derived
    from config_file's parent directory (config/<species>/<species>_..._config.yaml).
    No-op (with a log message) for species that script doesn't support yet.
    """
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import generate_connected_sample as gcs

    species = Path(config_file).parent.name
    if species not in gcs.SPECIES:
        logging.info(
            f"generate_connected_sample.py has no config for species '{species}' yet — "
            f"skipping connected-sample generation."
        )
        return

    sample_output_dir = REPO_ROOT / "samples" / species
    anchor_genes_file = gcs.SPECIES[species]["anchor_genes_file"]
    logging.info(f"Generating connected sample for '{species}' -> {sample_output_dir} ...")
    gcs.generate_sample(
        species,
        input_dir=str(output_dir),
        output_dir=str(sample_output_dir),
        size_budget=size_budget,
        anchor_genes_file=anchor_genes_file,
        interactive=False,
    )
    logging.info("Connected sample generation complete.")


@app.command()
def download_data(
    output_dir: Annotated[Path, typer.Option(exists=False, file_okay=False, dir_okay=True)],
    config_file: str = "config/hsa/hsa_data_source_config.yaml",
    source: str = None,
    sample_fraction: float = 0.01,
    no_checksum: Annotated[bool, typer.Option("--no-checksum", help="Skip sha256 checksums in the provenance manifest (faster on huge files).")] = False,
    sample_size_budget: Annotated[int, typer.Option(help="Node budget passed to generate_connected_sample.py.")] = 180,
    skip_sample_generation: Annotated[bool, typer.Option("--skip-sample-generation", help="Don't run generate_connected_sample.py after downloading.")] = False,
):
    """Download data sources defined in a species config YAML.

    Writes a provenance manifest (download_manifest.json) and append-only version
    history (versions.json) under the output directory. After a full download (no
    --source), automatically runs scripts/generate_connected_sample.py against
    output_dir to (re)build the connected KG sample under samples/<species>/, for
    species that script already supports.

    Examples:
        python download_data.py --output-dir data/hsa
        python download_data.py --output-dir data/dmel --config-file config/dmel/dmel_data_source_config.yaml
        python download_data.py --output-dir data/hsa --source reactome
        python download_data.py --output-dir data/hsa --no-checksum
        python download_data.py --output-dir data/dmel --config-file config/dmel/dmel_data_source_config.yaml --skip-sample-generation
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

    if source or skip_sample_generation:
        return

    try:
        _generate_connected_sample(config_file, output_dir, sample_size_budget)
    except Exception as e:
        logging.error(f"Connected-sample generation failed (download itself succeeded): {e}")


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