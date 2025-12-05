"""
Entrez to Ensembl Gene ID Processor.

Maintains mappings between NCBI Entrez Gene IDs and Ensembl Gene IDs.

Data sources:
- NCBI Gene Info: https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
- GENCODE: https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/

Update strategy: Time-based (every 7 days, as these databases update less frequently)
"""

import requests
import gzip
import re
import tempfile
from pathlib import Path
from typing import Dict, Any
from .base_mapping_processor import BaseMappingProcessor


class EntrezEnsemblProcessor(BaseMappingProcessor):

    NCBI_GENE_INFO_URL = (
        "https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz"
    )

    GENCODE_URL = (
        "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_46/"
        "gencode.v46.chr_patch_hapl_scaff.annotation.gtf.gz"
    )

    def __init__(
        self,
        cache_dir: str = 'aux_files/entrez_ensembl',
        update_interval_hours: int = 168
    ):
        super().__init__(
            name='entrez_ensembl',
            cache_dir=cache_dir,
            update_interval_hours=update_interval_hours
        )

    def get_remote_urls(self):
        return [self.NCBI_GENE_INFO_URL, self.GENCODE_URL]

    def fetch_data(self) -> Dict[str, Any]:
        temp_dir = Path(tempfile.mkdtemp())

        print(f"{self.name}: Fetching NCBI Gene Info...")
        gene_info_path = temp_dir / "gene_info.gz"

        response = requests.get(self.NCBI_GENE_INFO_URL, timeout=(30, 600), stream=True)
        response.raise_for_status()

        downloaded = 0
        chunk_size = 1024 * 1024
        with open(gene_info_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    mb_downloaded = downloaded // (1024 * 1024)
                    if mb_downloaded > 0 and downloaded % (1024 * 1024) < chunk_size:
                        print(f"{self.name}: Downloaded {mb_downloaded} MB...")

        print(f"{self.name}: NCBI Gene Info downloaded successfully")

        print(f"{self.name}: Fetching GENCODE annotations (large file, ~60MB compressed)...")
        gencode_path = temp_dir / "gencode.gtf.gz"

        response = requests.get(self.GENCODE_URL, timeout=(30, 900), stream=True)
        response.raise_for_status()

        downloaded = 0
        with open(gencode_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    mb_downloaded = downloaded // (1024 * 1024)
                    if mb_downloaded > 0 and mb_downloaded % 10 == 0 and downloaded % (10 * 1024 * 1024) < chunk_size:
                        print(f"{self.name}: Downloaded {mb_downloaded} MB...")

        print(f"{self.name}: GENCODE annotations downloaded successfully ({downloaded // (1024 * 1024)} MB)")

        return {
            'gene_info_path': str(gene_info_path),
            'gencode_path': str(gencode_path),
            'temp_dir': str(temp_dir)
        }

    def process_data(self, raw_data: Dict[str, Any]) -> Dict[str, str]:
        gene_info_path = Path(raw_data['gene_info_path'])
        gencode_path = Path(raw_data['gencode_path'])
        temp_dir = Path(raw_data['temp_dir'])

        try:
            print(f"{self.name}: Parsing NCBI Gene Info (streaming)...")
            entrez_to_symbol = {}

            with gzip.open(gene_info_path, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.startswith('#') or not line.strip():
                        continue

                    if line_num % 10000 == 0:
                        print(f"{self.name}: Processed {line_num:,} lines from Gene Info...")

                    fields = line.split('\t')
                    if len(fields) < 11:
                        continue

                    tax_id = fields[0]
                    if tax_id != '9606':
                        continue

                    entrez_id = fields[1]
                    symbol = fields[2]
                    hgnc_symbol = fields[10] if len(fields) > 10 and fields[10] != '-' else symbol

                    if hgnc_symbol and hgnc_symbol != '-':
                        entrez_to_symbol[entrez_id] = hgnc_symbol

            print(f"{self.name}: Found {len(entrez_to_symbol)} Entrez-HGNC mappings")

            print(f"{self.name}: Parsing GENCODE annotations (streaming, this may take a few minutes)...")
            symbol_to_ensembl = {}

            with gzip.open(gencode_path, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.startswith('#') or not line.strip():
                        continue

                    if line_num % 100000 == 0:
                        print(f"{self.name}: Processed {line_num:,} lines from GENCODE...")

                    fields = line.split('\t')
                    if len(fields) < 9:
                        continue

                    feature_type = fields[2]
                    if feature_type != 'gene':
                        continue

                    attributes = fields[8]

                    ensembl_match = re.search(r'gene_id "([^"]+)"', attributes)
                    if not ensembl_match:
                        continue
                    ensembl_id = ensembl_match.group(1).split('.')[0]

                    gene_name_match = re.search(r'gene_name "([^"]+)"', attributes)
                    if not gene_name_match:
                        continue
                    gene_name = gene_name_match.group(1)

                    symbol_to_ensembl[gene_name] = ensembl_id

            print(f"{self.name}: Found {len(symbol_to_ensembl)} HGNC-Ensembl mappings")

            print(f"{self.name}: Creating Entrez-Ensembl mappings...")
            entrez_to_ensembl = {}

            for entrez_id, symbol in entrez_to_symbol.items():
                if symbol in symbol_to_ensembl:
                    ensembl_id = symbol_to_ensembl[symbol]
                    entrez_to_ensembl[entrez_id] = ensembl_id

            print(f"{self.name}: Created {len(entrez_to_ensembl)} Entrez-Ensembl mappings")

            return entrez_to_ensembl

        finally:
            print(f"{self.name}: Cleaning up temporary files...")
            if gene_info_path.exists():
                gene_info_path.unlink()
            if gencode_path.exists():
                gencode_path.unlink()
            if temp_dir.exists():
                temp_dir.rmdir()

    def get_ensembl_id(self, entrez_id: str) -> str:
        if not self.mapping:
            self.load_or_update()

        return self.mapping.get(entrez_id)

    def get_entrez_id(self, ensembl_id: str) -> str:
        if not self.mapping:
            self.load_or_update()

        base_ensembl = ensembl_id.split('.')[0]

        for entrez_id, ens_id in self.mapping.items():
            if ens_id == base_ensembl:
                return entrez_id

        return None
