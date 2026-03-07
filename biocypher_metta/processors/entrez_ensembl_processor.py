"""
Entrez to Ensembl Gene ID Processor.

Maintains mappings between NCBI Entrez Gene IDs and Ensembl Gene IDs,
plus gene alias dictionaries (symbol, synonyms, HGNC, etc.).

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
from typing import Dict, Any, List, Optional
from biocypher._logger import logger
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
        cache_dir: str = 'aux_files/hsa/entrez_ensembl',
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

        logger.info(f"{self.name}: Fetching NCBI Gene Info...")
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
                        logger.info(f"{self.name}: Downloaded {mb_downloaded} MB...")

        logger.info(f"{self.name}: NCBI Gene Info downloaded successfully")

        logger.info(f"{self.name}: Fetching GENCODE annotations (large file, ~60MB compressed)...")
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
                        logger.info(f"{self.name}: Downloaded {mb_downloaded} MB...")

        logger.info(f"{self.name}: GENCODE annotations downloaded successfully ({downloaded // (1024 * 1024)} MB)")

        return {
            'gene_info_path': str(gene_info_path),
            'gencode_path': str(gencode_path),
            'temp_dir': str(temp_dir)
        }

    def process_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        gene_info_path = Path(raw_data['gene_info_path'])
        gencode_path = Path(raw_data['gencode_path'])
        temp_dir = Path(raw_data['temp_dir'])

        try:
            logger.info(f"{self.name}: Parsing NCBI Gene Info (streaming)...")
            entrez_to_symbol = {}
            gene_aliases = {}

            with gzip.open(gene_info_path, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.startswith('#') or not line.strip():
                        continue

                    if line_num % 10000 == 0:
                        logger.info(f"{self.name}: Processed {line_num:,} lines from Gene Info...")

                    fields = line.split('\t')
                    if len(fields) < 16:
                        continue

                    tax_id = fields[0]
                    if tax_id != '9606':
                        continue

                    entrez_id = fields[1]
                    symbol = fields[2]
                    synonyms = fields[4]
                    dbxrefs = fields[5]
                    symbol_from_nomenclature = fields[10] if fields[10] != '-' else symbol
                    full_name = fields[11]
                    other_designations = fields[13]

                    if symbol_from_nomenclature and symbol_from_nomenclature != '-':
                        entrez_to_symbol[entrez_id] = symbol_from_nomenclature

                    # Build gene aliases (same logic as gencode_gene_adapter.get_gene_alias)
                    split_dbxrefs = dbxrefs.split('|')
                    hgnc = ''
                    ensembl = ''
                    for ref in split_dbxrefs:
                        if ref.startswith('HGNC:'):
                            hgnc = ref[5:]
                        if ref.startswith('Ensembl:'):
                            ensembl = ref[8:]

                    if ensembl or hgnc:
                        complete_synonyms = [symbol]
                        for s in synonyms.split('|'):
                            complete_synonyms.append(s)
                        if hgnc:
                            complete_synonyms.append(hgnc)
                        for s in other_designations.split('|'):
                            complete_synonyms.append(s)
                        complete_synonyms.append(symbol_from_nomenclature)
                        complete_synonyms.append(full_name)
                        complete_synonyms = list(set(complete_synonyms))
                        if '-' in complete_synonyms:
                            complete_synonyms.remove('-')
                        if ensembl:
                            gene_aliases[ensembl] = complete_synonyms
                        if hgnc:
                            gene_aliases[hgnc] = complete_synonyms

            logger.info(f"{self.name}: Found {len(entrez_to_symbol)} Entrez-HGNC mappings")
            logger.info(f"{self.name}: Built {len(gene_aliases)} gene alias entries")

            logger.info(f"{self.name}: Parsing GENCODE annotations (streaming, this may take a few minutes)...")
            symbol_to_ensembl = {}

            with gzip.open(gencode_path, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.startswith('#') or not line.strip():
                        continue

                    if line_num % 100000 == 0:
                        logger.info(f"{self.name}: Processed {line_num:,} lines from GENCODE...")

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

            logger.info(f"{self.name}: Found {len(symbol_to_ensembl)} HGNC-Ensembl mappings")

            logger.info(f"{self.name}: Creating Entrez-Ensembl mappings...")
            entrez_to_ensembl = {}

            for entrez_id, symbol in entrez_to_symbol.items():
                if symbol in symbol_to_ensembl:
                    ensembl_id = symbol_to_ensembl[symbol]
                    entrez_to_ensembl[entrez_id] = ensembl_id

            logger.info(f"{self.name}: Created {len(entrez_to_ensembl)} Entrez-Ensembl mappings")

            return {
                'entrez_to_ensembl': entrez_to_ensembl,
                'gene_aliases': gene_aliases
            }

        finally:
            logger.info(f"{self.name}: Cleaning up temporary files...")
            if gene_info_path.exists():
                gene_info_path.unlink()
            if gencode_path.exists():
                gencode_path.unlink()
            if temp_dir.exists():
                temp_dir.rmdir()

    def _is_nested_format(self) -> bool:
        """Check if mapping uses the new nested format with sub-dicts."""
        return (isinstance(self.mapping, dict)
                and 'entrez_to_ensembl' in self.mapping
                and isinstance(self.mapping['entrez_to_ensembl'], dict))

    @property
    def entrez_to_ensembl(self) -> Dict[str, str]:
        """Entrez→Ensembl mapping dict. Handles both old flat and new nested formats."""
        if not self.mapping:
            self.load_or_update()
        if self._is_nested_format():
            return self.mapping['entrez_to_ensembl']
        # Old flat format (legacy cache) — force re-fetch
        logger.info(f"{self.name}: Detected legacy flat mapping format, re-fetching...")
        self.update_mapping(force=True)
        return self.mapping['entrez_to_ensembl']

    @property
    def gene_aliases(self) -> Dict[str, List[str]]:
        """Gene alias dict keyed by Ensembl ID and HGNC ID."""
        if not self.mapping:
            self.load_or_update()
        if self._is_nested_format():
            return self.mapping.get('gene_aliases', {})
        # Old flat format — force re-fetch
        logger.info(f"{self.name}: Detected legacy flat mapping format, re-fetching...")
        self.update_mapping(force=True)
        return self.mapping.get('gene_aliases', {})

    def get_gene_aliases(self, identifier: str) -> Optional[List[str]]:
        """Look up aliases by Ensembl ID or HGNC ID."""
        return self.gene_aliases.get(identifier)

    def get_ensembl_id(self, entrez_id: str) -> Optional[str]:
        if not self.mapping:
            self.load_or_update()
        return self.entrez_to_ensembl.get(entrez_id)

    def get_entrez_id(self, ensembl_id: str) -> Optional[str]:
        if not self.mapping:
            self.load_or_update()

        base_ensembl = ensembl_id.split('.')[0]
        mapping = self.entrez_to_ensembl

        for entrez_id, ens_id in mapping.items():
            if ens_id == base_ensembl:
                return entrez_id

        return None
