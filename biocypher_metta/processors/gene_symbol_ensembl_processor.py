"""
Gene Symbol → Ensembl Gene ID Processor (via BioMart).

Maps current gene symbols *and their synonyms* to Ensembl gene IDs.
Supports both the vertebrate Ensembl BioMart and the Metazoa BioMart.

Data sources:
- Vertebrate: https://www.ensembl.org/biomart/martservice
- Metazoa:    https://metazoa.ensembl.org/biomart/martservice

Example usage:
    mmu = GeneSymbolEnsemblProcessor(
        dataset='mmusculus_gene_ensembl',
        cache_dir='aux_files/mmu/gene_symbol_ensembl',
    )
    mmu.get_ensembl_gene('Brca1')   # → 'ENSMUSG00000017537'

    cel = GeneSymbolEnsemblProcessor(
        dataset='celegans_eg_gene',
        cache_dir='aux_files/cel/gene_symbol_ensembl',
        biomart_url='https://metazoa.ensembl.org/biomart/martservice',
        virtual_schema='metazoa_mart',
    )
    cel.get_ensembl_gene('unc-22')  # → 'WBGene00006920'
"""

import requests
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from biocypher._logger import logger
from .base_mapping_processor import BaseMappingProcessor


VERTEBRATE_BIOMART_URL = "https://www.ensembl.org/biomart/martservice"
METAZOA_BIOMART_URL    = "https://metazoa.ensembl.org/biomart/martservice"

_QUERY_XML_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query virtualSchemaName="{virtual_schema}" formatter="TSV" header="1" uniqueRows="0" count="" datasetConfigVersion="0.6">
    <Dataset name="{dataset}" interface="default">
        <Attribute name="ensembl_gene_id" />
        <Attribute name="external_gene_name" />
        <Attribute name="external_synonym" />
    </Dataset>
</Query>"""

# Ready-made configs for supported species.
# taxon_id → (dataset, biomart_url, virtual_schema, species_code)
GENE_SYMBOL_CONFIGS: Dict[int, tuple] = {
    10090: ('mmusculus_gene_ensembl',   VERTEBRATE_BIOMART_URL, 'default',      'mmu'),
    10116: ('rnorvegicus_gene_ensembl', VERTEBRATE_BIOMART_URL, 'default',      'rno'),
    6239:  ('celegans_eg_gene',         METAZOA_BIOMART_URL,    'metazoa_mart', 'cel'),
    7227:  ('dmelanogaster_eg_gene',    METAZOA_BIOMART_URL,    'metazoa_mart', 'dmel'),
}


class GeneSymbolEnsemblProcessor(BaseMappingProcessor):

    def __init__(
        self,
        dataset: str,
        cache_dir: str,
        biomart_url: str = VERTEBRATE_BIOMART_URL,
        virtual_schema: str = 'default',
        update_interval_hours: int = 168,
    ):
        """
        :param dataset:               BioMart dataset, e.g. 'mmusculus_gene_ensembl'.
        :param cache_dir:             Directory for the pickle cache.
        :param biomart_url:           BioMart service URL.
        :param virtual_schema:        BioMart virtual schema name ('default' or 'metazoa_mart').
        :param update_interval_hours: Refresh interval (default 7 days).
        """
        self.dataset = dataset
        self.biomart_url = biomart_url
        self._query_xml = _QUERY_XML_TEMPLATE.format(
            virtual_schema=virtual_schema,
            dataset=dataset,
        )
        name = f"gene_symbol_{dataset.replace('_gene_ensembl', '').replace('_eg_gene', '')}"
        super().__init__(
            name=name,
            cache_dir=cache_dir,
            update_interval_hours=update_interval_hours,
        )

    @classmethod
    def for_taxon(cls, taxon_id: int, cache_base_dir: str = 'aux_files') -> 'GeneSymbolEnsemblProcessor':
        """Convenience constructor using the built-in GENE_SYMBOL_CONFIGS registry."""
        if taxon_id not in GENE_SYMBOL_CONFIGS:
            raise ValueError(
                f"No GeneSymbolEnsemblProcessor config for taxon_id {taxon_id}. "
                f"Supported: {list(GENE_SYMBOL_CONFIGS)}"
            )
        dataset, biomart_url, virtual_schema, sp_code = GENE_SYMBOL_CONFIGS[taxon_id]
        return cls(
            dataset=dataset,
            cache_dir=f"{cache_base_dir}/{sp_code}/gene_symbol_ensembl",
            biomart_url=biomart_url,
            virtual_schema=virtual_schema,
        )

    # BioMart is dynamic; HEAD requests don't yield useful metadata.
    def get_remote_urls(self) -> None:
        return None

    def check_update_needed(self) -> bool:
        if not self.mapping_file.exists() or not self.version_file.exists():
            logger.info(f"{self.name}: Cache not found. Update needed.")
            return True

        version_info = self._load_version_info()
        if not version_info or 'timestamp' not in version_info:
            logger.warning(f"{self.name}: Invalid version file. Update needed.")
            return True

        if self.update_interval:
            last_update = datetime.fromisoformat(version_info['timestamp'])
            age = datetime.now() - last_update
            if age <= self.update_interval:
                if version_info.get('entries', -1) == 0:
                    logger.warning(
                        f"{self.name}: Cached mapping is empty (0 entries). Forcing re-download."
                    )
                    return True
                logger.info(
                    f"{self.name}: Using cached mapping "
                    f"(age: {age.days}d {age.seconds // 3600}h; "
                    f"refresh window: {self.update_interval.days}d)."
                )
                return False

        return super().check_update_needed()

    def fetch_data(self) -> str:
        logger.info(f"{self.name}: Querying BioMart ({self.dataset})...")

        with tempfile.NamedTemporaryFile(suffix='.tsv', delete=False) as tmp:
            temp_file = Path(tmp.name)
        try:
            response = requests.get(
                self.biomart_url,
                params={'query': self._query_xml},
                timeout=(30, 900),
                stream=True,
            )
            response.raise_for_status()

            total_bytes = 0
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        total_bytes += len(chunk)

            logger.info(f"{self.name}: Download complete ({total_bytes // 1024} KB)")

            with open(temp_file, 'r', encoding='utf-8') as f:
                data = f.read()

            if data.strip().startswith('<!DOCTYPE') or data.strip().startswith('<html'):
                raise ValueError(
                    f"{self.name}: BioMart returned an HTML error page — "
                    f"check dataset='{self.dataset}'"
                )
            if 'Query ERROR' in data[:300]:
                raise ValueError(f"{self.name}: BioMart query error: {data[:500]}")

            return data
        finally:
            if temp_file.exists():
                temp_file.unlink()

    def process_data(self, raw_data: str) -> Dict[str, Any]:
        logger.info(f"{self.name}: Parsing BioMart TSV response...")

        symbol_to_ensembl: Dict[str, str] = {}

        header_seen = False
        row_count = 0
        rows = []

        for line in raw_data.split('\n'):
            line = line.strip()
            if not line:
                continue

            fields = line.split('\t')

            if not header_seen:
                header_seen = True
                continue

            if len(fields) < 2:
                continue

            ensembl_gene = fields[0]
            gene_name    = fields[1] if len(fields) > 1 else ''
            synonym      = fields[2] if len(fields) > 2 else ''

            if not ensembl_gene:
                continue

            row_count += 1
            rows.append((ensembl_gene, gene_name, synonym))

        # Two passes so a current symbol always wins over a synonym on collision,
        # regardless of which row the BioMart dump happens to list first: synonyms
        # are filled in with setdefault (first-wins, no better tiebreaker exists),
        # then every current gene_name is applied with a direct overwrite.
        for ensembl_gene, gene_name, synonym in rows:
            if synonym:
                symbol_to_ensembl.setdefault(synonym, ensembl_gene)
        for ensembl_gene, gene_name, synonym in rows:
            if gene_name:
                symbol_to_ensembl[gene_name] = ensembl_gene

        logger.info(f"{self.name}: Parsed {row_count:,} BioMart rows")
        logger.info(f"{self.name}: symbol→Ensembl gene: {len(symbol_to_ensembl):,} entries")

        return {'symbol_to_ensembl_gene': symbol_to_ensembl}

    def get_ensembl_gene(self, symbol: str) -> Optional[str]:
        """Return the Ensembl gene ID for a gene symbol (current name or synonym)."""
        if not self.mapping:
            self.load_or_update()
        return self.mapping.get('symbol_to_ensembl_gene', {}).get(symbol)
