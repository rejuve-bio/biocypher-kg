"""
Generic BioMart → Ensembl ID Processor.

Maps species-specific gene identifiers to Ensembl IDs (genes, transcripts,
peptides) by querying Ensembl BioMart. Works for any species available in
BioMart; the caller supplies the three species-specific parameters.

Data source:
- Ensembl BioMart: https://www.ensembl.org/biomart/martservice

Update strategy: Time-based (every 7 days by default).

Example — Rattus norvegicus (RGD IDs):
    proc = BioMartEnsemblProcessor(
        dataset='rnorvegicus_gene_ensembl',
        species_id_attr='rgd',
        cache_dir='aux_files/rno/rgd_ensembl',
    )

Example — Mus musculus (MGI IDs):
    proc = BioMartEnsemblProcessor(
        dataset='mmusculus_gene_ensembl',
        species_id_attr='mgi_id',
        cache_dir='aux_files/mmu/mgi_ensembl',
    )
"""

import requests
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from biocypher._logger import logger
from .base_mapping_processor import BaseMappingProcessor


BIOMART_URL = "https://www.ensembl.org/biomart/martservice"

_QUERY_XML_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="0" count="" datasetConfigVersion="0.6">
    <Dataset name="{dataset}" interface="default">
        <Attribute name="ensembl_gene_id" />
        <Attribute name="ensembl_transcript_id" />
        <Attribute name="ensembl_peptide_id" />
        <Attribute name="external_gene_name" />
        <Attribute name="{species_id_attr}" />
    </Dataset>
</Query>"""


class BioMartEnsemblProcessor(BaseMappingProcessor):

    def __init__(
        self,
        dataset: str,
        species_id_attr: str,
        cache_dir: str,
        update_interval_hours: int = 168,
    ):
        """
        :param dataset:           Ensembl BioMart dataset name,
                                  e.g. 'rnorvegicus_gene_ensembl'.
        :param species_id_attr:   BioMart attribute name for the species-specific
                                  identifier, e.g. 'rgd' or 'mgi_id'.
        :param cache_dir:         Directory for the pickle cache,
                                  e.g. 'aux_files/rno/rgd_ensembl'.
        :param update_interval_hours: Refresh interval (default 7 days).
        """
        self.dataset = dataset
        self.species_id_attr = species_id_attr
        self._query_xml = _QUERY_XML_TEMPLATE.format(
            dataset=dataset,
            species_id_attr=species_id_attr,
        )
        name = f"biomart_{dataset.replace('_gene_ensembl', '')}"
        super().__init__(
            name=name,
            cache_dir=cache_dir,
            update_interval_hours=update_interval_hours,
        )

    # BioMart is a dynamic service — HEAD requests yield no useful version
    # metadata, so we rely purely on the time-based interval.
    def get_remote_urls(self) -> None:
        return None

    def check_update_needed(self) -> bool:
        """Honor the configured time window before doing any remote check."""
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
        logger.info(f"{self.name}: Querying Ensembl BioMart ({self.dataset})...")

        with tempfile.NamedTemporaryFile(suffix='.tsv', delete=False) as tmp:
            temp_file = Path(tmp.name)
        try:
            response = requests.get(
                BIOMART_URL,
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
                        if total_bytes % (10 * 1024 * 1024) < 1024 * 1024 and total_bytes > 0:
                            logger.info(f"{self.name}: Downloaded {total_bytes // (1024 * 1024)} MB...")

            logger.info(f"{self.name}: Download complete ({total_bytes // 1024} KB)")

            with open(temp_file, 'r', encoding='utf-8') as f:
                data = f.read()

            if data.strip().startswith('<!DOCTYPE') or data.strip().startswith('<html'):
                raise ValueError(
                    f"{self.name}: BioMart returned an HTML error page — "
                    f"check dataset='{self.dataset}' and species_id_attr='{self.species_id_attr}'"
                )
            return data
        finally:
            if temp_file.exists():
                temp_file.unlink()

    def process_data(self, raw_data: str) -> Dict[str, Any]:
        logger.info(f"{self.name}: Parsing BioMart TSV response...")

        species_id_to_ensembl_gene: Dict[str, str] = {}
        ensembl_gene_to_species_id: Dict[str, str] = {}
        gene_name_to_ensembl: Dict[str, str] = {}
        ensembl_gene_to_transcripts: Dict[str, List[str]] = {}
        ensembl_gene_to_peptides: Dict[str, List[str]] = {}

        header_seen = False
        row_count = 0

        for line in raw_data.split('\n'):
            line = line.strip()
            if not line:
                continue

            fields = line.split('\t')

            if not header_seen:
                header_seen = True
                continue

            if len(fields) < 5:
                continue

            ensembl_gene    = fields[0]
            ensembl_tx      = fields[1]
            ensembl_peptide = fields[2]
            gene_name       = fields[3]
            species_id      = fields[4]

            if not ensembl_gene:
                continue

            row_count += 1

            if species_id:
                species_id_to_ensembl_gene[species_id] = ensembl_gene
                ensembl_gene_to_species_id[ensembl_gene] = species_id

            if gene_name:
                gene_name_to_ensembl[gene_name] = ensembl_gene

            if ensembl_tx:
                txs = ensembl_gene_to_transcripts.setdefault(ensembl_gene, [])
                if ensembl_tx not in txs:
                    txs.append(ensembl_tx)

            if ensembl_peptide:
                peps = ensembl_gene_to_peptides.setdefault(ensembl_gene, [])
                if ensembl_peptide not in peps:
                    peps.append(ensembl_peptide)

        logger.info(f"{self.name}: Parsed {row_count:,} BioMart rows")
        logger.info(f"{self.name}: {self.species_id_attr}→Ensembl gene: {len(species_id_to_ensembl_gene):,}")
        logger.info(f"{self.name}: Gene name→Ensembl: {len(gene_name_to_ensembl):,}")
        logger.info(f"{self.name}: Ensembl genes with transcripts: {len(ensembl_gene_to_transcripts):,}")
        logger.info(f"{self.name}: Ensembl genes with peptides: {len(ensembl_gene_to_peptides):,}")

        return {
            'species_id_to_ensembl_gene': species_id_to_ensembl_gene,
            'ensembl_gene_to_species_id': ensembl_gene_to_species_id,
            'gene_name_to_ensembl': gene_name_to_ensembl,
            'ensembl_gene_to_transcripts': ensembl_gene_to_transcripts,
            'ensembl_gene_to_peptides': ensembl_gene_to_peptides,
        }

    # --- Sub-mapping properties ---

    @property
    def species_id_to_ensembl_gene(self) -> Dict[str, str]:
        if not self.mapping:
            self.load_or_update()
        return self.mapping.get('species_id_to_ensembl_gene', {})

    @property
    def ensembl_gene_to_species_id(self) -> Dict[str, str]:
        if not self.mapping:
            self.load_or_update()
        return self.mapping.get('ensembl_gene_to_species_id', {})

    @property
    def gene_name_to_ensembl(self) -> Dict[str, str]:
        if not self.mapping:
            self.load_or_update()
        return self.mapping.get('gene_name_to_ensembl', {})

    @property
    def ensembl_gene_to_transcripts(self) -> Dict[str, List[str]]:
        if not self.mapping:
            self.load_or_update()
        return self.mapping.get('ensembl_gene_to_transcripts', {})

    @property
    def ensembl_gene_to_peptides(self) -> Dict[str, List[str]]:
        if not self.mapping:
            self.load_or_update()
        return self.mapping.get('ensembl_gene_to_peptides', {})

    # --- Lookup helpers ---

    def get_ensembl_gene(self, species_id: str) -> Optional[str]:
        return self.species_id_to_ensembl_gene.get(species_id)

    def get_species_id(self, ensembl_gene_id: str) -> Optional[str]:
        return self.ensembl_gene_to_species_id.get(ensembl_gene_id)

    def get_ensembl_gene_by_name(self, gene_name: str) -> Optional[str]:
        return self.gene_name_to_ensembl.get(gene_name)

    def get_transcripts(self, ensembl_gene_id: str) -> Optional[List[str]]:
        return self.ensembl_gene_to_transcripts.get(ensembl_gene_id)

    def get_peptides(self, ensembl_gene_id: str) -> Optional[List[str]]:
        return self.ensembl_gene_to_peptides.get(ensembl_gene_id)
