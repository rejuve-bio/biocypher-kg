"""
Ensembl Protein to UniProt ID Processor.

Maintains mappings between Ensembl Protein IDs (ENSP) and UniProt IDs.

Data source:
- UniProt ID Mapping: https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/

Update strategy: Time-based (every 7 days)
"""

import requests
import gzip
from typing import Dict, Any, Optional
from .base_mapping_processor import BaseMappingProcessor


class EnsemblUniProtProcessor(BaseMappingProcessor):
    UNIPROT_IDMAPPING_URL = (
        "https://ftp.uniprot.org/pub/databases/uniprot/current_release/"
        "knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping.dat.gz"
    )

    def __init__(
        self,
        cache_dir: str = 'aux_files/ensembl_uniprot',
        update_interval_hours: Optional[int] = None
    ):
        super().__init__(
            name='ensembl_uniprot',
            cache_dir=cache_dir,
            update_interval_hours=update_interval_hours
        )

    def get_remote_urls(self):
        return [self.UNIPROT_IDMAPPING_URL]

    def fetch_data(self) -> str:
        print(f"{self.name}: Fetching UniProt ID mappings...")
        print(f"{self.name}: This may take a while (file is ~500MB compressed)...")

        response = requests.get(self.UNIPROT_IDMAPPING_URL, timeout=600, stream=True)
        response.raise_for_status()

        chunks = []
        total_size = 0
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                chunks.append(chunk)
                total_size += len(chunk)
                if total_size % (10 * 1024 * 1024) == 0:
                    print(f"{self.name}: Downloaded {total_size // (1024 * 1024)}MB...")

        compressed_data = b''.join(chunks)
        print(f"{self.name}: Decompressing...")
        data = gzip.decompress(compressed_data).decode('utf-8')

        return data

    def process_data(self, raw_data: str) -> Dict[str, str]:
        print(f"{self.name}: Parsing ID mappings...")

        ensembl_to_uniprot = {}
        line_count = 0

        for line in raw_data.split('\n'):
            line_count += 1
            if line_count % 1000000 == 0:
                print(f"{self.name}: Processed {line_count // 1000000}M lines...")

            if not line.strip():
                continue

            fields = line.split('\t')
            if len(fields) != 3:
                continue

            uniprot_id = fields[0]
            id_type = fields[1]
            external_id = fields[2]

            if id_type in ['Ensembl_PRO', 'Ensembl']:
                if external_id.startswith('ENSP'):
                    base_ensembl = external_id.split('.')[0]
                    ensembl_to_uniprot[base_ensembl] = uniprot_id
                    ensembl_to_uniprot[external_id] = uniprot_id

        print(f"{self.name}: Created {len(ensembl_to_uniprot)} Ensembl-UniProt mappings")

        return ensembl_to_uniprot

    def get_uniprot_id(self, ensembl_protein_id: str) -> str:
        if not self.mapping:
            self.load_or_update()

        uniprot_id = self.mapping.get(ensembl_protein_id)
        if uniprot_id:
            return uniprot_id

        base_id = ensembl_protein_id.split('.')[0]
        return self.mapping.get(base_id)

    def get_ensembl_id(self, uniprot_id: str) -> str:
        if not self.mapping:
            self.load_or_update()

        for ensembl_id, uni_id in self.mapping.items():
            if uni_id == uniprot_id and '.' not in ensembl_id:
                return ensembl_id

        return None
