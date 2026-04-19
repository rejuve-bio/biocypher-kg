# BioCypher KG Mapping Processors

This package provides automatic updating mapping processors for biological identifier conversions.

## Overview

All processors inherit from `BaseMappingProcessor` and provide:
- **Automatic update checking** (time-based or dependency-based)
- **Caching with pickle files** for fast loading
- **Version tracking** with metadata
- **Graceful fallback** to cached data on network failures

## Available Processors

### 1. HGNCProcessor

Maps between HGNC gene symbols, numeric IDs, aliases, and Ensembl IDs.

**Data Source:** HGNC REST API
**Update Strategy:** Time-based (every 48 hours) - API lacks remote version metadata
**Mappings:**
- Current HGNC symbols (`current_symbols`)
- Previous/alias symbols → current symbols (`symbol_aliases`)
- Symbol ↔ Ensembl ID (`symbol_to_ensembl`, `ensembl_to_symbol`)
- HGNC ID → Symbol (`hgnc_id_to_symbol`)
- HGNC ID → Ensembl ID (`hgnc_id_to_ensembl`)

**Usage:**
```python
from biocypher_metta.processors import HGNCProcessor

# Initialize processor (uses aux_files/hgnc by default)
hgnc = HGNCProcessor(
    cache_dir='aux_files/hgnc',  # Default location
    update_interval_hours=48
)

# Load or update mapping
hgnc.load_or_update()

# Use the processor
result = hgnc.process_identifier('TP53')
print(result)  # {'status': 'current', 'original': 'TP53', 'current': 'TP53'}

# Get current symbol
symbol = hgnc.get_current_symbol('old_symbol_name')

# Get Ensembl ID from gene symbol
ensembl_id = hgnc.get_ensembl_id('TP53')

# Get Ensembl ID from HGNC numeric ID
ensembl_id = hgnc.get_ensembl_id('HGNC:11998')  # Also works!

# Get symbol from HGNC ID
symbol = hgnc.get_symbol_from_hgnc_id('HGNC:11998')
```

### 2. DBSNPProcessor

Maps between dbSNP rsIDs and genomic positions (chr:pos). Used by 7 adapters:
ABC, CADD, RefSeqClosestGene (forward lookup + referential-integrity filter),
Roadmap DHS/Chromatin/H3 (forward lookup), and TopLD (reverse lookup).

**Data source:** dbSNP VCF (~30 GB download for the full release)
**Update strategy:** Manual — build once every ~3 months via `scripts/update_dbsnp.py`
**Backend:** SQLite (`dbsnp_mapping.db`), with legacy pickle fallback for old caches
**Schema:** Single `rsid_to_pos(rsid, chr, pos)` table with `idx_pos(chr, pos)` for reverse lookup
**Chromosome format:** UCSC (`chr1`, `chrX`, `chrM`) — RefSeq accessions in the VCF are normalized on read

#### Three variants

| Variant | What's in it | Size | Purpose |
|---------|--------------|------|---------|
| `sample` | Adapter-driven: exactly the rsIDs + positions referenced by sample adapter inputs | ~100-200 KB (committed to repo) | CI/testing; `--dataset sample` works without any downloads |
| `common` | Variants with MAF ≥ 1% in any `FREQ` population | ~5 GB | Beta / dev — the primary artifact. Rare rsIDs are filtered out by design |
| `full`   | All rsIDs with chr + pos | ~35-50 GB | Production with rare-variant coverage |

**Filter-is-the-feature:** under `common`, adapters that look up rare rsIDs will silently drop those rows (via `except KeyError: continue`). This is intentional — it's the mechanism that scales the KG down for beta development.

#### Layout

```
<cache_root>/
├── common/
│   ├── dbsnp_mapping.db
│   └── dbsnp_version.json
└── full/                        (only if you built it)
    ├── dbsnp_mapping.db
    └── dbsnp_version.json
```

#### Build pipeline

```bash
# Common variants (primary artifact for beta/dev)
python3 scripts/update_dbsnp.py \
    --cache-dir /data/dbsnp/common --common-only --temp-dir /tmp

# Full dataset (only when needed)
python3 scripts/update_dbsnp.py \
    --cache-dir /data/dbsnp/full --temp-dir /tmp
```

Flags:
- `--cache-dir` (required) — output directory for `dbsnp_mapping.db` + `dbsnp_version.json`
- `--common-only` — apply the MAF ≥ 1% filter
- `--temp-dir` — where to download the ~30 GB VCF (defaults to `--cache-dir`; use a different disk if space is tight)
- `--keep-vcf` — preserve the downloaded VCF after processing (for debugging / re-inspection)
- `--force` — rebuild even if the 90-day refresh interval hasn't elapsed

#### Run pipeline

```bash
# Sample — no dbSNP flags needed (uses the committed sample cache)
python3 create_knowledge_graph.py --species hsa --dataset sample --output-dir output/hsa_sample

# Dev / beta — common variant
python3 create_knowledge_graph.py --species hsa --dataset full \
    --dbsnp-cache-root /data/dbsnp --dbsnp-variant common \
    --output-dir output/hsa

# Production with full coverage
python3 create_knowledge_graph.py --species hsa --dataset full \
    --dbsnp-cache-root /data/dbsnp --dbsnp-variant full \
    --output-dir output/hsa
```

Or set `dbsnp_cache_root` + `dbsnp_variant` in `config/species_config.yaml` to avoid retyping.

#### Usage (direct processor)

```python
from biocypher_metta.processors import DBSNPProcessor

# Point at a variant subdir
dbsnp = DBSNPProcessor(cache_dir='/data/dbsnp/common')
dbsnp.load_mapping()                 # reads SQLite DB + version JSON
print(dbsnp.is_common_only())        # True / False / None (legacy)

position = dbsnp.get_position('rs123456')
# {'chr': 'chr1', 'pos': 12345}

rsid_to_pos, pos_to_rsid = dbsnp.get_dict_wrappers()
```

#### Regenerating the sample cache

The repo ships with a committed sample cache at `aux_files/hsa/sample_dbsnp/dbsnp_mapping.db`
(~100-200 KB). It's built adapter-driven from an existing `common` DB, so it contains exactly
the rsIDs + positions referenced by sample adapter inputs — nothing more.

**You only need to regenerate it if you change sample adapter input files.** Most users never do.

```bash
# Prerequisite: a common DB exists somewhere
python3 scripts/update_dbsnp.py --cache-dir /data/dbsnp/common --common-only --temp-dir /tmp

# Rebuild the sample DB
python3 scripts/build_sample_dbsnp.py \
    --source /data/dbsnp/common \
    --adapters-config config/hsa/hsa_adapters_config_sample.yaml \
    --output aux_files/hsa/sample_dbsnp

# Commit
git add aux_files/hsa/sample_dbsnp/dbsnp_mapping.db aux_files/hsa/sample_dbsnp/dbsnp_version.json
```

The builder parses the sample adapters config, finds every adapter that consumes dbSNP lookups,
reads their sample CSVs to extract referenced rsIDs and `(chr, pos)` pairs, then queries the
source DB for each one. Any miss is logged but non-fatal (consistent with `common`'s filter semantics).

#### Migrating a legacy common DB

Pre-fix builds of `update_dbsnp.py` stored raw RefSeq chromosome names (`NC_000016.10`) instead
of UCSC names (`chr16`). The one-shot migration rewrites in place — no re-download needed:

```bash
python3 scripts/migrate_dbsnp_chroms.py \
    --db /data/dbsnp/common/dbsnp_mapping.db
```

Takes 10-15 min for a 5 GB DB. Drops any alt / decoy contigs, rebuilds the `idx_pos` index,
and (by default) `VACUUM`s to reclaim space.

#### End-to-end flow

```
            ┌─────────────────────────────────────────┐
            │  dbSNP VCF  (ftp.ncbi.nih.gov, ~30 GB)  │
            └─────────────┬───────────────────────────┘
                          │
          scripts/update_dbsnp.py    (download + filter + write SQLite)
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
     common DB        full DB      (choose one)
     ~5 GB            ~35-50 GB
          │               │
          └───────┬───────┘
                  │
  scripts/build_sample_dbsnp.py    (auto-extract refs, query source)
                  │
                  ▼
     committed sample DB in repo
     aux_files/hsa/sample_dbsnp/dbsnp_mapping.db

     create_knowledge_graph.py    (resolve variant, load DB, feed adapters)
       --dataset sample                              → sample DB
       --dataset full --dbsnp-variant common         → common DB
       --dataset full --dbsnp-variant full           → full DB
```

### 3. EntrezEnsemblProcessor

Maps between NCBI Entrez Gene IDs and Ensembl Gene IDs, and provides gene alias dictionaries.

**Data Sources:**
- NCBI Gene Info (`Homo_sapiens.gene_info.gz`)
- GENCODE annotations

**Update Strategy:** Remote version checking (ETag, Last-Modified headers)

**Mappings:**
- Entrez Gene ID → Ensembl Gene ID (`entrez_to_ensembl`)
- Gene aliases keyed by Ensembl ID and HGNC ID (`gene_aliases`)

The gene alias dict is built from NCBI Gene Info fields: symbol, synonyms, dbxrefs (HGNC/Ensembl), nomenclature symbol, full name, and other designations. This replaces the need for a separate `Homo_sapiens.gene_info.gz` file and the `get_gene_alias()` method in `GencodeGeneAdapter` for human data. Dmel still uses its own file-based path (`Drosophila_melanogaster.gene_info.gz`).

**Usage:**
```python
from biocypher_metta.processors import EntrezEnsemblProcessor

# Initialize processor (uses aux_files/hsa/entrez_ensembl by default)
processor = EntrezEnsemblProcessor(
    cache_dir='aux_files/hsa/entrez_ensembl',  # Default location
    update_interval_hours=168  # 7 days
)

# Load or update mapping
processor.load_or_update()

# Get Ensembl ID from Entrez ID
ensembl_id = processor.get_ensembl_id('7157')  # TP53 Entrez ID
print(ensembl_id)  # ENSG00000141510

# Reverse lookup
entrez_id = processor.get_entrez_id('ENSG00000141510')

# Access entrez→ensembl dict directly
mapping = processor.entrez_to_ensembl  # {entrez_id: ensembl_id, ...}

# Access gene aliases
aliases = processor.gene_aliases  # {ensembl_id: [syns], hgnc_id: [syns], ...}

# Lookup aliases for a specific gene
tp53_aliases = processor.get_gene_aliases('ENSG00000141510')
print(tp53_aliases)  # ['TP53', 'p53', 'tumor protein p53', ...]
```

**Note:** The internal mapping format is nested (`{'entrez_to_ensembl': {...}, 'gene_aliases': {...}}`). Adapters should use the `entrez_to_ensembl` property instead of accessing `processor.mapping` directly. Legacy flat-format caches are auto-detected and re-fetched.

### 4. EnsemblUniProtProcessor

Maps between Ensembl Protein IDs (ENSP) and UniProt IDs.

**Data Source:** UniProt ID Mapping
**Update Strategy:** Remote version checking (ETag, Last-Modified headers)

**Usage:**
```python
from biocypher_metta.processors import EnsemblUniProtProcessor

# Initialize processor (uses aux_files/ensembl_uniprot by default)
processor = EnsemblUniProtProcessor(
    cache_dir='aux_files/ensembl_uniprot',  # Default location
    update_interval_hours=168  # 7 days
)

# Load or update mapping
processor.load_or_update()

# Get UniProt ID from Ensembl Protein ID
uniprot_id = processor.get_uniprot_id('ENSP00000269305')
print(uniprot_id)  # P04637 (TP53)

# Reverse lookup
ensembl_id = processor.get_ensembl_id('P04637')
```

### 5. GOSubontologyProcessor

Maps GO term IDs to their subontologies (biological_process, molecular_function, cellular_component).

**Data Source:** Gene Ontology OWL file (via OntologyAdapter)
**Update Strategy:** Dependency-based (updates when GO.owl changes)

**Usage:**
```python
from biocypher_metta.processors import GOSubontologyProcessor
import rdflib

# Initialize processor (uses aux_files/go_subontology by default)
processor = GOSubontologyProcessor(
    cache_dir='aux_files/go_subontology',  # Default location
    dependency_file='path/to/go.owl'
)

# Set the RDF graph (typically done by GeneOntologyAdapter)
graph = rdflib.Graph()
graph.parse('path/to/go.owl')
processor.set_graph(graph)

# Load or update mapping
processor.load_or_update()

# Get subontology for a GO term
subontology = processor.get_subontology('GO:0008150')
print(subontology)  # biological_process

# Check subontology type
is_bp = processor.is_biological_process('GO:0008150')  # True

# Filter GO terms by subontology
bp_terms = processor.filter_by_subontology(
    ['GO:0008150', 'GO:0003674', 'GO:0005575'],
    'biological_process'
)
```

## Creating a Custom Processor

To create a new mapping processor, inherit from `BaseMappingProcessor` and implement two methods:

```python
from biocypher_metta.processors import BaseMappingProcessor
from typing import Dict, Any

class MyCustomProcessor(BaseMappingProcessor):
    """Processor for custom ID mappings."""

    SOURCE_URL = "https://example.com/data.txt"

    def __init__(self, cache_dir='aux_files/custom', update_interval_hours=168):
        super().__init__(
            name='custom',
            cache_dir=cache_dir,
            update_interval_hours=update_interval_hours
        )

    def fetch_data(self) -> Any:
        """Fetch raw data from source."""
        import requests
        response = requests.get(self.SOURCE_URL, timeout=30)
        response.raise_for_status()
        return response.text

    def process_data(self, raw_data: str) -> Dict[str, str]:
        """Process raw data into mapping dictionary."""
        mapping = {}
        for line in raw_data.split('\n'):
            if line.strip():
                id1, id2 = line.split('\t')
                mapping[id1] = id2
        return mapping

    def get_mapped_id(self, source_id: str) -> str:
        """Get mapped ID."""
        if not self.mapping:
            self.load_or_update()
        return self.mapping.get(source_id)
```

## Update Strategies

Processors use three intelligent update strategies:

### 1. Time-Based Updates

Checks if specified time interval has passed since last update. Used when remote version checking is unavailable.

**Used by:** HGNCProcessor (API lacks version metadata)

```python
processor = HGNCProcessor(update_interval_hours=48)
processor.load_or_update()  # Updates if >48 hours have passed
```

### 2. Remote Version Checking

Checks HTTP headers (Last-Modified, ETag, Content-Length) to detect remote changes without downloading data.

**Used by:** EntrezEnsemblProcessor, EnsemblUniProtProcessor

```python
processor = EntrezEnsemblProcessor()
processor.load_or_update()  # Updates only if remote file changed
```

### 3. Dependency-Based Updates

Checks if a dependency file has been modified more recently than the cache.

**Used by:** GOSubontologyProcessor (updates when GO graph changes)

```python
processor = GOSubontologyProcessor(
    dependency_file='path/to/go.owl'
)
processor.load_or_update()  # Updates if go.owl is newer than mapping
```

### 4. Manual Only

No automatic updates - requires explicit rebuild via standalone script.

**Used by:** DBSNPProcessor (30GB download, updated via cronjob)

```python
# Processor only loads, never updates
dbsnp = DBSNPProcessor()
dbsnp.load_mapping()  # Never triggers download
```

## File Structure

All mapping files are organized under the `aux_files/` directory. Each processor creates two files:

```
aux_files/
├── hsa/
│   ├── hgnc/
│   │   ├── hgnc_mapping.pkl          # Gzip-compressed pickled mapping dictionary
│   │   └── hgnc_version.json         # Metadata (timestamp, entries, etc.)
│   ├── entrez_ensembl/
│   │   ├── entrez_ensembl_mapping.pkl # Contains entrez→ensembl + gene aliases
│   │   └── entrez_ensembl_version.json
│   ├── ensembl_uniprot/
│   │   ├── ensembl_uniprot_mapping.pkl
│   │   └── ensembl_uniprot_version.json
│   └── sample_dbsnp/
│       ├── dbsnp_mapping.pkl
│       └── dbsnp_version.json
├── dmel/
│   └── Drosophila_melanogaster.gene_info.gz  # Dmel gene aliases (file-based, not processor-managed)
├── go_subontology/                   # Species-agnostic, stays at top level
│   ├── go_subontology_mapping.pkl
│   └── go_subontology_version.json
└── ... (legacy static pickle files)
```

**Note:** For human, gene aliases are now included in the EntrezEnsemblProcessor cache — the separate `Homo_sapiens.gene_info.gz` file is no longer needed. Dmel still uses its own species-specific file.

**Note:** All `.pkl` files are gzip-compressed to save space and reduce repository size. The processors automatically handle compression/decompression transparently. Legacy uncompressed pickle files are automatically detected and re-saved as compressed files on first load.

## Forcing Updates

To force an update regardless of the schedule:

```python
processor.load_or_update(force=True)
```

## Error Handling

Processors gracefully handle network failures:

1. Attempt to fetch new data from source
2. If fetch fails and cached data exists, use cached data
3. If fetch fails and no cached data, raise error

```python
processor = HGNCProcessor()
success = processor.update_mapping()
if success:
    print("Mapping ready")
else:
    print("Failed to update and no cache available")
```

## Integration with Adapters

Adapters should use processors during initialization. Use the `entrez_to_ensembl` property (not `.mapping` directly) to access the entrez→ensembl dict:

```python
class MyAdapter(Adapter):
    def __init__(self, entrez_to_ensembl_processor=None, **kwargs):
        super().__init__(**kwargs)

        # Initialize processor if not provided
        if entrez_to_ensembl_processor is None:
            self.processor = EntrezEnsemblProcessor()
            self.processor.load_or_update()
        else:
            self.processor = entrez_to_ensembl_processor

    def get_edges(self):
        # Use .entrez_to_ensembl property for the flat dict
        entrez_ensembl_dict = self.processor.entrez_to_ensembl
        for entrez_id in self.data:
            ensembl_id = entrez_ensembl_dict.get(entrez_id)
            if ensembl_id:
                # Process edge...
                pass
```

## Migration Guide

### From Legacy HGNCSymbolProcessor

**Old code:**
```python
from biocypher_metta.adapters.hgnc_processor import HGNCSymbolProcessor

hgnc = HGNCSymbolProcessor(
    pickle_file_path='hgnc_gene_data/hgnc_data.pkl',
    version_file_path='hgnc_gene_data/hgnc_version.txt'
)
hgnc.update_hgnc_data()
symbol = hgnc.get_current_symbol('TP53')
```

**New code:**
```python
from biocypher_metta.processors import HGNCProcessor

# Uses aux_files/hgnc by default
hgnc = HGNCProcessor()
hgnc.load_or_update()
symbol = hgnc.get_current_symbol('TP53')
```

The legacy adapter still works but emits a deprecation warning.

## Performance Considerations

1. **Lazy Loading:** Only load mappings when needed
2. **Caching:** Mappings are cached in memory after loading
3. **Large Files:** UniProt mappings (~500MB) are downloaded in chunks
4. **Update Frequency:** Balance freshness vs. download time

## Troubleshooting

### Mapping not updating
- Check network connectivity
- Verify update interval has passed: `processor.check_update_needed()`
- Force update: `processor.load_or_update(force=True)`

### Out of memory
- Large mappings (UniProt) require sufficient RAM
- Consider increasing system memory or reducing update frequency

### Corrupted cache
- Delete `.pkl` and `.json` files in cache directory
- Run `processor.load_or_update(force=True)`

## Contributing

To add a new processor:

1. Create a new file in `biocypher_metta/processors/`
2. Inherit from `BaseMappingProcessor`
3. Implement `fetch_data()` and `process_data()`
4. Add to `__init__.py`
5. Add documentation to this README
6. Write tests
