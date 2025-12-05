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

Maps between dbSNP rsIDs and genomic positions (chr:pos).

**Data Source:** dbSNP VCF (30GB download)
**Update Strategy:** Manual only (no auto-updates) - see update_dbsnp.py
**Mappings:**
- rsID → genomic position (`rsid_to_pos`)
- Genomic position → rsID (`pos_to_rsid`)

**Usage:**
```python
from biocypher_metta.processors import DBSNPProcessor

# Load-only processor (never auto-updates)
dbsnp = DBSNPProcessor(cache_dir='/mnt/hdd_2/kedist/rsids_map')
dbsnp.load_mapping()  # Only loads, never downloads

# Get position from rsID
position = dbsnp.get_position('rs123456')
# Returns: {'chr': 'chr1', 'pos': 12345}

# Get wrappers for dict-like access
rsid_to_pos, pos_to_rsid = dbsnp.get_dict_wrappers()
```

**Note:** Update dbSNP using `update_dbsnp.py` script (see main README).

### 3. EntrezEnsemblProcessor

Maps between NCBI Entrez Gene IDs and Ensembl Gene IDs.

**Data Sources:**
- NCBI Gene Info
- GENCODE annotations

**Update Strategy:** Remote version checking (ETag, Last-Modified headers)

**Usage:**
```python
from biocypher_metta.processors import EntrezEnsemblProcessor

# Initialize processor (uses aux_files/entrez_ensembl by default)
processor = EntrezEnsemblProcessor(
    cache_dir='aux_files/entrez_ensembl',  # Default location
    update_interval_hours=168  # 7 days
)

# Load or update mapping
processor.load_or_update()

# Get Ensembl ID from Entrez ID
ensembl_id = processor.get_ensembl_id('7157')  # TP53 Entrez ID
print(ensembl_id)  # ENSG00000141510

# Reverse lookup
entrez_id = processor.get_entrez_id('ENSG00000141510')
```

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
├── hgnc/
│   ├── hgnc_mapping.pkl          # Gzip-compressed pickled mapping dictionary
│   └── hgnc_version.json         # Metadata (timestamp, entries, etc.)
├── entrez_ensembl/
│   ├── entrez_ensembl_mapping.pkl
│   └── entrez_ensembl_version.json
├── ensembl_uniprot/
│   ├── ensembl_uniprot_mapping.pkl
│   └── ensembl_uniprot_version.json
├── go_subontology/
│   ├── go_subontology_mapping.pkl
│   └── go_subontology_version.json
└── ... (legacy static pickle files)
```

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

Adapters should use processors during initialization:

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
        for entrez_id in self.data:
            ensembl_id = self.processor.get_ensembl_id(entrez_id)
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
