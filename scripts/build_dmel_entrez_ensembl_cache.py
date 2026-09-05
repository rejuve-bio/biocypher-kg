"""
Bootstrap aux_files/dmel/entrez_ensembl/{entrez_ensembl_mapping.pkl,entrez_ensembl_version.json}
from LOCAL files, bypassing EntrezEnsemblProcessor.fetch_data()'s network download.

Why this exists: EntrezEnsemblProcessor.fetch_data() downloads
Adapter.SPECIES_INFO[7227]['features_data_url'], which points at
https://s3ftp.flybase.org/genomes/Drosophila_melanogaster/current/gtf/dmel-all-r6.67.gtf.gz.
That host returns an AWS WAF bot-challenge (HTTP 202, empty body) to non-browser
clients, so the processor silently builds an EMPTY mapping (0 entries) — which
breaks every adapter that resolves Entrez IDs (TFLink, coxpresdb) for dmel.

The exact same GTF is already available locally
(/mnt/hdd_1/biocypher-kg/input/dmel/dmel-all-r6.67.gtf.gz), so this script
replicates EntrezEnsemblProcessor.process_data()'s parsing logic against the
local file and writes the cache in the exact format BaseMappingProcessor
expects (gzip-pickled {'entrez_to_ensembl', 'gene_aliases'} dict + a
version.json with a fresh timestamp and correct entry count), so
EntrezEnsemblProcessor.load_or_update() picks it up as a valid, non-empty,
recent cache and never attempts the blocked download.

Usage:
    python scripts/build_dmel_entrez_ensembl_cache.py
"""

import argparse
import gzip
import json
import logging
import pickle
import re
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_GENE_INFO = "aux_files/dmel/Drosophila_melanogaster.gene_info.gz"
DEFAULT_GTF = "/mnt/hdd_1/biocypher-kg/input/dmel/dmel-all-r6.67.gtf.gz"
DEFAULT_CACHE_DIR = "aux_files/dmel/entrez_ensembl"
TAX_ID = "7227"

GENE_ID_RE = re.compile(r'gene_id "([^"]+)"')
GENE_NAME_RE = re.compile(r'(?:gene_name|gene_symbol) "([^"]+)"')


def build_mapping(gene_info_path, gtf_path):
    entrez_to_symbol = {}
    gene_aliases = {}

    logger.info("Parsing local NCBI gene_info: %s", gene_info_path)
    with gzip.open(gene_info_path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            fields = line.split("\t")
            if len(fields) < 16 or fields[0] != TAX_ID:
                continue

            entrez_id, symbol, synonyms, dbxrefs = fields[1], fields[2], fields[4], fields[5]
            symbol_from_nomenclature = fields[10] if fields[10] != "-" else symbol
            full_name, other_designations = fields[11], fields[13]

            if symbol_from_nomenclature and symbol_from_nomenclature != "-":
                entrez_to_symbol[entrez_id] = symbol_from_nomenclature

            hgnc = ensembl = ""
            for ref in dbxrefs.split("|"):
                if ref.startswith("HGNC:"):
                    hgnc = ref[5:]
                if ref.startswith(("Ensembl:", "FLYBASE:")):
                    ensembl = ref.split(":", 1)[1]

            if ensembl or hgnc:
                complete_synonyms = {symbol, symbol_from_nomenclature, full_name}
                complete_synonyms.update(synonyms.split("|"))
                complete_synonyms.update(other_designations.split("|"))
                if hgnc:
                    complete_synonyms.add(hgnc)
                complete_synonyms.discard("-")
                if ensembl:
                    gene_aliases[ensembl] = list(complete_synonyms)
                if hgnc:
                    gene_aliases[hgnc] = list(complete_synonyms)

    logger.info("Found %d Entrez-symbol mappings", len(entrez_to_symbol))

    logger.info("Parsing local FlyBase GTF: %s", gtf_path)
    symbol_to_fbgn = {}
    with gzip.open(gtf_path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            fields = line.split("\t")
            if len(fields) < 9 or fields[2] != "gene":
                continue
            attrs = fields[8]
            gid_m = GENE_ID_RE.search(attrs)
            name_m = GENE_NAME_RE.search(attrs)
            if not gid_m or not name_m:
                continue
            symbol_to_fbgn[name_m.group(1)] = gid_m.group(1).split(".")[0]

    logger.info("Found %d symbol-FBgn mappings", len(symbol_to_fbgn))

    entrez_to_ensembl = {
        entrez_id: symbol_to_fbgn[symbol]
        for entrez_id, symbol in entrez_to_symbol.items()
        if symbol in symbol_to_fbgn
    }
    logger.info("Built %d Entrez-FBgn mappings", len(entrez_to_ensembl))

    return {"entrez_to_ensembl": entrez_to_ensembl, "gene_aliases": gene_aliases}


def write_cache(mapping, cache_dir):
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    mapping_file = cache_dir / "entrez_ensembl_mapping.pkl"
    version_file = cache_dir / "entrez_ensembl_version.json"

    with gzip.open(mapping_file, "wb") as f:
        pickle.dump(mapping, f, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Wrote %s", mapping_file)

    total_entries = sum(len(v) for v in mapping.values())
    version_info = {
        "timestamp": datetime.now().isoformat(),
        "processor": "entrez_ensembl",
        "entries": total_entries,
        "source": "local files (s3ftp.flybase.org blocked by WAF bot-challenge, see docstring)",
    }
    with open(version_file, "w") as f:
        json.dump(version_info, f, indent=2)
    logger.info("Wrote %s (%d total entries)", version_file, total_entries)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gene-info", default=DEFAULT_GENE_INFO)
    parser.add_argument("--gtf", default=DEFAULT_GTF)
    parser.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR)
    args = parser.parse_args()

    mapping = build_mapping(args.gene_info, args.gtf)
    if not mapping["entrez_to_ensembl"]:
        logger.error("Built an empty mapping — check input file paths/formats before overwriting the cache.")
        raise SystemExit(1)
    write_cache(mapping, args.cache_dir)


if __name__ == "__main__":
    main()
