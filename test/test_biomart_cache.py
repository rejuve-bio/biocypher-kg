"""BioMart Ensembl cache behavior.

Guards the CI-stall regression: a committed/pre-built BioMart cache must be treated
as authoritative, so an old timestamp never triggers a runtime re-query of the
slow/unreliable BioMart API. BioMart is only queried when the cache is missing or
empty. All tests are network-free (they build a local cache in tmp_path).
"""

import gzip
import json
import pickle

from biocypher_metta.processors.biomart_ensembl_processor import BioMartEnsemblProcessor

_FULL_MAPPING = {
    "species_id_to_ensembl_gene": {"MGI:1": "ENSMUSG1"},
    "ensembl_gene_to_species_id": {"ENSMUSG1": "MGI:1"},
    "gene_name_to_ensembl": {"gene_a": "ENSMUSG1"},
    "ensembl_gene_to_transcripts": {"ENSMUSG1": ["ENSMUST1"]},
    "ensembl_gene_to_peptides": {"ENSMUSG1": ["ENSMUSP1"]},
}
_EMPTY_MAPPING = {k: {} for k in _FULL_MAPPING}


def _write_cache(cache_dir, name, timestamp, mapping):
    cache_dir.mkdir(parents=True, exist_ok=True)
    with gzip.open(cache_dir / f"{name}_mapping.pkl", "wb") as f:
        pickle.dump(mapping, f, protocol=pickle.HIGHEST_PROTOCOL)
    (cache_dir / f"{name}_version.json").write_text(
        json.dumps({
            "timestamp": timestamp,
            "processor": name,
            "entries": sum(len(v) for v in mapping.values()),
        })
    )


def _proc(cache_dir):
    return BioMartEnsemblProcessor(
        dataset="mmusculus_gene_ensembl", species_id_attr="mgi_id", cache_dir=str(cache_dir)
    )


def test_prebuilt_cache_is_authoritative_when_stale(tmp_path):
    cd = tmp_path / "biomart_ensembl"
    # A deliberately ancient timestamp — under the old logic this re-queried BioMart.
    _write_cache(cd, "biomart_mmusculus", "2000-01-01T00:00:00", _FULL_MAPPING)
    p = _proc(cd)
    assert p.check_update_needed() is False  # authoritative: no re-fetch despite age
    p.load_or_update()
    assert p.get_ensembl_gene("MGI:1") == "ENSMUSG1"


def test_missing_cache_needs_update(tmp_path):
    assert _proc(tmp_path / "does_not_exist").check_update_needed() is True


def test_empty_cache_forces_update(tmp_path):
    cd = tmp_path / "biomart_ensembl"
    _write_cache(cd, "biomart_mmusculus", "2000-01-01T00:00:00", _EMPTY_MAPPING)
    assert _proc(cd).check_update_needed() is True  # entries == 0 → refresh
