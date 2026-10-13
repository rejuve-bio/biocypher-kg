"""Tests for the TfbsAdapter — both UCSC/human and modERN/dmel modes."""

import gzip
import os
import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Ensure the project root is importable
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biocypher_metta.adapters.tfbs_adapter import TfbsAdapter

# ---------------------------------------------------------------------------
# Paths to sample files
# ---------------------------------------------------------------------------
SAMPLES_DIR = ROOT / 'samples'
HUMAN_SAMPLE = str(SAMPLES_DIR / 'hsa' / 'tfbs' / 'encRegTfbsClustered.txt.gz')
DMEL_SAMPLE = str(SAMPLES_DIR / 'dmel' / 'tfbs' / 'sample_modern_dmel_tfbs.tsv')
DMEL_TF_MAP = str(SAMPLES_DIR / 'dmel' / 'tfbs' / 'sample_modern_tf_fbgn_map.tsv')


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def _make_mock_hgnc():
    """Create a mock HGNCProcessor that maps a few known TF names."""
    mock = MagicMock()
    mapping = {
        'POLR2A': 'ENSG00000181222',
        'RAD21': 'ENSG00000164754',
        'ARID1B': 'ENSG00000049618',
        'NCOR1': 'ENSG00000141027',
        'HDAC1': 'ENSG00000116478',
    }
    mock.get_ensembl_id.side_effect = lambda name: mapping.get(name)
    return mock


# ===========================================================================
#  HUMAN (ucsc_cluster) MODE TESTS
# ===========================================================================

class TestHumanUCSCMode:
    """Verify that the default human/UCSC behavior is unchanged."""

    @pytest.fixture()
    def adapter_nodes(self):
        return TfbsAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=HUMAN_SAMPLE,
            label='tfbs',
            taxon_id=9606,
            hgnc_processor=_make_mock_hgnc(),
        )

    @pytest.fixture()
    def adapter_edges(self):
        return TfbsAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=HUMAN_SAMPLE,
            label='gene_tfbs',
            taxon_id=9606,
            hgnc_processor=_make_mock_hgnc(),
        )

    def test_nodes_emitted(self, adapter_nodes):
        nodes = list(adapter_nodes.get_nodes())
        assert len(nodes) > 0, "Should emit at least one node"

    def test_node_id_format(self, adapter_nodes):
        node_id, label, props = next(iter(adapter_nodes.get_nodes()))
        assert node_id.startswith('ENCODE_TFBS:'), f"Got {node_id}"
        assert 'GRCh38' in node_id, "Human nodes must use GRCh38 assembly"

    def test_node_label(self, adapter_nodes):
        _, label, _ = next(iter(adapter_nodes.get_nodes()))
        assert label == 'tfbs'

    def test_node_properties(self, adapter_nodes):
        _, _, props = next(iter(adapter_nodes.get_nodes()))
        assert 'chr' in props
        assert 'start' in props
        assert 'end' in props
        assert props['taxon_id'] == '9606'
        assert props['source'] == 'ENCODE'
        assert 'genome.ucsc.edu' in props['source_url']

    def test_edges_emitted(self, adapter_edges):
        edges = list(adapter_edges.get_edges())
        assert len(edges) > 0, "Should emit at least one edge"

    def test_edge_format(self, adapter_edges):
        gene_id, tfbs_id, label, props = next(iter(adapter_edges.get_edges()))
        assert gene_id.startswith('ENSEMBL:ENSG'), f"Got {gene_id}"
        assert tfbs_id.startswith('ENCODE_TFBS:')
        assert label == 'gene_tfbs'

    def test_edge_score_normalized(self, adapter_edges):
        """Human scores should be divided by 1000."""
        _, _, _, props = next(iter(adapter_edges.get_edges()))
        assert 'score' in props
        # Original sample scores are 92–275; dividing by 1000 gives < 1
        assert props['score'] < 1.0, f"Expected normalized score < 1, got {props['score']}"

    def test_unmapped_tf_skipped(self, adapter_edges):
        """TFs that HGNC can't map should not produce edges."""
        edges = list(adapter_edges.get_edges())
        gene_ids = {e[0] for e in edges}
        # All gene IDs should start with ENSEMBL:
        for gid in gene_ids:
            assert gid.startswith('ENSEMBL:'), f"Unexpected gene ID format: {gid}"

    def test_default_input_format(self, adapter_nodes):
        assert adapter_nodes.input_format == 'ucsc_cluster'


# ===========================================================================
#  DMEL (modern_tsv) MODE TESTS
# ===========================================================================

class TestDmelModernMode:
    """Verify dmel/modERN TSV behavior."""

    @pytest.fixture()
    def adapter_nodes(self):
        return TfbsAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=DMEL_SAMPLE,
            label='tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
            source='ENCODE modERN',
            source_url='https://www.encodeproject.org/',
        )

    @pytest.fixture()
    def adapter_edges(self):
        return TfbsAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=DMEL_SAMPLE,
            label='gene_tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
            source='ENCODE modERN',
            source_url='https://www.encodeproject.org/',
        )

    # --- Node tests ---

    def test_nodes_count(self, adapter_nodes):
        """Sample has 6 rows → 6 nodes (all rows produce nodes regardless of TF mapping)."""
        nodes = list(adapter_nodes.get_nodes())
        assert len(nodes) == 6

    def test_node_id_format(self, adapter_nodes):
        node_id, _, _ = next(iter(adapter_nodes.get_nodes()))
        # First row: chr=2L, start=5000, end=5400, assembly=dm6
        assert node_id == 'ENCODE_TFBS:2L_5000_5400_dm6', f"Got {node_id}"

    def test_node_label(self, adapter_nodes):
        _, label, _ = next(iter(adapter_nodes.get_nodes()))
        assert label == 'tfbs'

    def test_node_properties(self, adapter_nodes):
        _, _, props = next(iter(adapter_nodes.get_nodes()))
        assert props['chr'] == '2L'
        assert props['start'] == 5000
        assert props['end'] == 5400
        assert props['taxon_id'] == '7227'
        assert props['source'] == 'ENCODE modERN'
        assert props['source_url'] == 'https://www.encodeproject.org/'

    # --- Edge tests ---

    def test_edges_count(self, adapter_edges):
        """5 mapped TFs (dl, twi, sna, CG6854, Mad), unmapped_tf is skipped.
        CG6854 maps to 2 genes → 6 edges total."""
        edges = list(adapter_edges.get_edges())
        assert len(edges) == 6

    def test_edge_gene_id_format(self, adapter_edges):
        gene_id, _, _, _ = next(iter(adapter_edges.get_edges()))
        assert gene_id.startswith('FlyBase:FBgn'), f"Got {gene_id}"

    def test_first_edge_details(self, adapter_edges):
        gene_id, tfbs_id, label, props = next(iter(adapter_edges.get_edges()))
        assert gene_id == 'FlyBase:FBgn0000463'  # dl
        assert tfbs_id == 'ENCODE_TFBS:2L_5000_5400_dm6'
        assert label == 'gene_tfbs'

    def test_multi_gene_tf_produces_multiple_edges(self, adapter_edges):
        """CG6854 maps to FBgn0266441 and FBgn0266452 — should produce 2 edges."""
        edges = list(adapter_edges.get_edges())
        cg6854_edges = [e for e in edges if 'FBgn0266441' in e[0] or 'FBgn0266452' in e[0]]
        assert len(cg6854_edges) == 2
        gene_ids = {e[0] for e in cg6854_edges}
        assert gene_ids == {'FlyBase:FBgn0266441', 'FlyBase:FBgn0266452'}

    def test_unmapped_tf_skipped(self, adapter_edges):
        """'unmapped_tf' is not in the map file — should produce no edges."""
        edges = list(adapter_edges.get_edges())
        gene_ids = {e[0] for e in edges}
        assert all('unmapped' not in gid for gid in gene_ids)

    def test_edge_score_raw(self, adapter_edges):
        """Default dmel score_transform is 'raw' — score should not be divided."""
        _, _, _, props = next(iter(adapter_edges.get_edges()))
        # First row score is 142
        assert props['score'] == 142.0

    def test_edge_provenance(self, adapter_edges):
        _, _, _, props = next(iter(adapter_edges.get_edges()))
        assert props['source'] == 'ENCODE modERN'
        assert props['source_url'] == 'https://www.encodeproject.org/'

    # --- Score transform tests ---

    def test_score_transform_log10(self):
        adapter = TfbsAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=DMEL_SAMPLE,
            label='gene_tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
            score_transform='log10',
        )
        _, _, _, props = next(iter(adapter.get_edges()))
        # log10(142) ≈ 2.152
        import math
        assert abs(props['score'] - math.log10(142)) < 0.001

    # --- Genomic location filter tests ---

    def test_chr_filter(self):
        adapter = TfbsAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=DMEL_SAMPLE,
            label='tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
            chr='2L',
        )
        nodes = list(adapter.get_nodes())
        # Only chr=2L rows: 2 out of 6
        assert len(nodes) == 2
        for _, _, props in nodes:
            assert props['chr'] == '2L'

    # --- No provenance mode ---

    def test_no_provenance(self):
        adapter = TfbsAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=DMEL_SAMPLE,
            label='tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
        )
        _, _, props = next(iter(adapter.get_nodes()))
        assert 'source' not in props
        assert 'source_url' not in props

    # --- No properties mode ---

    def test_no_properties(self):
        adapter = TfbsAdapter(
            write_properties=False,
            add_provenance=False,
            filepath=DMEL_SAMPLE,
            label='tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
        )
        _, _, props = next(iter(adapter.get_nodes()))
        assert props == {}


# ===========================================================================
#  GZIPPED DMEL INPUT TEST
# ===========================================================================

class TestDmelGzippedInput:
    """Verify that the modERN reader also works with gzipped input."""

    @pytest.fixture()
    def gz_sample(self, tmp_path):
        """Create a gzipped version of the sample TSV."""
        src = Path(DMEL_SAMPLE)
        gz_path = tmp_path / 'sample_modern_dmel_tfbs.tsv.gz'
        with open(src, 'rb') as f_in:
            with gzip.open(gz_path, 'wb') as f_out:
                f_out.write(f_in.read())
        return str(gz_path)

    def test_gz_nodes(self, gz_sample):
        adapter = TfbsAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gz_sample,
            label='tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
        )
        nodes = list(adapter.get_nodes())
        assert len(nodes) == 6

    def test_gz_edges(self, gz_sample):
        adapter = TfbsAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gz_sample,
            label='gene_tfbs',
            taxon_id=7227,
            input_format='modern_tsv',
            tf_map_path=DMEL_TF_MAP,
            assembly='dm6',
            gene_prefix='FlyBase',
        )
        edges = list(adapter.get_edges())
        assert len(edges) == 6
