"""
Tests for the FlyBase regulatory-annotation adapter.

Uses a small synthetic GFF3 snippet to verify all output modes:
  1. Enhancer nodes
  2. Enhancer → gene edges
  3. Regulatory region nodes (non-TFBS, non-enhancer)
  4. Gene → TFBS edges (with BDGP6 assembly)
  5. Regulatory feature → sequence type edges (classified_as)
"""

import gzip
import os
import tempfile

import pytest

from biocypher_metta.adapters.dmel.flybase_regulatory_adapter import (
    FlyBaseRegulatoryAdapter,
)

# ---------------------------------------------------------------------------
#  Fixture: synthetic GFF3 data
# ---------------------------------------------------------------------------

SAMPLE_GFF3 = """\
##gff-version 3
##genome-build FlyBase r6.69
2L\tREDfly_CRMs\tregulatory_region\t5468\t5967\t.\t.\t.\tID=FBsf0000926668;Name=Unspecified_Kc167-AF_1;Dbxref=FlyBase:FBsf0000926668;library=REDfly_CRMs:FBlc0000493
2L\tREDfly_CRMs\tregulatory_region\t100000\t100500\t.\t.\t.\tID=FBsf0001111111;Name=test_enhancer_region;Dbxref=FlyBase:FBsf0001111111;library=REDfly_CRMs:FBlc0000493;associated_genes=sna:FBgn0003448
2L\tREDfly_CRMs\tregulatory_region\t200000\t200800\t.\t.\t.\tID=FBsf0002222222;Name=another_enhancer_dual;Dbxref=FlyBase:FBsf0002222222;associated_genes=Thor:FBgn0261560,pax:FBgn0004579
3R\tmE1_TFBS_sens\tTF_binding_site\t3272\t3476\t.\t.\t.\tID=FBsf0000226205;Name=TFBS_sens_011071;Dbxref=FlyBase:FBsf0000226205;library=mE1_TFBS_sens:FBlc0000315;bound_moiety=sens:FBgn0002573
3R\tFlyBase\tinsulator\t50000\t50500\t.\t.\t.\tID=FBsf0003333333;Name=test_insulator;Dbxref=FlyBase:FBsf0003333333;library=insulator_lib:FBlc0000100
X\tFlyBase\tTSS\t10000\t10001\t.\t+\t.\tID=FBsf0004444444;Name=test_TSS;Dbxref=FlyBase:FBsf0004444444
scaffold_123\tFlyBase\tregulatory_region\t100\t200\t.\t.\t.\tID=FBsf9999999999;Name=scaffold_should_be_skipped
"""


@pytest.fixture
def gff3_path(tmp_path):
    """Create a gzip-compressed GFF3 file (plain gzip, not tarball)."""
    path = tmp_path / "test_flybase.gff.gz"
    with gzip.open(str(path), "wt", encoding="utf-8") as f:
        f.write(SAMPLE_GFF3)
    return str(path)


# ---------------------------------------------------------------------------
#  Tests: Enhancer nodes
# ---------------------------------------------------------------------------


class TestEnhancerNodes:
    def test_enhancer_nodes_emitted(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=gff3_path,
            label="flybase_enhancer",
        )
        nodes = list(adapter.get_nodes())
        # Only entries with "enhancer" in Name AND associated_genes should be emitted.
        assert len(nodes) == 2
        ids = {n[0] for n in nodes}
        assert "FlyBase:FBsf0001111111" in ids
        assert "FlyBase:FBsf0002222222" in ids

    def test_enhancer_node_properties(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=gff3_path,
            label="flybase_enhancer",
        )
        nodes = list(adapter.get_nodes())
        node = [n for n in nodes if n[0] == "FlyBase:FBsf0001111111"][0]
        assert node[1] == "enhancer"
        assert node[2]["chr"] == "2L"
        assert node[2]["start"] == 100000
        assert node[2]["end"] == 100500
        assert node[2]["name"] == "test_enhancer_region"
        assert node[2]["source"] == "FlyBase"

    def test_non_enhancer_regulatory_region_excluded(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_enhancer",
        )
        nodes = list(adapter.get_nodes())
        ids = {n[0] for n in nodes}
        # FBsf0000926668 has no "enhancer" in Name
        assert "FlyBase:FBsf0000926668" not in ids


# ---------------------------------------------------------------------------
#  Tests: Enhancer → gene edges
# ---------------------------------------------------------------------------


class TestEnhancerGeneEdges:
    def test_enhancer_gene_edges(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_enhancer_gene",
        )
        edges = list(adapter.get_edges())
        # FBsf0001111111 → 1 gene, FBsf0002222222 → 2 genes = 3 edges
        assert len(edges) == 3

    def test_edge_targets_correct(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_enhancer_gene",
        )
        edges = list(adapter.get_edges())
        targets = {e[1] for e in edges}
        assert "FlyBase:FBgn0003448" in targets  # sna
        assert "FlyBase:FBgn0261560" in targets  # Thor
        assert "FlyBase:FBgn0004579" in targets  # pax

    def test_confirmed_property(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_enhancer_gene",
        )
        edges = list(adapter.get_edges())
        for edge in edges:
            assert edge[3]["confirmed"] is True


# ---------------------------------------------------------------------------
#  Tests: Regulatory region nodes (non-TFBS, non-enhancer)
# ---------------------------------------------------------------------------


class TestRegulatoryRegionNodes:
    def test_regulatory_region_nodes(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=gff3_path,
            label="flybase_regulatory_region",
        )
        nodes = list(adapter.get_nodes())
        ids = {n[0] for n in nodes}
        # Should include: 1 plain regulatory_region + 1 insulator + 1 TSS = 3
        # Should NOT include: TFBS, scaffold, or enhancer-qualifying entries
        assert len(nodes) == 3
        assert "FlyBase:FBsf0000926668" in ids   # plain regulatory_region
        assert "FlyBase:FBsf0003333333" in ids    # insulator
        assert "FlyBase:FBsf0004444444" in ids    # TSS
        # scaffold should be excluded
        assert "FlyBase:FBsf9999999999" not in ids
        # enhancers should NOT appear here (emitted by _enhancer_nodes)
        assert "FlyBase:FBsf0001111111" not in ids
        assert "FlyBase:FBsf0002222222" not in ids

    def test_library_property(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_regulatory_region",
        )
        nodes = list(adapter.get_nodes())
        insulator = [n for n in nodes if n[0] == "FlyBase:FBsf0003333333"][0]
        assert insulator[2]["library"] == "FBlc0000100"
        assert insulator[2]["feature_type"] == "insulator"
        assert insulator[2]["so_term"] == "SO:0000627"


# ---------------------------------------------------------------------------
#  Tests: Gene → TFBS edges (with BDGP6 assembly)
# ---------------------------------------------------------------------------


class TestGeneTfbsEdges:
    def test_gene_tfbs_edges(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=True,
            filepath=gff3_path,
            label="flybase_gene_tfbs",
        )
        edges = list(adapter.get_edges())
        assert len(edges) == 1
        edge = edges[0]
        assert edge[0] == "FlyBase:FBgn0002573"  # sens gene
        # TFBS ID should use BDGP6 assembly, not GRCh38
        assert edge[1] == "FLYBASE_TFBS:3R_3272_3476_BDGP6"
        assert edge[2] == "gene_tfbs"
        assert edge[3]["source_id"] == "FBsf0000226205"


# ---------------------------------------------------------------------------
#  Tests: Regulatory feature → SO edges (classified_as)
# ---------------------------------------------------------------------------


class TestRegulatoryFeatureSOEdges:
    def test_so_edges(self, gff3_path):
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_regulatory_region_so",
        )
        edges = list(adapter.get_edges())
        # All unique features across ALL types:
        # 3 regulatory_regions + 1 insulator + 1 TSS + 1 TFBS = 6
        assert len(edges) == 6
        # Check the edge label is classified_as
        for edge in edges:
            assert edge[2] == "regulatory_feature_classified_as"
        # Check SO term targets
        so_targets = {e[1][1] if isinstance(e[1], tuple) else e[1] for e in edges}
        assert "SO_0005836" in so_targets  # regulatory_region
        assert "SO_0000627" in so_targets  # insulator
        assert "SO_0000315" in so_targets  # TSS
        assert "SO_0000235" in so_targets  # TF_binding_site

    def test_tfbs_so_edge_uses_bdgp6(self, gff3_path):
        """TFBS→SO edges should use BDGP6 assembly in the TFBS ID."""
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_regulatory_region_so",
        )
        edges = list(adapter.get_edges())
        tfbs_edges = [e for e in edges if e[1] == ("sequence_type", "SO_0000235")]
        assert len(tfbs_edges) == 1
        assert "BDGP6" in tfbs_edges[0][0]
        assert "GRCh38" not in tfbs_edges[0][0]


# ---------------------------------------------------------------------------
#  Tests: Scaffold filtering
# ---------------------------------------------------------------------------


class TestScaffoldFiltering:
    def test_scaffold_excluded(self, gff3_path):
        """Entries on numeric scaffold IDs should be excluded."""
        adapter = FlyBaseRegulatoryAdapter(
            write_properties=True,
            add_provenance=False,
            filepath=gff3_path,
            label="flybase_regulatory_region",
        )
        nodes = list(adapter.get_nodes())
        ids = {n[0] for n in nodes}
        assert "FlyBase:FBsf9999999999" not in ids
