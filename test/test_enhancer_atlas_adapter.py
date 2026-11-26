"""Unit tests for biocypher_metta.adapters.enhancer_atlas_adapter.

The tests build their own small 'lifted' folders in a temporary directory (the layout written by lift_enhancer_atlas.py), so they need no data files:
  bed/<tissue>.bed            chr, start (0-based), end, signal, ...
  enhancer_gene/<tissue>_EP.txt   chr:start-end_GENE$symbol$gene_chr$TSS$strand <TAB> score   (CRLF line ends, as in the original files)
  ep_only_nodes.bed           intervals that are in no BED (used only when tissue_in_id is false)
"""
import logging
import pickle

import pytest

from biocypher_metta.adapters.enhancer_atlas_adapter import ACTIVE_IN_LABELS, EnhancerAtlasAdapter

MODULE = "biocypher_metta.adapters.enhancer_atlas_adapter"


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, newline="")


@pytest.fixture
def human(tmp_path):
    root = tmp_path / "hsa"
    _write(root / "bed" / "T1.bed", "chr1\t100\t200\t5.1\tchr1\t90\t190\nchr1\t2100\t2700\t6.2\tchr1\t2000\t2600\nchr9\t50\t150\t9.5\tchr4\t50\t150\n")
    _write(root / "bed" / "T2.bed", "chr1\t2100\t2700\t1.1\tchr1\t2000\t2600\nchr1\t5100\t5200\t2.2\tchr1\t5000\t5100\n")
    _write(root / "bed" / "Th1.bed", "chr1\t5100\t5200\t3.3\tchr1\t5000\t5100\n")
    _write(root / "bed" / "T3.bed", "chr1\t100\t200\t4.4\tchr1\t90\t190\n")          # its term is an EFO one: no node type, skipped
    _write(root / "bed" / "T4.bed", "chr1\t100\t200\t4.4\tchr1\t90\t190\n")          # no term at all: skipped
    _write(root / "enhancer_gene" / "T1_EP.txt",
           "chr1:100-200_ENSG1$G1$chr1$900$+\t1.5\r\n"
           "chr1:7100-7200_ENSG4$G4$chr3$40$+\t1.8\r\n"                                # interval that is in no BED of T1
           "chr15_KI270727v1_random:1000-2000_ENSG9$X$chr15_KI270727v1_random$5000$+\t1.2\r\n")   # contig name with underscores
    _write(root / "enhancer_gene" / "th1_EP.txt", "chr1:5100-5200_ENSG1$G1$chr1$900$+\t2.0\r\n")      # file name differs in case from the BED (Th1)
    _write(root / "enhancer_gene" / "T2_EP.txt", "chr1:5100-5200_ENSG2$G2$chr1$100$+\t2.1\r\n")
    _write(root / "ep_only_nodes.bed", "chr1\t7100\t7200\tep_only\tchr1\t7000\t7100\nchr15_KI270727v1_random\t1000\t2000\tep_only\tchr15\t1000\t2000\n")
    with open(root / "tissues.pkl", "wb") as f:
        pickle.dump({"T1": "UBERON_0001", "T2": "CLO_2", "th1": "CL_3", "T3": "EFO_0000222"}, f)
    return root


def make(root, label, taxon=9606, assembly="GRCh38", write_properties=True, **kw):
    return EnhancerAtlasAdapter(str(root / "bed"), str(root / "enhancer_gene"), str(root / "ep_only_nodes.bed"), str(root / "tissues.pkl"),
                                write_properties, True, taxon, assembly, label, **kw)


def nodes(root, **kw):
    return list(make(root, "enhancer", **kw).get_nodes())


def edges(root, label, **kw):
    return list(make(root, label, **kw).get_edges())


def all_edges(root, **kw):
    return {label: edges(root, label, **kw) for label in ["enhancer_gene", *ACTIVE_IN_LABELS]}


# ---------------------------------------------------------------- nodes
def test_node_ids_carry_assembly_taxon_and_tissue(human):
    ids = [n[0] for n in nodes(human)]
    assert len(ids) == len(set(ids)) == 10                       # 8 BED records + 2 intervals that only an EP file lists
    assert "SO:chr1_101_200_GRCh38_9606_T1" in ids               # BED start 100 is 0-based: the ID uses 101
    assert "SO:chr1_7101_7200_GRCh38_9606_T1" in ids             # created from the EP file of T1
    assert "SO:chr15_KI270727v1_random_1001_2000_GRCh38_9606_T1" in ids


def test_node_properties(human):
    nid, label, props = next(n for n in nodes(human) if n[0] == "SO:chr1_101_200_GRCh38_9606_T1")
    assert label == "enhancer"
    assert props["chr"] == "chr1" and props["start"] == 101 and props["end"] == 200 and props["taxon_id"] == 9606
    assert props["source"] == "Enhancer Atlas" and props["source_url"].startswith("http")


def test_tissue_token_is_the_bed_file_name_even_for_a_differently_cased_ep_file(human):
    ids = {n[0] for n in nodes(human)}
    assert "SO:chr1_5101_5200_GRCh38_9606_Th1" in ids            # the EP file is th1_EP.txt
    assert not any(i.endswith("_th1") for i in ids)


def test_without_tissue_in_the_id_identical_intervals_collapse(human):
    ids = [n[0] for n in nodes(human, tissue_in_id=False)]
    assert len(ids) == len(set(ids)) == 6                        # 4 unique BED intervals + the 2 ep_only intervals
    assert "SO:chr1_101_200_GRCh38_9606" in ids


def test_write_properties_false_gives_empty_properties(human):
    assert all(n[2] == {} for n in nodes(human, write_properties=False))


def test_region_filter(human):
    ids = [n[0] for n in nodes(human, chr="chr1", start=1, end=3000)]
    assert ids and all(i.startswith("SO:chr1_") for i in ids)
    assert not any("chr9" in i or "KI270727" in i for i in ids)


# ---------------------------------------------------------------- enhancer-gene edges
@pytest.mark.parametrize("tissue_in_id", [True, False])
def test_every_edge_starts_at_an_emitted_node(human, tissue_in_id):
    ids = {n[0] for n in nodes(human, tissue_in_id=tissue_in_id)}
    for label, es in all_edges(human, tissue_in_id=tissue_in_id).items():
        assert all(e[0] in ids for e in es), label


def test_enhancer_gene_edges(human):
    es = edges(human, "enhancer_gene")
    assert len(es) == 5
    src, dst, label, props = next(e for e in es if e[1] == "ENSEMBL:ENSG9")
    assert src == "SO:chr15_KI270727v1_random_1001_2000_GRCh38_9606_T1"      # contig names with '_' are parsed
    assert label == "enhancer_gene" and props["score"] == pytest.approx(1.2)
    assert props["biological_context"] == "UBERON_0001"
    th1 = next(e for e in es if e[0].endswith("_Th1"))
    assert th1[3]["biological_context"] == "CL_3"                              # case-insensitive lookup in the pickle


# ---------------------------------------------------------------- active-in edges
def test_active_in_edges_are_split_by_target_node_type(human):
    ai = all_edges(human)
    assert len(ai["enhancer_anatomy"]) == 3 and {e[1] for e in ai["enhancer_anatomy"]} == {"UBERON:0001"}
    assert len(ai["enhancer_cell_line"]) == 2 and {e[1] for e in ai["enhancer_cell_line"]} == {"CLO:2"}
    assert len(ai["enhancer_cell_type"]) == 1 and {e[1] for e in ai["enhancer_cell_type"]} == {"CL:3"}
    assert ai["enhancer_tissue"] == [] and ai["enhancer_developmental_stage"] == []
    src, dst, label, props = ai["enhancer_anatomy"][0]
    assert label == "enhancer_anatomy" and "signal" in props and props["taxon_id"] == 9606


def test_tissues_without_a_usable_term_are_skipped_and_reported_once(human, caplog):
    with caplog.at_level(logging.WARNING, logger=MODULE):
        edges(human, "enhancer_anatomy")
        edges(human, "enhancer_cell_line")                        # a second record must not repeat the warnings
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "no ontology term for BED tissue 'T4'" in text
    assert "no node type for EFO:0000222" in text
    assert text.count("'T4'") == 1


# ---------------------------------------------------------------- errors
def test_unsupported_taxon_assembly_pairs_are_refused(human):
    for taxon, assembly in ((9606, "hg19"), (7227, "GRCh38"), (10090, "GRCm39")):
        with pytest.raises(ValueError):
            make(human, "enhancer", taxon=taxon, assembly=assembly)


def test_unknown_edge_label_is_refused(human):
    with pytest.raises(ValueError):
        list(make(human, "active_in").get_edges())


def test_tissue_names_that_collide_in_lowercase_are_refused(human):
    with open(human / "tissues.pkl", "wb") as f:
        pickle.dump({"HeLa-S3": "CLO_1", "Hela-S3": "CLO_2"}, f)
    with pytest.raises(ValueError):
        make(human, "enhancer")


# ---------------------------------------------------------------- fly
def test_fly_ids_use_flybase_names_and_dm6(tmp_path):
    root = tmp_path / "dmel"
    _write(root / "bed" / "S2.bed", "2L\t100\t200\t5.1\t2L\t90\t190\nUnmapped_Scaffold_37\t5\t50\t7.3\tchrU\t5\t50\n")
    _write(root / "enhancer_gene" / "S2_EP.txt",
           "2L:100-200_FBgn0000001$a$2L$900\t21.5\r\nUnmapped_Scaffold_37:5-50_FBgn0000003$c$2L$1500\t22.5\r\n")
    _write(root / "ep_only_nodes.bed", "")
    with open(root / "tissues.pkl", "wb") as f:
        pickle.dump({"S2": "FBbt_00005737"}, f)
    ids = {n[0] for n in nodes(root, taxon=7227, assembly="dm6")}
    assert ids == {"SO:2L_101_200_dm6_7227_S2", "SO:Unmapped_Scaffold_37_6_50_dm6_7227_S2"}
    es = edges(root, "enhancer_gene", taxon=7227, assembly="dm6")
    assert {e[0] for e in es} == ids and all(e[1].startswith("ENSEMBL:FBgn") for e in es)
    anatomy = edges(root, "enhancer_anatomy", taxon=7227, assembly="dm6")
    assert len(anatomy) == 2 and {e[1] for e in anatomy} == {"FBbt:00005737"}
