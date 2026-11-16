"""Where the downloader puts files, checked against the paths the adapters read."""
import io
import tarfile
from pathlib import Path

import yaml

from biocypher_dataset_downloader.download_manager import (
    DownloadManager, _is_tar, _parse_url_comment, _post_processed_path, _should_skip_extract,
)

ROOT = Path(__file__).resolve().parents[1]


def _manager(tmp_path, sources):
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(yaml.safe_dump(sources))
    out = tmp_path / "out"
    mgr = DownloadManager(str(cfg), out, sample_fraction=0)
    mgr._head_metadata = lambda url: None
    mgr._already_downloaded = lambda path, url: False
    return mgr, out


def _tar_gz(members: dict, links=()) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        for name, target in links:
            info = tarfile.TarInfo(name)
            info.type, info.linkname = tarfile.SYMTYPE, target
            tf.addfile(info)
    return buf.getvalue()


def test_a_tar_archive_is_unpacked_beside_it_and_nothing_escapes(tmp_path):
    archive = _tar_gz({"pwm/A.pwm": b">A\n1\t0\t0\t0\n", "../escaped.txt": b"x"},
                      links=[("pwm/link.pwm", "/etc/passwd")])
    files = {"motifs.tar.gz": archive, "annotation.tsv": b"model\ttf\n"}
    src = {"hocomoco": {"url": ["https://x/annotation.tsv", "https://x/motifs.tar.gz"]}}
    mgr, out = _manager(tmp_path, src)
    mgr._download_file = lambda url, path, *a, **k: path.write_bytes(files[url.rsplit("/", 1)[1]]) or True

    mgr.process_download("hocomoco", src["hocomoco"])

    assert (out / "hocomoco" / "pwm" / "A.pwm").read_bytes().startswith(b">A")
    assert (out / "hocomoco" / "annotation.tsv").exists()
    assert not (out / "hocomoco" / "motifs.tar.gz").exists()  # the archive is gone
    assert not (out / "escaped.txt").exists() and not (tmp_path / "escaped.txt").exists()
    assert not (out / "hocomoco" / "pwm" / "link.pwm").exists()


def _planned(species: str, out: Path) -> set:
    """Every path (relative to the input folder) this species' url downloads land at,
    using the downloader's own rules, without fetching anything."""
    cfg = yaml.safe_load((ROOT / f"config/{species}/{species}_data_source_config.yaml").read_text())
    mgr = DownloadManager.__new__(DownloadManager)
    mgr.output_dir = out  # _process_urls makes the (empty) folders
    landed = set()

    def fake_one(url, path, extract, compress=None, move_to_dest=None):
        _, comment = _parse_url_comment(url)
        dest = _post_processed_path(path, extract, compress, comment, move_to_dest)
        if extract and _is_tar(dest) and not _should_skip_extract(comment):
            dest = dest.parent / "<unpacked>"
        landed.add(str(dest.relative_to(out)) if dest.is_relative_to(out) else str(dest))
        return 0, 0, [], 0

    mgr._download_one = fake_one
    mgr._resolve_move_to = lambda dest: Path("/cwd") / dest
    for key, entry in cfg.items():
        if isinstance(entry, dict) and "url" in entry:
            move_to = entry.get("move_to") or {}
            if isinstance(move_to, list):
                move_to = {k: v for d in move_to for k, v in d.items()}
            mgr._process_urls(entry["url"], key, extract=entry.get("extract", True),
                              compress=entry.get("compress"), move_to=move_to)
    return landed


def test_hsa_downloads_land_where_the_adapters_read_them(tmp_path):
    planned = _planned("hsa", tmp_path)
    for path in [
        "gtex/eqtl/gtex.forgedb.csv.gz",                             # gtex_eqtl, gtex_expression
        "catlasv1/cCRE_hg38.tsv.gz",                                  # catlas adapters
        "catlasv1/Cell_ontology.tsv",
        "hocomoco/HOCOMOCOv11_core_annotation_HUMAN_mono.tsv",        # hocomoco
        "hocomoco/<unpacked>",                                         # -> hocomoco/pwm/*.pwm
        "reactome/reactome_reaction_exporter_All_species.txt",        # reaction adapters
        "forgedb/roadmap/dhs/forge2.erc2-DHS.forgedb.csv.gz",         # roadmap_dhs
        "forgedb/roadmap/chromatin_state/forge2.erc2-chromatin15state-all.0.forgedb.csv.gz",
        "forgedb/roadmap/h3_marks/forge2.erc2-H3-all.9.forgedb.csv.gz",
        "alliance/ORTHOLOGY-ALLIANCE_COMBINED.tsv.gz",
    ]:
        assert path in planned, path


def test_dmel_flybase_files_stay_where_the_adapters_read_them(tmp_path):
    planned = _planned("dmel", tmp_path)
    assert "flybase/fbal_to_fbgn_fb_2026_02.tsv.gz" in planned        # dmel_allele & co.
    assert "eva/7227_GCA_000001215.4_current_ids.vcf.gz" in planned  # eva_snp


SPECIES = ["hsa", "mmu", "rno", "dmel", "cel"]


def _urls(species: str, key: str) -> list:
    from biocypher_dataset_downloader.versioning.base import iter_source_urls
    cfg = yaml.safe_load((ROOT / f"config/{species}/{species}_data_source_config.yaml").read_text())
    return [u.split("#")[0].strip() for u in iter_source_urls(cfg.get(key) or {})]


def _assembly(url: str):
    """The genome build a download is on, from its link."""
    import re
    if "Gencode_human" in url:
        return "GRCh38"
    if "Gencode_mouse" in url:
        return "GRCm39"
    m = re.search(r"(GRCh3[78]|GRCm3[89]|GRCr8|mRatBN7\.2|WBcel235|BDGP6)", url)
    return m.group(1) if m else None


def test_each_species_gene_and_rna_positions_are_on_one_genome_build():
    """Rat once had genes on GRCr8 (Ensembl) and ncRNAs on mRatBN7.2 (RNAcentral), so
    their positions didn't line up (and the overlap links between them were wrong)."""
    for sp in SPECIES:
        gtf = [u for k in ("gencode", "ensembl") for u in _urls(sp, k) if ".gtf" in u]
        rna = [u for u in _urls(sp, "rna_central") if u.endswith(".bed.gz")]
        assert gtf and rna, sp
        assert {_assembly(u) for u in gtf} == {_assembly(u) for u in rna}, (sp, gtf, rna)


def test_versioned_ensembl_files_come_from_their_release_folder():
    """current/ only holds the newest release: a versioned file name under it breaks
    as soon as Ensembl publishes the next one."""
    for sp in SPECIES:
        for key in ("ensembl", "gencode"):
            for u in _urls(sp, key):
                if ("ensembl.org" in u or "ensemblgenomes" in u) and ".gtf" in u:
                    assert "/current/" not in u, (sp, u)


def test_reactome_rows_without_a_species_prefix_stay_with_their_own_species(tmp_path):
    """R-NUL events carry no species prefix; each row names its organism. Every species
    used to get every R-NUL row (cow, chicken, ...)."""
    from biocypher_metta.adapters.reactome_edges_adapter import ReactomeEdgesAdapter

    f = tmp_path / "ChEBI2ReactomeReactions.txt"
    f.write_text("".join(f"{chebi}\t{event}\turl\tname\tTAS\t{organism}\n" for chebi, event, organism in [
        ("15377", "R-NUL-1", "Mus musculus"),
        ("15378", "R-NUL-2", "Bos taurus"),
        ("15379", "R-NUL-3", "Homo sapiens"),
        ("15380", "R-MMU-4", "Mus musculus"),
        ("15381", "R-HSA-5", "Homo sapiens"),
    ]))

    def reactions(taxon_id):
        a = ReactomeEdgesAdapter.__new__(ReactomeEdgesAdapter)  # skip the UniProt id map
        a.filepath, a.label, a.taxon_id = str(f), "small_molecule_to_reaction", taxon_id
        a.write_properties, a.add_provenance = True, False
        a.source, a.source_url, a.ensembl_uniprot_map = "REACTOME", "https://reactome.org", {}
        return sorted(edge[1] for edge in a.get_edges())

    assert reactions(10090) == ["R-MMU-4", "R-NUL-1"]
    assert reactions(9606) == ["R-HSA-5", "R-NUL-3"]
    assert reactions(6239) == []
