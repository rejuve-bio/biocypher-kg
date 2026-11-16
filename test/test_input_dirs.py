"""Resolving adapter input paths across several input folders.

The console passes a read-only folder of data the user already has first, then its
own download folder; each relative path should resolve to the first folder that has it.
"""
from create_knowledge_graph import _maybe_prepend


def test_first_folder_holding_the_file_wins(tmp_path):
    mine, downloads = tmp_path / "mine", tmp_path / "downloads"
    (mine / "reactome").mkdir(parents=True)
    (mine / "reactome" / "a.txt").write_text("x")
    (downloads / "abc").mkdir(parents=True)
    (downloads / "abc" / "b.csv").write_text("x")
    (downloads / "reactome").mkdir()
    (downloads / "reactome" / "a.txt").write_text("y")  # downloaded too: yours wins
    roots = [mine, downloads]
    assert _maybe_prepend("reactome/a.txt", roots) == str(mine / "reactome" / "a.txt")
    assert _maybe_prepend("abc/b.csv", roots) == str(downloads / "abc" / "b.csv")


def test_missing_paths_point_at_the_first_folder(tmp_path):
    roots = [tmp_path / "mine", tmp_path / "downloads"]
    assert _maybe_prepend("uniprot/x.dat.gz", roots) == str(tmp_path / "mine" / "uniprot" / "x.dat.gz")


def test_prefix_style_paths_follow_their_source_folder(tmp_path):
    """An arg naming a file prefix (not an existing path) goes to the folder that holds
    the source's directory."""
    mine, downloads = tmp_path / "mine", tmp_path / "downloads"
    (downloads / "topld" / "AFR").mkdir(parents=True)
    assert _maybe_prepend("topld/AFR/AFR_chr", [mine, downloads]) == str(
        downloads / "topld" / "AFR" / "AFR_chr"
    )


def test_absolute_and_dot_paths_are_untouched(tmp_path):
    assert _maybe_prepend("/abs/x", [tmp_path]) == "/abs/x"
    assert _maybe_prepend("./aux_files/x", [tmp_path]) == "./aux_files/x"


def test_single_dir_still_works(tmp_path):
    assert _maybe_prepend("abc/x", tmp_path) == str(tmp_path / "abc" / "x")
