"""Folder pickers in Settings: browse only what the console can read."""
import pytest

from backend.core.console import folders

MOUNTINFO = """\
1 0 0:1 / / rw - overlay overlay rw
2 1 0:2 / /proc rw - proc proc rw
3 1 0:3 / /dev rw - tmpfs tmpfs rw
4 1 0:4 /host/etc/hosts /etc/hosts rw - fakeowner /run/host_mark rw
5 1 0:5 /Users/a/kg-data /Users/a/kg-data rw - fakeowner /run/host_mark rw
6 1 0:6 /home/alice/dbsnp /home/alice/my\\040dbsnp rw - ext4 /dev/sda1 rw
7 1 0:7 /var/lib/docker/volumes/x /app/kg-service/.builds rw - ext4 /dev/vda1 rw
"""


def test_only_folders_you_mounted_are_starting_points():
    assert folders.mounted_folders(MOUNTINFO) == [
        "/etc/hosts",                 # a single file: dropped later by the is_dir check
        "/Users/a/kg-data",
        "/home/alice/my dbsnp",       # data under /home on Linux is kept
    ]


@pytest.fixture
def shared(tmp_path, monkeypatch):
    """A console in Docker with Documents, the data folder and one drive shared."""
    home = tmp_path / "Users" / "you"
    docs = home / "Documents"
    hsa = docs / "projects" / "hsa"
    (hsa / "gencode").mkdir(parents=True)
    (hsa / "genes.tsv").write_text("hello")
    cache = docs / "dbsnp"
    from dbsnp_cache_helper import make_dbsnp_cache
    make_dbsnp_cache(cache / "common", "common")
    (docs / ".hidden" / "hsa").mkdir(parents=True)  # hidden folders aren't searched
    data = tmp_path / "kg-data"
    (data / "input").mkdir(parents=True)
    volumes = tmp_path / "Volumes"
    (volumes / "LabDrive").mkdir(parents=True)
    (volumes / "Macintosh HD").symlink_to("/")  # what macOS puts there
    monkeypatch.setattr(folders, "in_docker", lambda: True)
    monkeypatch.setattr(folders, "_mounted", lambda: [str(docs), str(data), str(volumes)])
    monkeypatch.setattr(folders.settings, "DATA_ROOT", str(data))
    monkeypatch.setattr(folders, "VOLUMES", str(volumes))
    return {"docs": docs, "hsa": hsa, "cache": cache, "data": data, "tmp": tmp_path}


# What the browser reports for the hsa folder chosen in Finder.
HSA = [{"name": "gencode", "dir": True}, {"name": "genes.tsv", "dir": False, "size": 5}]


def test_the_shared_places(shared):
    places = folders.places()
    assert [(p["label"], p["icon"]) for p in places] == [
        ("Documents", "documents"), ("Console data folder", "data"), ("LabDrive", "drive"),
    ]  # "Macintosh HD" (a link to /) isn't a drive


def test_finds_the_folder_chosen_in_finder(shared):
    out = folders.locate("hsa", HSA, count=2)
    assert out == {"matches": [str(shared["hsa"])]}


def test_a_folder_with_the_same_name_but_other_contents_isnt_it(shared):
    other = shared["docs"] / "old" / "hsa"
    other.mkdir(parents=True)
    (other / "genes.tsv").write_text("different size")
    assert folders.locate("hsa", HSA, count=2)["matches"] == [str(shared["hsa"])]
    # Identical copies: both, so the person can say which.
    copy = shared["docs"] / "backup" / "hsa"
    (copy / "gencode").mkdir(parents=True)
    (copy / "genes.tsv").write_text("hello")
    assert sorted(folders.locate("hsa", HSA, count=2)["matches"]) == sorted(
        [str(shared["hsa"]), str(copy)])


def test_a_folder_the_console_cant_open_says_where_it_can(shared):
    with pytest.raises(folders.FolderError, match="Desktop, Documents or Downloads"):
        folders.locate("elsewhere", [], count=0)


def test_dbsnp_and_data_folder_fields_check_the_choice(shared, monkeypatch):
    common = [{"name": "common", "dir": True}]
    assert folders.locate("dbsnp", common, 1, kind="dbsnp")["matches"] == [str(shared["cache"])]
    with pytest.raises(folders.FolderError, match="No dbSNP cache here"):
        folders.locate("hsa", HSA, 2, kind="dbsnp")
    with pytest.raises(folders.FolderError, match="hsa adapters read"):
        folders.locate("hsa", HSA, 2, kind="source", species="hsa")
    monkeypatch.setattr(folders, "_writable", lambda p: str(p).startswith(str(shared["data"])))
    with pytest.raises(folders.FolderError, match="can't save in"):
        folders.locate("hsa", HSA, 2, kind="writable")
    input_dir = [{"name": "input", "dir": True}]
    assert folders.locate("kg-data", input_dir, 1, kind="writable")["matches"] == [str(shared["data"])]
