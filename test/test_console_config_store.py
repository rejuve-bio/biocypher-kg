"""Each console instance's own config (config_store): shipped config plus its edits."""
import pytest

from backend.core.console import config_store as cs


@pytest.fixture
def dirs(monkeypatch, tmp_path):
    shipped, live = tmp_path / ".config-shipped", tmp_path / "config"
    (shipped / "hsa").mkdir(parents=True)
    live.mkdir()
    (shipped / "species_config.yaml").write_text("hsa: {}\n")
    (shipped / "hsa" / "hsa_schema_config.yaml").write_text("gene: v1\n")
    monkeypatch.setattr(cs, "shipped_dir", lambda: shipped)
    monkeypatch.setattr(cs, "live_dir", lambda: live)
    return shipped, live


def test_edits_stay_and_everything_else_follows_the_shipped_config(dirs):
    shipped, live = dirs
    schema = live / "hsa" / "hsa_schema_config.yaml"
    assert sorted(cs.sync()["added"]) == ["hsa/hsa_schema_config.yaml", "species_config.yaml"]
    assert cs.status(schema) == {"managed": True, "edited": False, "shipped_changed": False,
                                 "edited_at": None}

    schema.write_text("gene: mine\n")  # edited in the Config page
    cs.mark_edited(schema)
    assert cs.status(schema)["edited"] and not cs.status(schema)["shipped_changed"]

    # An image update: new shipped versions, one file no longer shipped.
    (shipped / "hsa" / "hsa_schema_config.yaml").write_text("gene: v2\n")
    (shipped / "species_config.yaml").write_text("hsa: {}\ndmel: {}\n")
    (live / "old_config.yaml").write_text("gone upstream\n")
    changed = cs.sync()
    assert changed["kept"] == ["hsa/hsa_schema_config.yaml"]
    assert changed["updated"] == ["species_config.yaml"] and changed["removed"] == ["old_config.yaml"]
    assert schema.read_text() == "gene: mine\n"  # this instance's edit is kept
    assert cs.status(schema)["shipped_changed"]   # and it says the shipped one moved on

    cs.reset(schema)
    assert schema.read_text() == "gene: v2\n" and not cs.status(schema)["edited"]


def test_saving_the_shipped_content_back_is_not_an_edit(dirs):
    _, live = dirs
    cs.sync()
    schema = live / "hsa" / "hsa_schema_config.yaml"
    schema.write_text("gene: mine\n")
    cs.mark_edited(schema)
    schema.write_text("gene: v1\n")
    cs.mark_edited(schema)
    assert not cs.status(schema)["edited"]


def test_a_checkout_without_a_shipped_copy_edits_its_own_config(monkeypatch, tmp_path):
    monkeypatch.setattr(cs, "shipped_dir", lambda: tmp_path / "missing")
    monkeypatch.setattr(cs, "live_dir", lambda: tmp_path)
    assert cs.sync() == {"updated": [], "added": [], "removed": [], "kept": []}
    assert cs.status(tmp_path / "x.yaml")["managed"] is False
