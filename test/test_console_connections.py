"""Per-species database connections: Run here (the console's own login) and Use
existing (your database, your login). No databases needed: checks are faked."""
import pytest

from backend.core.console import connections, instances, runtime_config


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    """A private settings store and two species."""
    monkeypatch.setattr(runtime_config, "store_path", lambda: tmp_path / "console_settings.json")
    monkeypatch.setattr(connections.et, "species_list", lambda: ["hsa", "dmel"])
    monkeypatch.setattr(instances.et, "species_list", lambda: ["hsa", "dmel"])


def _neo4j_ok(monkeypatch, expect=None):
    def probe(uri, user, password, database, timeout=5.0):
        if expect and (user, password) != expect:
            return False, "Wrong username or password.", "auth"
        return True, None, None
    monkeypatch.setattr(connections, "probe_neo4j", probe)


def test_use_existing_saves_its_own_login(monkeypatch):
    _neo4j_ok(monkeypatch, expect=("alice", "her-pass"))
    out = connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687",
                                       "alice", "her-pass", "kg")
    assert out["neo4j"]["source"] == "existing"
    login = connections.neo4j_login("hsa")
    assert login == {"uri": "bolt://lab-server:7687", "user": "alice",
                     "password": "her-pass", "database": "kg"}
    # The other species is untouched and has no database.
    assert connections.neo4j_login("dmel") is None


def test_use_existing_is_not_saved_when_the_login_fails(monkeypatch):
    _neo4j_ok(monkeypatch, expect=("alice", "her-pass"))
    with pytest.raises(connections.ConnectError, match="Wrong username or password"):
        connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687", "alice", "nope", "neo4j")
    assert connections.neo4j_login("hsa") is None


def test_blank_password_keeps_the_saved_one_only_for_the_same_address(monkeypatch):
    _neo4j_ok(monkeypatch, expect=("alice", "her-pass"))
    connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687", "alice", "her-pass", "neo4j")
    connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687", "alice", "", "neo4j")  # kept
    assert connections.neo4j_login("hsa")["password"] == "her-pass"
    with pytest.raises(connections.ConnectError, match="password"):
        connections.connect_existing("hsa", "neo4j", "bolt://other:7687", "alice", "", "neo4j")


def test_mork_needs_only_an_address(monkeypatch):
    monkeypatch.setattr(connections, "probe_mork", lambda url, timeout=5.0: (True, None, None))
    out = connections.connect_existing("dmel", "mork", "http://lab-server:8027")
    assert out["mork"] == {"source": "existing", "address": "http://lab-server:8027",
                           "ok": True, "error": None, "reason": None,
                           "unencrypted": True}  # another computer, plain http


def test_disconnect_clears_the_whole_login(monkeypatch):
    _neo4j_ok(monkeypatch)
    connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687", "alice", "her-pass", "neo4j")
    connections.disconnect("hsa", "neo4j")
    assert connections.neo4j_login("hsa") is None
    assert runtime_config.species_override("hsa") == {}


def test_console_login_is_only_shown_for_console_run_instances(monkeypatch):
    _neo4j_ok(monkeypatch)
    instances._wire("hsa", "neo4j", {"user": "neo4j", "password": "generated-123"})
    assert connections.console_login("hsa") == {"user": "neo4j", "password": "generated-123"}
    connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687", "alice", "her-pass", "neo4j")
    with pytest.raises(connections.ConnectError):
        connections.console_login("hsa")  # your own password is never sent back


def test_setup_login_new_data_gets_a_generated_password(monkeypatch):
    monkeypatch.setattr(instances, "_volume_exists", lambda name: False)
    monkeypatch.setattr(instances, "_self_context", lambda: {"project": "p", "network": "n"})
    login = instances._neo4j_setup_login("dmel", has_container=False)
    assert login["user"] == "neo4j" and len(login["password"]) >= 16


def test_setup_login_is_the_kept_one_never_a_guess(monkeypatch):
    monkeypatch.setattr(instances, "_volume_exists", lambda name: True)
    monkeypatch.setattr(instances, "_self_context", lambda: {"project": "p", "network": "n"})
    # Data the console has no login for is only usable through "Use existing".
    with pytest.raises(instances.InstanceError, match="Use existing"):
        instances._neo4j_setup_login("dmel", has_container=False)
    connections.save_managed_login("dmel", {"user": "neo4j", "password": "kept-pass-123"})
    assert instances._neo4j_setup_login("dmel", has_container=False)["password"] == "kept-pass-123"


def test_your_own_neo4j_needs_its_whole_login(monkeypatch):
    """No guessed username or database: Use existing takes exactly what's entered."""
    _neo4j_ok(monkeypatch)
    for user, database in (("", "neo4j"), ("alice", "")):
        with pytest.raises(connections.ConnectError, match="username and database"):
            connections.connect_existing("hsa", "neo4j", "bolt://lab:7687", user, "pw", database)
    assert connections.neo4j_login("hsa") is None


def test_remove_keeps_a_species_on_its_own_database(monkeypatch):
    _neo4j_ok(monkeypatch)
    connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687", "alice", "her-pass", "neo4j")
    monkeypatch.setattr(instances, "_containers", lambda: [{"Names": ["/neo4j-hsa"], "Labels": {}}])
    monkeypatch.setattr(instances, "_docker", lambda *a, **k: (204, None))
    instances.remove("hsa", "neo4j")  # the unused console instance
    assert connections.neo4j_login("hsa")["uri"] == "bolt://lab-server:7687"


def test_load_uses_the_species_login(monkeypatch):
    from backend.core.console.job_runner import build_load_argv

    _neo4j_ok(monkeypatch)
    connections.connect_existing("hsa", "neo4j", "bolt://lab-server:7687", "alice", "her-pass", "kg")
    argv = build_load_argv("neo4j", "/out", "hsa")
    assert argv[argv.index("--uri") + 1] == "bolt://lab-server:7687"
    assert argv[argv.index("--username") + 1] == "alice"
    assert argv[argv.index("--password") + 1] == "her-pass"
    assert argv[argv.index("--database") + 1] == "kg"
    with pytest.raises(ValueError, match="dmel has no Neo4j yet"):
        build_load_argv("neo4j", "/out", "dmel")
