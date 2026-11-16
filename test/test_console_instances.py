"""Console-managed Neo4j/MORK instances: what gets created (no Docker needed).

Instances must match what older docker/up.sh versions created (names, ports,
volumes), so databases made that way keep their data when the console runs them.
"""
import pytest

from backend.core.config import settings
from backend.core.console import instances

CTX = {"project": "biocypher-console", "network": "biocypher-console_default"}


@pytest.fixture(autouse=True)
def _species(monkeypatch):
    monkeypatch.setattr(instances.et, "species_list", lambda: ["hsa", "dmel"])


LOGIN = {"user": "neo4j", "password": "s3cretpass"}


def test_neo4j_spec_matches_up_sh(monkeypatch):
    monkeypatch.setattr(settings, "DATA_ROOT", "/data/kg")
    spec = instances.container_spec("dmel", "neo4j", CTX, "neo4j:5.21", LOGIN)
    assert "NEO4J_AUTH=neo4j/s3cretpass" in spec["Env"]
    ports = spec["HostConfig"]["PortBindings"]
    assert ports["7687/tcp"][0] == {"HostIp": "127.0.0.1", "HostPort": "7688"}  # base + index
    assert ports["7474/tcp"][0]["HostPort"] == "7475"
    binds = spec["HostConfig"]["Binds"]
    assert "biocypher-console_neo4j_data_dmel:/data" in binds
    assert "/data/kg:/data/kg:ro" in binds  # loaders import build output from here
    aliases = spec["NetworkingConfig"]["EndpointsConfig"]["biocypher-console_default"]["Aliases"]
    assert aliases == ["neo4j-dmel"]


def test_neo4j_needs_a_real_password():
    with pytest.raises(instances.InstanceError, match="password"):
        instances.container_spec("hsa", "neo4j", CTX, "neo4j:5.21",
                                 {"user": "neo4j", "password": "short"})


def test_mork_spec_and_internal_urls(monkeypatch):
    monkeypatch.setattr(settings, "DATA_ROOT", "/data/kg")
    spec = instances.container_spec("hsa", "mork", CTX, "biocypher-console-mork:latest")
    # No port on this computer: MORK takes no password (the console uses its network).
    assert "PortBindings" not in spec["HostConfig"]
    assert spec["HostConfig"]["SecurityOpt"] == ["no-new-privileges:true"]
    assert "biocypher-console_mork_persist_hsa:/app/persist" in spec["HostConfig"]["Binds"]
    assert instances.internal_url("hsa", "mork") == "http://mork-hsa:8027"
    assert instances.internal_url("dmel", "neo4j") == "bolt://neo4j-dmel:7687"


def test_mork_image_prefers_what_up_sh_built():
    images = [
        {"RepoTags": ["biocypher-console-mork:latest"]},
        {"RepoTags": ["biocypher-console-mork-hsa:latest"]},
        {"RepoTags": ["neo4j:5.21"]},
    ]
    assert instances._mork_image("hsa", images) == "biocypher-console-mork-hsa:latest"
    assert instances._mork_image("dmel", images) == "biocypher-console-mork:latest"
    assert instances._mork_image("hsa", [{"RepoTags": ["neo4j:5.21"]}]) is None


def test_unknown_species_is_rejected():
    with pytest.raises(instances.InstanceError, match="Unknown species"):
        instances.host_ports("zzz", "neo4j")


def test_finds_instances_made_by_compose_or_up_sh():
    """up.sh/compose name containers <project>-<service>-1; they still count."""
    console_made = {"Names": ["/mork-hsa"], "Labels": {}}
    compose_made = {"Names": ["/biocypher-console-neo4j-hsa-1"],
                    "Labels": {"com.docker.compose.service": "neo4j-hsa"}}
    containers = [console_made, compose_made]
    assert instances._find("hsa", "mork", containers) is console_made
    assert instances._find("hsa", "neo4j", containers) is compose_made
    assert instances._name_of(compose_made) == "biocypher-console-neo4j-hsa-1"
    assert instances._find("dmel", "neo4j", containers) is None


def test_taken_host_ports_shift_to_the_next_block():
    spec = instances.container_spec("hsa", "neo4j", CTX, "neo4j:5.21", LOGIN)
    instances._shift_ports(spec, 10)
    ports = spec["HostConfig"]["PortBindings"]
    assert ports["7474/tcp"][0]["HostPort"] == "7484"
    assert ports["7687/tcp"][0]["HostPort"] == "7697"


def test_container_log_frames_are_stripped():
    """Docker multiplexes non-TTY logs with 8-byte frame headers."""
    def frame(stream, text):
        body = text.encode()
        return bytes([stream, 0, 0, 0]) + len(body).to_bytes(4, "big") + body

    raw = frame(1, "Starting...\n") + frame(2, "ERROR bad config\n")
    assert instances._demux(raw) == "Starting...\nERROR bad config\n"
    assert instances._demux(b"plain tty output\n") == "plain tty output\n"


def _fake_container(state="running"):
    return {"Names": ["/neo4j-hsa"], "Labels": {}, "State": state,
            "Ports": [{"PrivatePort": 7474, "PublicPort": 7474}]}


def test_setup_waits_until_the_instance_answers(monkeypatch):
    """Started is not ready: don't report connected until it answers."""
    answers = iter([False, False, True])
    monkeypatch.setattr(instances, "_containers", lambda: [_fake_container()])
    monkeypatch.setattr(instances, "_answers", lambda host, port: next(answers))
    monkeypatch.setattr(instances.time, "sleep", lambda s: None)
    instances._wait_ready("hsa", "neo4j", "neo4j-hsa")  # returns once it answers
    assert next(answers, "done") == "done"


def test_setup_reports_a_crash_while_starting(monkeypatch, caplog):
    """Plain words for the UI; the instance's own log lines go to the server log."""
    monkeypatch.setattr(instances, "_containers", lambda: [_fake_container("restarting")])
    monkeypatch.setattr(instances, "_last_log", lambda name: " Last log lines: boom")
    with pytest.raises(instances.InstanceError, match="stopped while starting") as err:
        instances._wait_ready("hsa", "neo4j", "neo4j-hsa")
    assert "boom" not in str(err.value) and "boom" in caplog.text
