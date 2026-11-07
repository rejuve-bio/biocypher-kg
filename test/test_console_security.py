"""Console security: only this computer, only its own pages, no secrets in logs."""
import pytest
from fastapi.testclient import TestClient

from backend.api import security
from backend.api.main import app
from backend.core.console import connections, job_runner

# The console answers to localhost only; tests talk to it the way a browser would.
local = TestClient(app, base_url="http://localhost:9000")
URL = "/api/console/settings"


def test_other_host_names_are_refused():
    """DNS rebinding: a website's name pointed at 127.0.0.1 isn't this computer."""
    evil = TestClient(app, base_url="http://attacker.example:9000")
    r = evil.get(URL)
    assert r.status_code == 400 and "only answers on this computer" in r.text
    assert local.get(URL).status_code == 200


@pytest.mark.parametrize("host,ok", [
    ("localhost:9000", True), ("127.0.0.1:9000", True), ("[::1]:9000", True),
    ("console.localhost", True), ("attacker.example", False), ("127.0.0.1.nip.io", False),
])
def test_host_names(host, ok):
    assert security.allowed_host(host) is ok


def test_other_websites_cant_send_requests():
    """CSRF: browsers label requests from other sites; those are refused."""
    r = local.put(URL, json={}, headers={"Origin": "https://attacker.example"})
    assert r.status_code == 403
    r = local.get(URL, headers={"Sec-Fetch-Site": "cross-site"})
    assert r.status_code == 403
    # The console's own page, and tools on this computer (no Origin), get through.
    r = local.put(URL, json={}, headers={"Origin": "http://localhost:9000"})
    assert r.status_code != 403
    assert local.get(URL, headers={"Sec-Fetch-Site": "same-origin"}).status_code == 200


def test_responses_carry_protective_headers():
    r = local.get(URL)
    assert r.headers["x-frame-options"] == "DENY"  # not shown inside other sites
    assert "frame-ancestors 'none'" in r.headers["content-security-policy"]
    assert "script-src 'self'" in r.headers["content-security-policy"]
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["cache-control"] == "no-store"  # API answers can hold a password
    assert "access-control-allow-origin" not in r.headers  # no wildcard CORS


def test_api_docs_are_off_by_default():
    assert local.get("/docs").status_code == 404
    assert local.get("/openapi.json").status_code == 404


def test_the_neo4j_password_never_reaches_the_command_line_or_log():
    argv = ["uv", "run", "python", "kg-service/neo4j_loader.py", "--uri", "bolt://x:7687",
            "--username", "neo4j", "--password", "s3cret-pass", "--database", "neo4j"]
    clean, env = job_runner._secrets_to_env(argv)
    assert "s3cret-pass" not in " ".join(clean) and "--password" not in clean
    assert env == {"NEO4J_PASSWORD": "s3cret-pass"}
    assert job_runner._secrets_to_env(["x", "--password=abc"]) == (["x"], {"NEO4J_PASSWORD": "abc"})


def test_settings_cant_change_the_program_that_runs_builds():
    before = job_runner.settings.UV_BIN
    r = local.put(URL, json={"build": {"uv_bin": "/bin/sh", "max_build_history": 20}})
    assert r.status_code == 200
    assert job_runner.settings.UV_BIN == before
    assert "uv_bin" not in r.json()["build"]


@pytest.mark.parametrize("path", ["/", "/Users/someone", "relative/path"])
def test_the_data_folder_cant_be_a_system_or_top_level_folder(path):
    r = local.put(URL, json={"build": {"data_root": path}})
    assert r.status_code == 400


@pytest.mark.parametrize("address,warn", [
    ("bolt://lab-server:7687", True), ("http://lab-server:8027", True),
    ("neo4j+s://abc.databases.neo4j.io", False), ("bolt+ssc://lab-server:7687", False),
    ("https://lab-server:8027", False), ("bolt://localhost:7687", False),
])
def test_unencrypted_connections_to_other_computers_are_flagged(address, warn):
    assert connections.unencrypted(address) is warn
