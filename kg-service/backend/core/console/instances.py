"""Console-managed database instances: one Neo4j and one MORK per species.

The console normally only connects to databases. When it can reach the Docker Engine
(the Docker socket mounted into its container), it can also create, start and stop
each species' Neo4j and MORK on its own Docker network and wire the per-species
connection settings, so a researcher sets everything up from Settings, without the
repo or an env file.

Instances mirror what docker/up.sh creates (names, ports, volumes), so the two
approaches share data: neo4j-<sp> / mork-<sp>, host ports base + species index, and
<project>_neo4j_data_<sp> / <project>_mork_persist_<sp> volumes.

Docker's Engine API is plain JSON over HTTP on a Unix socket, so this uses the
standard library rather than a Docker SDK dependency.
"""
from __future__ import annotations

import http.client
import json
import logging
import os
import socket
import threading
import time
from typing import Optional
from urllib.parse import quote

from backend.core.config import settings
from backend.core.console import explore_targets as et

logger = logging.getLogger(__name__)

DOCKER_SOCK = os.environ.get("DOCKER_SOCK", "/var/run/docker.sock")
KINDS = ("neo4j", "mork")
NEO4J_IMAGE = os.environ.get("NEO4J_IMAGE", "neo4j:5.21")
# Host ports: base + the species' index, matching docker/up.sh.
NEO4J_BOLT_BASE, NEO4J_HTTP_BASE, MORK_PORT_BASE = 7687, 7474, 8432


class InstanceError(Exception):
    """A problem worth showing to the user, in plain words (no Docker terms)."""


# Shown when the console can't use Docker at all. The researcher can't fix that from
# the console, but can still connect databases they already have.
NO_DOCKER = "This console can't run databases on this computer. Use existing ones instead."
LABELS = {"neo4j": "Neo4j", "mork": "MORK"}


def _fail(message: str, detail: str = "") -> InstanceError:
    """A plain message for the UI; the technical detail goes to the server log."""
    if detail:
        logger.warning("%s (%s)", message, detail)
    return InstanceError(message)


# ── Minimal Docker Engine API client over the Unix socket ─────────────────────

class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float):
        super().__init__("localhost", timeout=timeout)
        self._sock_path = path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._sock_path)
        self.sock = sock


def _docker(method: str, path: str, body: Optional[dict] = None, timeout: float = 30,
            raw: bool = False):
    """Call the Engine API (unversioned path = the daemon's current API version).
    ``raw`` returns the body as bytes (container logs are a binary stream)."""
    conn = _UnixHTTPConnection(DOCKER_SOCK, timeout)
    try:
        headers = {"Content-Type": "application/json"} if body is not None else {}
        conn.request(method, path, json.dumps(body) if body is not None else None, headers)
        resp = conn.getresponse()
        body_bytes = resp.read()
    except OSError as exc:
        raise _fail(NO_DOCKER, f"Docker at {DOCKER_SOCK}: {exc}") from exc
    finally:
        conn.close()
    if raw:
        return resp.status, body_bytes
    data = None
    if body_bytes:
        try:
            data = json.loads(body_bytes)
        except ValueError:
            data = body_bytes.decode(errors="replace")  # streamed endpoints (image pull)
    return resp.status, data


def _error(data) -> str:
    return data.get("message") if isinstance(data, dict) else str(data)


def docker_available() -> bool:
    if not os.path.exists(DOCKER_SOCK):
        return False
    try:
        status, _ = _docker("GET", "/_ping", timeout=5)
        return status == 200
    except InstanceError:
        return False


def _self_context() -> dict:
    """The console's own compose project and Docker network, so instances join it
    and the console reaches them by name (neo4j-hsa, mork-hsa)."""
    status, data = _docker("GET", f"/containers/{quote(socket.gethostname())}/json")
    if status != 200:
        raise _fail(NO_DOCKER, "can't find the console's own container to share its network")
    labels = (data.get("Config") or {}).get("Labels") or {}
    networks = list(((data.get("NetworkSettings") or {}).get("Networks") or {}).keys())
    if not networks:
        raise _fail(NO_DOCKER, "the console's container has no Docker network to share")
    return {"project": labels.get("com.docker.compose.project") or "biocypher-console",
            "network": networks[0]}


# ── What each instance looks like ──────────────────────────────────────────────

def _species_index(species: str) -> int:
    names = et.species_list()
    if species not in names:
        raise InstanceError(f"Unknown species '{species}'.")
    return names.index(species)


def container_name(species: str, kind: str) -> str:
    return f"{kind}-{species}"


def host_ports(species: str, kind: str) -> dict:
    i = _species_index(species)
    if kind == "neo4j":
        return {"bolt": NEO4J_BOLT_BASE + i, "http": NEO4J_HTTP_BASE + i}
    return {"http": MORK_PORT_BASE + i}


def internal_url(species: str, kind: str) -> str:
    """How the console reaches the instance on the shared network."""
    name = container_name(species, kind)
    return f"bolt://{name}:7687" if kind == "neo4j" else f"http://{name}:8027"


def _mork_image(species: str, images: list) -> Optional[str]:
    """A locally available MORK image: MORK_IMAGE if set, else what up.sh built."""
    if os.environ.get("MORK_IMAGE"):
        return os.environ["MORK_IMAGE"]
    tags = [t for img in images for t in (img.get("RepoTags") or []) if t and "<none>" not in t]
    for wanted in (f"biocypher-console-mork-{species}:latest", "biocypher-console-mork:latest"):
        if wanted in tags:
            return wanted
    return next((t for t in tags if "mork" in t.split(":")[0]), None)


def build_folders() -> list:
    """The folders a database the console runs reads builds from, read-only, at the
    same path as in the console: the console data folder and the Builds folder (once,
    when one holds the other). Settings decides both, so a change re-creates the
    databases (refresh_in_background)."""
    from backend.core.console import runtime_config

    out: list = []
    for folder in (settings.DATA_ROOT, runtime_config.builds_folder()):
        if not folder:
            continue
        f = os.path.normpath(folder)
        if any(f == o or f.startswith(o.rstrip("/") + "/") for o in out):
            continue
        out = [o for o in out if not o.startswith(f.rstrip("/") + "/")] + [f]
    return sorted(out)


def _memory() -> Optional[str]:
    """Neo4j memory from Settings ("2G"), or None: it's required, with no default."""
    m = str(settings.NEO4J_MEMORY or "")
    return m if m[:-1].isdigit() and m.endswith("G") else None


NO_MEMORY = "set Neo4j memory in Settings first"


def container_spec(species: str, kind: str, ctx: dict, image: str,
                   login: Optional[dict] = None) -> dict:
    """The Engine API create body for a species' Neo4j or MORK. ``login`` is the
    Neo4j login to create it with (see _neo4j_setup_login)."""
    name = container_name(species, kind)
    project, sp = ctx["project"], species
    folders = [f"{f}:{f}:ro" for f in build_folders()]  # loaders read builds here
    ports = host_ports(species, kind)
    labels = {"kg-console.managed": "true", "kg-console.species": sp, "kg-console.kind": kind}
    networking = {"EndpointsConfig": {ctx["network"]: {"Aliases": [name]}}}
    restart = {"Name": "unless-stopped"}
    if kind == "neo4j":
        if not _memory():
            raise InstanceError(NO_MEMORY)
        password = (login or {}).get("password") or ""
        if len(password) < 8:
            # Neo4j refuses shorter initial passwords and would exit on start.
            raise InstanceError("Neo4j needs a password of at least 8 characters.")
        env = {
            # Neo4j's initial user is always "neo4j"; the password is the console's own.
            "NEO4J_AUTH": f"neo4j/{password}",
            "NEO4JLABS_PLUGINS": '["apoc"]',
            # Loads only import; Cypher writing files isn't needed, so it's off.
            "NEO4J_apoc_export_file_enabled": "false",
            "NEO4J_apoc_import_file_enabled": "true",
            "NEO4J_apoc_import_file_use__neo4j__config": "false",
            "NEO4J_dbms_security_procedures_unrestricted": "apoc.*",
            "NEO4J_dbms_security_allow__csv__import__from__file__urls": "true",
            "NEO4J_server_directories_import": "/",
            # Settings > Build settings > Neo4j memory.
            "NEO4J_server_memory_pagecache_size": _memory(),
            "NEO4J_server_memory_heap_max__size": _memory(),
        }
        binds = [f"{project}_neo4j_data_{sp}:/data", f"{project}_neo4j_logs_{sp}:/logs"] + folders
        return {
            "Image": image,
            "Env": [f"{k}={v}" for k, v in env.items()],
            "ExposedPorts": {"7687/tcp": {}, "7474/tcp": {}},
            "Labels": labels,
            "HostConfig": {
                "PortBindings": {
                    "7687/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(ports["bolt"])}],
                    "7474/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(ports["http"])}],
                },
                "Binds": binds,
                "RestartPolicy": restart,
                # Nothing inside may gain more rights than it started with (tested:
                # Neo4j still starts and drops to its own user).
                "SecurityOpt": ["no-new-privileges:true"],
            },
            "NetworkingConfig": networking,
        }
    binds = [f"{project}_mork_persist_{sp}:/app/persist"] + folders
    # No port on this computer: MORK takes no password, so anything that could reach
    # it (any program, or a web page through the browser) could change its data.
    # The console reaches it on the private network it shares with it.
    return {
        "Image": image,
        "Env": ["RUST_LOG=info", "SNAPSHOT_DIR=/app/persist", "SNAPSHOT_INTERVAL_SECONDS=300"],
        "ExposedPorts": {"8027/tcp": {}},
        "Labels": labels,
        "HostConfig": {
            "Binds": binds,
            "RestartPolicy": restart,
            "SecurityOpt": ["no-new-privileges:true"],
        },
        "NetworkingConfig": networking,
    }


# ── Status ─────────────────────────────────────────────────────────────────────

_progress: dict = {}  # (species, kind) -> {"state": ..., "message": ...}
_lock = threading.Lock()


def _containers() -> list:
    status, data = _docker("GET", "/containers/json?all=1")
    if status != 200:
        raise InstanceError(_error(data))
    return data


def _name_of(c: dict) -> str:
    return (c.get("Names") or ["/?"])[0].lstrip("/")


def _find(species: str, kind: str, containers: list) -> Optional[dict]:
    """A species' instance, whether the console created it (named neo4j-hsa) or
    docker compose / up.sh did (service neo4j-hsa, named biocypher-console-neo4j-hsa-1).
    Either way it answers to the neo4j-hsa alias on the shared network."""
    wanted = container_name(species, kind)
    for c in containers:
        if wanted in [n.lstrip("/") for n in c.get("Names") or []]:
            return c
    for c in containers:
        if (c.get("Labels") or {}).get("com.docker.compose.service") == wanted:
            return c
    return None


def _published(c: dict, private_port: int) -> Optional[int]:
    """The host port a container publishes for one of its ports, if any."""
    for p in c.get("Ports") or []:
        if p.get("PrivatePort") == private_port and p.get("PublicPort"):
            return p["PublicPort"]
    return None


def status() -> dict:
    """Each species' Neo4j and MORK: whether it exists/runs, whether the console is
    connected to it, where to open it, and any setup in progress."""
    available = docker_available()
    containers = _containers() if available else []
    out = []
    for sp in et.species_list():
        row = {"species": sp}
        for kind in KINDS:
            c = _find(sp, kind, containers)
            http_private = 7474 if kind == "neo4j" else 8027
            http_port = (_published(c, http_private) if c else None) or (
                host_ports(sp, kind)["http"] if kind == "neo4j" else None)
            target = et.neo4j_uri_for(sp) if kind == "neo4j" else et.mork_url_for(sp)
            with _lock:
                prog = dict(_progress[(sp, kind)]) if (sp, kind) in _progress else None
            row[kind] = {
                "name": _name_of(c) if c else container_name(sp, kind),
                "exists": c is not None,
                "running": bool(c) and c.get("State") == "running",
                "state": c.get("State") if c else None,
                "managed": bool(c) and (c.get("Labels") or {}).get("kg-console.managed") == "true",
                "url": internal_url(sp, kind),
                # Is this species' connection setting pointed at this instance?
                "connected": target == internal_url(sp, kind),
                # Where you open it from this machine (Neo4j Browser / MORK API).
                "open_url": f"http://localhost:{http_port}" if http_port else None,
                "progress": prog,
            }
        out.append(row)
    return {"docker": available, "species": out}


def local_addresses() -> dict:
    """Where this computer reaches each console-run instance: {(species, kind):
    address}, from the ports it publishes (or, for Neo4j, the ports it would get).
    MORK isn't reachable from this computer unless an older setup published it."""
    try:
        containers = _containers() if docker_available() else []
    except InstanceError:
        containers = []
    out = {}
    for sp in et.species_list():
        for kind in KINDS:
            c = _find(sp, kind, containers)
            if kind == "neo4j":
                port = (_published(c, 7687) if c else None) or host_ports(sp, kind)["bolt"]
                out[(sp, kind)] = f"bolt://localhost:{port}"
            elif c and _published(c, 8027):
                out[(sp, kind)] = f"http://localhost:{_published(c, 8027)}"
    return out


# ── Actions ────────────────────────────────────────────────────────────────────

def _set_progress(species: str, kind: str, state: str, message: str = "") -> None:
    with _lock:
        _progress[(species, kind)] = {"state": state, "message": message}


def _ensure_image(image: str, species: str, kind: str) -> None:
    status, _ = _docker("GET", f"/images/{quote(image, safe='')}/json")
    if status == 200:
        return
    _set_progress(species, kind, "pulling", f"downloading {LABELS[kind]}… (first time only)")
    repo, _, tag = image.rpartition(":") if ":" in image.split("/")[-1] else (image, "", "latest")
    status, data = _docker("POST", f"/images/create?fromImage={quote(repo)}&tag={quote(tag)}",
                           timeout=1800)
    if status != 200 or (isinstance(data, str) and '"error"' in data):
        raise _fail(f"Couldn't download {LABELS[kind]}. Check the internet connection and "
                    "try again.", f"pull {image}: {_error(data)}")


def _wire(species: str, kind: str, login: Optional[dict] = None) -> None:
    """Point the species at the instance the console runs, with its login."""
    from backend.core.console import runtime_config  # local: keep import graph simple

    if kind == "mork":
        runtime_config.save({"species": {species.upper(): {"mork_url": internal_url(species, kind)}}})
        return
    runtime_config.save({"species": {species.upper(): {
        "neo4j_uri": internal_url(species, kind),
        "neo4j_user": login["user"],
        "neo4j_password": login["password"],
        "neo4j_database": "neo4j",
    }}})


def _volume_exists(name: str) -> bool:
    status, _ = _docker("GET", f"/volumes/{quote(name, safe='')}")
    return status == 200


def _neo4j_setup_login(species: str, has_container: bool) -> dict:
    """The login the console uses for the Neo4j it runs for a species.

    Kept from an earlier setup if there was one: Remove keeps the data volume, and
    Neo4j keeps the password it was first created with. Data made with a password the
    console never had can only be used with "Use existing". Anything new gets a
    generated password."""
    from backend.core.console import connections

    login = connections.managed_login(species)
    if login:
        return login
    data_exists = has_container or _volume_exists(
        f"{_self_context()['project']}_neo4j_data_{species}"
    )
    if data_exists:
        raise InstanceError(
            f"{species} already has Neo4j data from an earlier setup, made with a password "
            "the console doesn't have. Use “Use existing” and enter that password."
        )
    return {"user": "neo4j", "password": connections.new_password()}


def _check_login(species: str, name: str, login: dict) -> None:
    """Started and answering isn't enough: make sure the login works too. Bolt can
    lag the HTTP port by a moment, so retry briefly while it's unreachable."""
    from backend.core.console import connections

    _set_progress(species, "neo4j", "starting", "signing in…")
    deadline = time.monotonic() + 60
    while True:
        ok, message, reason = connections.probe_neo4j(
            internal_url(species, "neo4j"), login["user"], login["password"], "neo4j",
        )
        if ok:
            return
        if reason == "auth":
            raise InstanceError(
                "It's running, but its data was made with a password the console doesn't "
                "have. Use “Use existing” and enter that password."
            )
        if time.monotonic() > deadline:
            raise _fail("It's running, but the console can't sign in to it. Try Start again.",
                        f"{name}: {message}")
        time.sleep(2)


def _shift_ports(spec: dict, by: int) -> None:
    for bindings in spec["HostConfig"].get("PortBindings", {}).values():
        for b in bindings:
            b["HostPort"] = str(int(b["HostPort"]) + by)


def _create_and_start(species: str, kind: str, spec: dict) -> str:
    """Create and start a new instance. If a host port is already taken (another
    Neo4j, say), retry on the next block of ports; the console itself doesn't need
    them, they're only for opening the instance from this machine."""
    name = container_name(species, kind)
    for _attempt in range(5):
        _set_progress(species, kind, "creating", "creating…")
        status, data = _docker("POST", f"/containers/create?name={quote(name)}", spec)
        if status not in (200, 201):
            raise _fail("Couldn't set it up. Try again.", f"create {name}: {_error(data)}")
        _set_progress(species, kind, "starting", "starting…")
        status, data = _docker("POST", f"/containers/{quote(name)}/start")
        if status in (204, 304):
            return name
        msg = _error(data)
        _docker("DELETE", f"/containers/{quote(name)}?force=1")  # leave nothing half-made
        if "port is already allocated" not in msg and "address already in use" not in msg:
            raise _fail("Couldn't start it. Try again.", f"start {name}: {msg}")
        _shift_ports(spec, 10)
    raise _fail("Couldn't start it: other programs on this computer are using the "
                "connections it needs.", f"no free host ports for {name}")


READY_TIMEOUT = float(os.environ.get("INSTANCE_READY_TIMEOUT", "300"))


def _demux(raw: bytes) -> str:
    """Container logs without a TTY come as frames: 8-byte header (stream, 0, 0, 0,
    big-endian size) then the text. Strip the headers; plain text passes through."""
    out, i = [], 0
    while i + 8 <= len(raw) and raw[i] in (0, 1, 2) and raw[i + 1:i + 4] == b"\0\0\0":
        size = int.from_bytes(raw[i + 4:i + 8], "big")
        out.append(raw[i + 8:i + 8 + size])
        i += 8 + size
    return (b"".join(out) if i else raw).decode(errors="replace")


def _last_log(name: str, lines: int = 3) -> str:
    """The container's last few log lines, so a failed start says why in the UI."""
    try:
        status, raw = _docker(
            "GET", f"/containers/{quote(name)}/logs?stdout=1&stderr=1&tail=20", raw=True,
        )
    except InstanceError:
        return ""
    if status != 200 or not raw:
        return ""
    tail = [ln.strip()[:240] for ln in _demux(raw).splitlines() if ln.strip()][-lines:]
    return (" Last log lines: " + " | ".join(tail)) if tail else ""


def _answers(host: str, port: int) -> bool:
    """True once something answers HTTP on host:port (any status code counts)."""
    conn = http.client.HTTPConnection(host, port, timeout=3)
    try:
        conn.request("GET", "/")
        conn.getresponse().read()
        return True
    except (OSError, http.client.HTTPException):
        return False
    finally:
        conn.close()


def _probe_addrs(species: str, kind: str, container: Optional[dict]) -> list:
    """Where the console can reach the instance's HTTP port: its network alias (the
    console runs in Docker on the same network), else the published host port."""
    private = 7474 if kind == "neo4j" else 8027
    addrs = [(container_name(species, kind), private)]
    published = _published(container, private) if container else None
    if published:
        addrs.append(("127.0.0.1", published))
    return addrs


def _wait_ready(species: str, kind: str, name: str) -> None:
    """A started container isn't a working database yet: Neo4j takes a while to boot
    (longer on first start, while it installs APOC). Wait until it answers, so the UI
    only says "connected" (and offers the browser link) once it is."""
    _set_progress(species, kind, "starting", "starting… (first time: a few minutes)")
    deadline = time.monotonic() + READY_TIMEOUT
    while True:
        c = _find(species, kind, _containers())
        if not c or c.get("State") != "running":
            state = (c or {}).get("State") or "removed"
            raise _fail("It stopped while starting. Try Start again.",
                        f"{name} {state}.{_last_log(name)}")
        if any(_answers(h, p) for h, p in _probe_addrs(species, kind, c)):
            return
        if time.monotonic() > deadline:
            raise _fail("It's taking too long to start. Try Start again.",
                        f"{name} hasn't answered after {int(READY_TIMEOUT // 60)} minutes."
                        f"{_last_log(name)}")
        time.sleep(2)


# ── Keeping them in step with Settings ─────────────────────────────────────────

def _inspect(name: str) -> dict:
    status, data = _docker("GET", f"/containers/{quote(name)}/json")
    return data if status == 200 and isinstance(data, dict) else {}


def _shared_folders(container: dict) -> list:
    """The folders a container was made with, as it sees them (each bind's target).
    Not the source: Docker Desktop reports that as /host_mnt/Users/..., and the
    target is the path builds are read at anyway (same path as in the console)."""
    out = []
    for b in (container.get("HostConfig") or {}).get("Binds") or []:
        parts = b.split(":")
        if not b.startswith("/") or len(parts) < 2:
            continue  # a named volume (the database's own data)
        out.append(parts[-2] if len(parts) >= 3 else parts[-1])
    return sorted(out)


def out_of_date(container: dict) -> bool:
    """Was this database made with other folders or memory than Settings has now?
    ``container`` is its inspect data. Only shared folders and memory are compared."""
    if _shared_folders(container) != build_folders():
        return True
    env = (container.get("Config") or {}).get("Env") or []
    if _memory() and any(e.startswith("NEO4J_AUTH=") for e in env):  # a Neo4j
        return f"NEO4J_server_memory_heap_max__size={_memory()}" not in env
    return False


def _managed(c: Optional[dict]) -> bool:
    return bool(c) and (c.get("Labels") or {}).get("kg-console.managed") == "true"


def _remove_container(name: str) -> None:
    """Stop (letting MORK save its data) and remove a container; volumes are kept."""
    _docker("POST", f"/containers/{quote(name)}/stop?t=60", timeout=90)
    status, data = _docker("DELETE", f"/containers/{quote(name)}?force=1", timeout=90)
    if status not in (204, 404):
        raise _fail("Couldn't update it. Try again.", f"remove {name}: {_error(data)}")


def refresh() -> list:
    """Re-create each running database the console runs that's out of date with
    Settings (folders, memory), keeping its data and login. A stopped one is
    updated when it's next started. Returns the (species, kind) refreshed."""
    if not docker_available():
        return []
    done = []
    for sp in et.species_list():
        for kind in KINDS:
            c = _find(sp, kind, _containers())
            if not _managed(c) or c.get("State") != "running":
                continue
            if kind == "neo4j" and not _memory():
                continue  # can't re-create it without its memory; it keeps running as is
            name = _name_of(c)
            if not out_of_date(_inspect(name)):
                continue
            with _lock:
                if (_progress.get((sp, kind)) or {}).get("state") in ("pulling", "creating", "starting"):
                    continue
            _set_progress(sp, kind, "starting", "updating to settings…")
            _setup_worker(sp, kind)  # re-creates it: it's out of date
            done.append((sp, kind))
    return done


_refreshing = threading.Lock()


def refresh_in_background() -> None:
    """refresh(), off the request thread (a Neo4j takes a while to come back)."""
    def run():
        if not _refreshing.acquire(blocking=False):
            return
        try:
            refresh()
        except Exception:  # noqa: BLE001 - never let this take the console down
            logger.exception("Updating the databases to Settings failed")
        finally:
            _refreshing.release()

    threading.Thread(target=run, daemon=True).start()


def cannot_read(species: Optional[str], kind: str, path: str) -> Optional[str]:
    """Why the database the console runs for this species can't read a build at
    ``path`` yet (it was made before the folders changed), or None when it can, or
    when it isn't one the console runs. Starts updating it."""
    if not species or not docker_available():
        return None
    try:
        c = _find(species, kind, _containers())
    except InstanceError:
        return None
    if not _managed(c):
        return None
    roots = _shared_folders(_inspect(_name_of(c)))
    p = os.path.normpath(path)
    if any(p == r or p.startswith(r.rstrip("/") + "/") for r in roots):
        return None
    refresh_in_background()
    return (f"{species} {LABELS[kind]} is updating to read the Builds folder · "
            "load again once it's running")


def _setup_worker(species: str, kind: str) -> None:
    try:
        existing = _find(species, kind, _containers())
        if _managed(existing) and out_of_date(_inspect(_name_of(existing))):
            # Made with other folders or memory: make it again (data and login kept).
            _set_progress(species, kind, "starting", "updating to settings…")
            _remove_container(_name_of(existing))
            existing = None
        login = None
        if kind == "neo4j":
            from backend.core.console import connections

            login = _neo4j_setup_login(species, has_container=bool(existing))
            # Keep it before anything is created: the data volume will need it from now on.
            connections.save_managed_login(species, login)
        if existing:
            # Already there (made by the console, compose or up.sh): start and connect it.
            name = _name_of(existing)
            _set_progress(species, kind, "starting", "starting…")
            status, data = _docker("POST", f"/containers/{quote(name)}/start")
            if status not in (204, 304):  # 304 = already running
                raise _fail("Couldn't start it. Try again.", f"start {name}: {_error(data)}")
        else:
            ctx = _self_context()
            if kind == "neo4j":
                image = NEO4J_IMAGE
            else:
                _, images = _docker("GET", "/images/json")
                image = _mork_image(species, images or [])
                if not image:
                    raise _fail("MORK can't run on this computer yet. Use an existing MORK "
                                "instead.", "no MORK image here (build one or set MORK_IMAGE)")
            spec = container_spec(species, kind, ctx, image, login)  # validates the password
            _ensure_image(image, species, kind)
            name = _create_and_start(species, kind, spec)
        _wait_ready(species, kind, name)
        if kind == "neo4j":
            _check_login(species, name, login)
        _wire(species, kind, login)
        _set_progress(species, kind, "done", "running · in use")
    except InstanceError as exc:
        _set_progress(species, kind, "error", str(exc))
    except Exception:  # noqa: BLE001 - report anything unexpected to the UI
        logger.exception("Instance setup failed for %s %s", kind, species)
        _set_progress(species, kind, "error", "setup failed · try again")


def setup(species: str, kind: str) -> None:
    """Create (if needed), start and connect a species' Neo4j or MORK, in the background."""
    if kind not in KINDS:
        raise InstanceError(f"Unknown instance kind '{kind}'.")
    _species_index(species)  # validates the species
    if kind == "neo4j" and not _memory():
        raise InstanceError(NO_MEMORY)
    if not docker_available():
        raise InstanceError(NO_DOCKER)
    with _lock:
        if (_progress.get((species, kind)) or {}).get("state") in ("pulling", "creating", "starting"):
            raise InstanceError(f"{species}'s {LABELS[kind]} is already being set up.")
    _set_progress(species, kind, "starting", "preparing…")
    threading.Thread(target=_setup_worker, args=(species, kind), daemon=True).start()


def remove(species: str, kind: str) -> None:
    """Remove a species' instance container but keep its data volume (and its
    login), so a later Set up brings the same data back. If the species was using
    this instance, its connection is cleared too."""
    if kind not in KINDS:
        raise InstanceError(f"Unknown instance kind '{kind}'.")
    found = _find(species, kind, _containers())
    if not found:
        raise InstanceError(f"{species} has no {LABELS[kind]} here to remove.")
    name = _name_of(found)
    # force=1 stops it first; no v=1, so named volumes (the data) are kept.
    status, data = _docker("DELETE", f"/containers/{quote(name)}?force=1", timeout=90)
    if status != 204:
        raise _fail("Couldn't remove it. Try again.", f"remove {name}: {_error(data)}")
    with _lock:
        _progress.pop((species, kind), None)
    from backend.core.console import runtime_config  # local: keep import graph simple

    # Only if the species was using this instance: one using your own database keeps it.
    target = et.neo4j_uri_for(species) if kind == "neo4j" else et.mork_url_for(species)
    if target == internal_url(species, kind):
        fields = ("neo4j_uri", "neo4j_user", "neo4j_password", "neo4j_database") \
            if kind == "neo4j" else ("mork_url",)
        runtime_config.save({"species": {species.upper(): {f: None for f in fields}}})


def stop(species: str, kind: str) -> None:
    if kind not in KINDS:
        raise InstanceError(f"Unknown instance kind '{kind}'.")
    found = _find(species, kind, _containers())
    if not found:
        raise InstanceError(f"{species} has no {LABELS[kind]} here to stop.")
    name = _name_of(found)
    status, data = _docker("POST", f"/containers/{quote(name)}/stop", timeout=90)
    if status not in (204, 304):
        raise _fail("Couldn't stop it. Try again.", f"stop {name}: {_error(data)}")
    with _lock:
        _progress.pop((species, kind), None)
