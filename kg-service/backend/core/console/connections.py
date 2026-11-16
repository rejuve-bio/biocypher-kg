"""Each species' Neo4j and MORK connection: where it is, how the console logs in,
and whether it works right now.

A species gets each database one of two ways (Settings > Species databases):

- Run here: the console runs it in Docker and owns its login. A new instance gets a
  generated password. That login is kept under ``managed`` even after Remove,
  because the data volume Remove keeps still requires it.
- Use existing: a database you already run. You give its address and, for Neo4j,
  its own username, password and database. They're checked before they're saved.
  Addresses are saved as typed (a missing scheme is added); "localhost" works
  because connections go through et.connect_address.

MORK has no login: its server takes no credentials, so an address is all it needs.

Store layout (runtime_config):
  species.<SP>: neo4j_uri, neo4j_user, neo4j_password, neo4j_database, mork_url
  managed.<SP>: neo4j_user, neo4j_password
"""
from __future__ import annotations

import http.client
import secrets
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple
from urllib.parse import urlparse

from backend.core.console import explore_targets as et
from backend.core.console import runtime_config

KINDS = ("neo4j", "mork")
# The login and database of a Neo4j the console runs: what it creates them with (Neo4j
# Community's built-in user and only database), not a guess for anyone else's.
CONSOLE_USER = "neo4j"
CONSOLE_DATABASE = "neo4j"
_NEO4J_FIELDS = ("neo4j_uri", "neo4j_user", "neo4j_password", "neo4j_database")


class ConnectError(ValueError):
    """A connection that can't be made or saved; the message is shown to the user."""


# ── Logins ─────────────────────────────────────────────────────────────────────

def _check(species: str, kind: str) -> None:
    if kind not in KINDS:
        raise ConnectError(f"Unknown database '{kind}' (neo4j or mork).")
    if species not in et.species_list():
        raise ConnectError(f"Unknown species '{species}'.")


def neo4j_login(species: str) -> Optional[dict]:
    """How the console reaches a species' Neo4j and logs in: uri, user, password,
    database, exactly as saved in Settings (no shared or guessed login). None when the
    species has no Neo4j."""
    uri = et.neo4j_uri_for(species)
    if not uri:
        return None
    saved = runtime_config.species_override(species)
    console = source(species, "neo4j", uri) == "console"
    return {
        "uri": uri,
        "user": saved.get("neo4j_user") or (CONSOLE_USER if console else ""),
        "password": saved.get("neo4j_password") or "",
        "database": saved.get("neo4j_database") or (CONSOLE_DATABASE if console else ""),
    }


def managed_login(species: str) -> Optional[dict]:
    """The login of the Neo4j the console runs for this species, if it ever made one."""
    m = (runtime_config.load().get("managed") or {}).get(species.upper()) or {}
    if not m.get("neo4j_password"):
        return None
    return {"user": m.get("neo4j_user") or CONSOLE_USER, "password": m["neo4j_password"]}


def save_managed_login(species: str, login: dict) -> None:
    runtime_config.save({"managed": {species.upper(): {
        "neo4j_user": login["user"], "neo4j_password": login["password"],
    }}})


def new_password() -> str:
    return secrets.token_urlsafe(18)


def normalize_address(kind: str, address: Optional[str]) -> str:
    """What people type ("localhost:7687", "myserver:8027") as a full address: Neo4j
    defaults to bolt://, MORK to http://."""
    address = (address or "").strip().rstrip("/")
    if address and "://" not in address:
        address = ("bolt://" if kind == "neo4j" else "http://") + address
    return address


def source(species: str, kind: str, address: Optional[str]) -> Optional[str]:
    """"console" when the address is the instance the console runs, "existing" for
    any other database, None for no address."""
    if not address:
        return None
    from backend.core.console import instances  # local: instances imports this module

    return "console" if address == instances.internal_url(species, kind) else "existing"


# ── Checks ─────────────────────────────────────────────────────────────────────

def probe_neo4j(uri: str, user: str, password: str, database: str,
                timeout: float = 5.0) -> Tuple[bool, Optional[str], Optional[str]]:
    """Log in and run a query. Returns (ok, message, reason); reason is one of
    "auth", "unreachable", "database", "address", "other". Messages name the
    address as given, not the one actually dialled (see et.connect_address)."""
    from neo4j import GraphDatabase
    from neo4j.exceptions import AuthError, ClientError, ServiceUnavailable

    driver = None
    try:
        driver = GraphDatabase.driver(et.connect_address(uri), auth=(user, password),
                                      connection_timeout=timeout,
                                      max_transaction_retry_time=timeout)
        with driver.session(database=database) as session:
            session.run("RETURN 1").consume()
        return True, None, None
    except AuthError:
        return False, "Wrong username or password.", "auth"
    except ServiceUnavailable:
        return (False, f"Can't reach Neo4j at {uri}. Check that it's running and the "
                "address is right.", "unreachable")
    except ClientError as exc:
        if "DatabaseNotFound" in (getattr(exc, "code", "") or ""):
            return False, f"There's no database named '{database}' there.", "database"
        return False, str(exc.message or exc), "other"
    except ValueError:  # bad scheme or malformed address
        return (False, "That doesn't look like a Neo4j address (for example "
                "localhost:7687).", "address")
    except Exception as exc:  # noqa: BLE001 - report anything else as-is
        return False, str(exc), "other"
    finally:
        if driver is not None:
            try:
                driver.close()
            except Exception:  # noqa: BLE001
                pass


def _http_answers(url: str, timeout: float) -> bool:
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    cls = http.client.HTTPSConnection if p.scheme == "https" else http.client.HTTPConnection
    conn = cls(p.hostname, p.port or (443 if p.scheme == "https" else 80), timeout=timeout)
    try:
        conn.request("GET", "/")
        conn.getresponse().read()
        return True
    except (OSError, http.client.HTTPException):
        return False
    finally:
        conn.close()


def probe_mork(url: str, timeout: float = 5.0) -> Tuple[bool, Optional[str], Optional[str]]:
    """Reach the server quickly, then ask MORK itself (so another web server at that
    address doesn't pass). Same (ok, message, reason) shape as probe_neo4j."""
    if urlparse(url).scheme not in ("http", "https") or not urlparse(url).hostname:
        return (False, "That doesn't look like a MORK address (for example "
                "localhost:8027).", "address")
    target = et.connect_address(url)
    if not _http_answers(target, timeout):
        return (False, f"Can't reach MORK at {url}. Check that it's running and the "
                "address is right.", "unreachable")
    try:
        from backend.core.mork_client import MORKClient

        MORKClient(mork_url=target).get_atom_count()
        return True, None, None
    except ImportError as exc:
        return False, str(exc), "other"
    except Exception:  # noqa: BLE001
        return False, f"Something answers at {url}, but it isn't MORK.", "other"


# ── What each species uses ─────────────────────────────────────────────────────

_NOT_RUNNING = "Not running. Start it under Species databases."
# Addresses that encrypt the connection (+ssc: encrypted, certificate not checked).
_ENCRYPTED = {"bolt+s", "bolt+ssc", "neo4j+s", "neo4j+ssc", "https"}
_THIS_COMPUTER = {"localhost", "127.0.0.1", "::1", "host.docker.internal"}


def unencrypted(address: Optional[str]) -> bool:
    """True for a connection to another computer that isn't encrypted: the password
    and data would cross the network readable by anyone on the way."""
    if not address:
        return False
    p = urlparse(address)
    host = (p.hostname or "").lower()
    if p.scheme in _ENCRYPTED or host in _THIS_COMPUTER or host.endswith(".localhost"):
        return False
    return True


def _shown(species: str, kind: str, address: str, src: Optional[str],
           local: Optional[dict]) -> str:
    """The address to show. A console-run instance's own address only works inside
    the console, so show where this computer reaches it (e.g. for Neo4j Browser)."""
    if src != "console":
        return address
    if local is None:
        from backend.core.console import instances  # local: instances imports this module

        local = instances.local_addresses()
    # None: only the console can reach it (MORK isn't opened to this computer).
    return local.get((species, kind))


def _neo4j_status(species: str, check: bool, local: Optional[dict] = None) -> dict:
    login = neo4j_login(species)
    if not login:
        return {"source": None, "address": None}
    src = source(species, "neo4j", login["uri"])
    shown = _shown(species, "neo4j", login["uri"], src, local)
    out = {
        "source": src,
        "address": shown,
        "unencrypted": src == "existing" and unencrypted(login["uri"]),
        "user": login["user"],
        "database": login["database"],
        "password_set": bool(login["password"]),
    }
    if check:
        ok, message, reason = probe_neo4j(login["uri"], login["user"], login["password"],
                                          login["database"])
        if message and src == "console":
            message = _NOT_RUNNING if reason == "unreachable" else message.replace(login["uri"], shown or "it")
        out.update(ok=ok, error=message, reason=reason)
    return out


def _mork_status(species: str, check: bool, local: Optional[dict] = None) -> dict:
    url = et.mork_url_for(species)
    if not url:
        return {"source": None, "address": None}
    src = source(species, "mork", url)
    shown = _shown(species, "mork", url, src, local)
    out = {"source": src, "address": shown,
           "unencrypted": src == "existing" and unencrypted(url)}
    if check:
        ok, message, reason = probe_mork(url)
        if message and src == "console":
            message = _NOT_RUNNING if reason == "unreachable" else message.replace(url, shown or "it")
        out.update(ok=ok, error=message, reason=reason)
    return out


def species_status(species: str, check: bool = True, local: Optional[dict] = None) -> dict:
    return {"species": species, "neo4j": _neo4j_status(species, check, local),
            "mork": _mork_status(species, check, local)}


def status(check: bool = True) -> list:
    """Every species: which Neo4j and MORK it uses, how it logs in, and (with check)
    whether that works right now. Checks run in parallel."""
    from backend.core.console import instances  # local: instances imports this module

    names = et.species_list()
    local = instances.local_addresses()  # one Docker call for every species
    with ThreadPoolExecutor(max_workers=max(1, min(8, len(names)))) as pool:
        return list(pool.map(lambda sp: species_status(sp, check, local), names))


# ── Changes ────────────────────────────────────────────────────────────────────

def connect_existing(species: str, kind: str, address: str, user: Optional[str] = None,
                     password: Optional[str] = None, database: Optional[str] = None) -> dict:
    """Point a species at a database you already run. Saved only once it answers
    and (for Neo4j) the login works."""
    _check(species, kind)
    address = normalize_address(kind, address)
    if not address:
        raise ConnectError("Enter the database's address.")
    key = species.upper()
    if kind == "mork":
        ok, message, _ = probe_mork(address)
        if not ok:
            raise ConnectError(message)
        runtime_config.save({"species": {key: {"mork_url": address}}})
        return species_status(species)

    user = (user or "").strip()
    database = (database or "").strip()
    if not user or not database:
        raise ConnectError("enter the username and database")
    if not password:
        # Blank keeps the saved password, but only for the database it was saved for.
        saved = runtime_config.species_override(species)
        if saved.get("neo4j_uri") == address and saved.get("neo4j_password"):
            password = saved["neo4j_password"]
        else:
            raise ConnectError("Enter this database's password.")
    ok, message, _ = probe_neo4j(address, user, password, database)
    if not ok:
        raise ConnectError(message)
    runtime_config.save({"species": {key: {
        "neo4j_uri": address, "neo4j_user": user,
        "neo4j_password": password, "neo4j_database": database,
    }}})
    return species_status(species)


def disconnect(species: str, kind: str) -> dict:
    """Stop using a species' database (the species leaves Explore until it has one)."""
    _check(species, kind)
    fields = _NEO4J_FIELDS if kind == "neo4j" else ("mork_url",)
    runtime_config.save({"species": {species.upper(): {f: None for f in fields}}})
    return species_status(species)


def console_login(species: str) -> dict:
    """The login of the Neo4j the console runs for a species, to sign in to Neo4j
    Browser. Only for console-run instances: your own databases' passwords are
    never sent back."""
    _check(species, "neo4j")
    login = neo4j_login(species)
    if not login or source(species, "neo4j", login["uri"]) != "console":
        raise ConnectError(f"{species} isn't using a Neo4j the console runs.")
    return {"user": login["user"], "password": login["password"]}
