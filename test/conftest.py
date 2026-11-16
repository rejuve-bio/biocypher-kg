import os
import sys
import tempfile
from pathlib import Path


# Ensure tests can import the local package when the project is run
# directly from the repository without an editable install.
ROOT = Path(__file__).resolve().parent.parent
ROOT_STR = str(ROOT)
EXISTING_PATHS = {
    str((Path.cwd() if entry in ("", ".") else Path(entry)).resolve())
    for entry in sys.path
}
if ROOT_STR not in EXISTING_PATHS:
    sys.path.insert(0, ROOT_STR)

# Make the kg-service FastAPI app importable (`backend.*`) for Console tests.
KG_SERVICE = ROOT / "kg-service"
if KG_SERVICE.is_dir() and str(KG_SERVICE) not in EXISTING_PATHS:
    sys.path.insert(0, str(KG_SERVICE))

# Isolate Console build-job artifacts to a temp dir so tests never write into the
# repo's kg-service/.builds. Must be set before backend.core.config is imported.
os.environ.setdefault("BUILDS_DIR", tempfile.mkdtemp(prefix="kg_console_builds_"))
# The Console reads REPO_ROOT for config introspection / shelling out to the CLI.
os.environ.setdefault("REPO_ROOT", ROOT_STR)
# FastAPI's TestClient calls itself "testserver"; the console otherwise only answers
# to localhost (see kg-service/backend/api/security.py).
os.environ.setdefault("CONSOLE_ALLOWED_HOSTS", "testserver")
# A console that has been set up: its required settings (no defaults, see
# runtime_config.require) chosen as Settings would. Tests of the not-set-up case
# clear them with monkeypatch.
os.environ.setdefault("DATA_ROOT", tempfile.mkdtemp(prefix="kg_console_data_"))
os.environ.setdefault("OUTPUT_ROOT", tempfile.mkdtemp(prefix="kg_console_out_"))
os.environ.setdefault("NEO4J_MEMORY", "1G")
# Never reach this computer's real Docker: the console can stop and re-create the
# databases it runs (instances.py), so tests fake Docker where they need it.
os.environ.setdefault("DOCKER_SOCK", str(Path(tempfile.gettempdir()) / "kg-console-tests-no-docker.sock"))


def pytest_addoption(parser):
    parser.addoption(
        "--adapters-config",
        action="store",
        default="config/hsa/hsa_adapters_config_sample.yaml",
        help="Path to the adapters config YAML file"
    )
    parser.addoption(
        "--primer-schema-config",
        action="store",
        default="config/primer_schema_config.yaml",
        help="Path to the primer (base) schema config YAML file"
    )
    parser.addoption(
        "--species-schema-config",
        action="store",
        default="config/hsa/hsa_schema_config.yaml",
        help="Path to the species-specific schema config YAML file"
    )
    parser.addoption(
        "--dbsnp-cache-root",
        action="store",
        default="aux_files/hsa/sample_dbsnp",
        help="Path to the dbSNP cache root matching the adapters config under test."
    )
    parser.addoption(
        "--adapter-test-mode",
        action="store",
        choices=["smoke", "full"],
        default="full",
        help="Adapter test depth: 'smoke' skips heavy adapters and limits samples; 'full' runs everything."
    )
    parser.addoption(
        "--adapter-max-adapters",
        action="store",
        type=int,
        default=25,
        help="Maximum number of adapters to sample per node/edge test in smoke mode."
    )
    parser.addoption(
        "--adapter-profile",
        action="store_const",
        const=True,
        dest="adapter_profile",
        default=True,
        help="Enable per-adapter runtime profiling output."
    )
    parser.addoption(
        "--no-adapter-profile",
        action="store_const",
        const=False,
        dest="adapter_profile",
        help="Disable per-adapter runtime profiling output."
    )
