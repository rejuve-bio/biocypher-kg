from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache

# Repo root = the biocypher-kg checkout that contains create_knowledge_graph.py.
# config.py lives at <repo>/kg-service/backend/core/config.py, so parents[3] is <repo>.
# The build CLI must run with this as its cwd (it hardcodes repo-root-relative paths).
_REPO_ROOT_DEFAULT = str(Path(__file__).resolve().parents[3])

class Settings(BaseSettings):
    # Neo4j
    NEO4J_URI: str = "bolt://localhost:27688"
    NEO4J_USER: str = "YOUR_USERNAME_HERE"
    NEO4J_PASSWORD: str = "YOUR_PASSWORD_HERE"
    NEO4J_DATABASE: str = "YOUR_DATABASE_NAME_HERE"
    ARCHIVE_BASE: str = "/mnt/hdd_1/biocypher-kg/output/human/biocypher-archives/"
    VERSION_DIFF_SCRIPT: str = "/home/abdum/services/biocypher-kg/version_diff.py"
    MORK_SUMMARY_SCRIPT: str = "/home/abdum/services/biocypher-kg/get_mork_summary.py"
    MORK_URL: str = "http://localhost:8432"
    MORK_LIVE_STATS_ENABLED: bool = False

    # Cache
    CACHE_TTL: int = 300

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = True
    APP_NAME: str = "BioCypher KG Observatory"
    APP_VERSION: str = "0.1.0"

    # ===== Console (build management) =====
    # Absolute path to the biocypher-kg checkout. Builds shell out to
    # `create_knowledge_graph.py` with this as cwd. Override via REPO_ROOT env var.
    REPO_ROOT: str = _REPO_ROOT_DEFAULT
    # Where build job artifacts (registry.json + per-job logs/output) are stored.
    # Empty string => <REPO_ROOT>/kg-service/.builds (resolved lazily in builds_dir).
    BUILDS_DIR: str = ""
    # Base dir for auto-named build outputs when the wizard's output field is blank.
    # When set, a blank output resolves to <DATA_ROOT>/<species>-<dataset>-<timestamp>.
    # In Docker this is mounted at the same path, so those outputs are host-visible.
    DATA_ROOT: str = ""
    # Max builds allowed to run concurrently. Default 1: full builds are memory/IO
    # heavy and two at once can OOM a single host. This is the Phase 3 parallel seam.
    MAX_CONCURRENT_BUILDS: int = 1
    # Retention: keep at most this many finished (terminal) build jobs; older ones
    # are pruned (registry entry + artifacts dir) when a new build is launched.
    MAX_BUILD_HISTORY: int = 50
    # Executable used to launch the build (`uv run python create_knowledge_graph.py`).
    UV_BIN: str = "uv"
    # When true, mount the built frontend (frontend/dist) as static files at "/".
    SERVE_FRONTEND: bool = True

    @property
    def repo_root_path(self) -> Path:
        return Path(self.REPO_ROOT).resolve()

    @property
    def builds_dir(self) -> Path:
        if self.BUILDS_DIR:
            return Path(self.BUILDS_DIR).resolve()
        return self.repo_root_path / "kg-service" / ".builds"

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()
