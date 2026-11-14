from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache

# Repo root (contains create_knowledge_graph.py); config.py is 3 levels under it.
_REPO_ROOT_DEFAULT = str(Path(__file__).resolve().parents[3])

class Settings(BaseSettings):
    # Neo4j
    # The observatory's one Neo4j/MORK (kg-service run on its own, set in kg-service/.env).
    # No defaults: unset means not configured, so nothing connects to a guessed address.
    # The console never uses these; each species' databases are set in Settings.
    NEO4J_URI: str = ""
    NEO4J_USER: str = ""
    NEO4J_PASSWORD: str = ""
    NEO4J_DATABASE: str = ""
    ARCHIVE_BASE: str = ""
    VERSION_DIFF_SCRIPT: str = "/home/abdum/services/biocypher-kg/version_diff.py"
    MORK_SUMMARY_SCRIPT: str = "/home/abdum/services/biocypher-kg/get_mork_summary.py"
    MORK_URL: str = ""
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
    # Absolute path to the checkout; override via REPO_ROOT env var.
    REPO_ROOT: str = _REPO_ROOT_DEFAULT
    # Build artifacts dir; empty => <REPO_ROOT>/kg-service/.builds.
    BUILDS_DIR: str = ""
    # Required, set in Settings; no defaults (see runtime_config.require):
    # the console data folder (downloads, prepared dbSNP cache, load archives),
    DATA_ROOT: str = ""
    # where builds are saved (Settings > Builds folder),
    OUTPUT_ROOT: str = ""
    # and memory for each Neo4j the console runs (page cache and heap each).
    NEO4J_MEMORY: str = ""
    # Max concurrent builds; default 1 (two full builds can OOM a single host).
    MAX_CONCURRENT_BUILDS: int = 1
    # Keep at most this many finished build jobs in History; older ones are pruned.
    MAX_BUILD_HISTORY: int = 20
    # Keep at most this many build OUTPUT folders per (writer, species) under DATA_ROOT.
    # Builds are large, so keep this small; raise it per deployment via the env file.
    MAX_OUTPUT_BUILDS: int = 3
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
