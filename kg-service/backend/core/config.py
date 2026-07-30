from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache

# Repo root (contains create_knowledge_graph.py); config.py is 3 levels under it.
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
    # Absolute path to the checkout; override via REPO_ROOT env var.
    REPO_ROOT: str = _REPO_ROOT_DEFAULT
    # Build artifacts dir; empty => <REPO_ROOT>/kg-service/.builds.
    BUILDS_DIR: str = ""
    # Base dir for auto-named build outputs when the output field is blank.
    DATA_ROOT: str = ""
    # Max concurrent builds; default 1 (two full builds can OOM a single host).
    MAX_CONCURRENT_BUILDS: int = 1
    # Keep at most this many finished build jobs; older ones are pruned.
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
