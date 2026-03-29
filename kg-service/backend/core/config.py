from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # Neo4j
    NEO4J_URI: str = "bolt://localhost:27688"
    NEO4J_USER: str = "YOUR_USERNAME_HERE"
    NEO4J_PASSWORD: str = "YOUR_PASSWORD_HERE"
    NEO4J_DATABASE: str = "YOUR_DATABASE_NAME_HERE"

    # Cache
    CACHE_TTL: int = 300

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = True
    APP_NAME: str = "BioCypher KG Observatory"
    APP_VERSION: str = "0.1.0"

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()
