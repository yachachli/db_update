"""
Environment variables and app configuration.
Credentials (KenPom, Odds API) are loaded from .env — never commit .env.

Paths default to folder containing app/ (backend or bracketiq when copied to db_update).
"""

import os
from pathlib import Path

from pydantic_settings import BaseSettings

# Folder containing app/ (backend or db_update/bracketiq)
_BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Load from environment or .env file."""

    KENPOM_EMAIL: str = ""
    KENPOM_PASSWORD: str = ""
    ODDS_API_KEY: str = ""
    NEON_DATABASE_URL: str = ""

    # Override via env; default is under _BASE_DIR so it works when copied to db_update/bracketiq
    CACHE_DIR: str = ""
    HISTORICAL_DIR: str = ""
    CURRENT_SEASON: str = "2026"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


def get_settings() -> Settings:
    return Settings()


def get_cache_dir() -> Path:
    raw = settings.CACHE_DIR.strip()
    path = Path(raw) if raw else (_BASE_DIR / "app" / "data" / "cache")
    if not path.is_absolute():
        path = _BASE_DIR / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_historical_dir() -> Path:
    raw = settings.HISTORICAL_DIR.strip()
    path = Path(raw) if raw else (_BASE_DIR / "app" / "data" / "historical")
    if not path.is_absolute():
        path = _BASE_DIR / path
    path.mkdir(parents=True, exist_ok=True)
    return path


settings = get_settings()
