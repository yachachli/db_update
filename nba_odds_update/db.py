"""Database connection — works with GitHub Actions secrets or local .env."""

import os
import pandas as pd
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_engine = None


def get_engine():
    global _engine
    if _engine is not None:
        return _engine

    # DB_URL = db_update repo convention; DATABASE_URL = nba_predictor / common
    url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        user = os.getenv("DB_USER")
        pw = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        name = os.getenv("DB_NAME")
        if not all([user, pw, host, name]):
            raise RuntimeError(
                "Set DB_URL, DATABASE_URL, or DB_USER/DB_PASS/DB_HOST/DB_NAME"
            )
        url = f"postgresql://{user}:{pw}@{host}/{name}?sslmode=require"

    _engine = create_engine(url, pool_pre_ping=True)
    return _engine


def query_df(sql, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)
