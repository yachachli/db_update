try:
    from dotenv import load_dotenv

    _ = load_dotenv()
except ImportError:
    pass

import os


class Env:
    DB_NAME = os.environ["DB_NAME"]
    DB_USER = os.environ["DB_USER"]
    DB_PASS = os.environ["DB_PASS"]
    DB_HOST = os.environ["DB_HOST"]

    MLB_API_KEY = os.environ["MLB_API_KEY"]
    MLB_API_HOST = os.environ["MLB_API_HOST"]

    WNBA_API_KEY = os.environ["WNBA_API_KEY"]
    WNBA_API_HOST = os.environ["WNBA_API_HOST"]

    API_CACHE_DIR = os.environ.get("API_CACHE_DIR", None)
