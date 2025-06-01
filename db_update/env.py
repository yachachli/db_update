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

    RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"]
    RAPIDAPI_HOST = os.environ["RAPIDAPI_HOST"]
