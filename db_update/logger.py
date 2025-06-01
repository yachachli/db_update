import json
import logging
import logging.config
import os
import pathlib
from datetime import datetime


def setup_logging():
    config_path = pathlib.Path("./db_update/logging-config.json")
    with open(config_path) as f:
        config = json.load(f)

    config["handlers"]["file"]["filename"] = (
        f"./logs/app-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )

    os.makedirs("./logs", exist_ok=True)

    logging.config.dictConfig(config)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


logger = logging.getLogger("app")
