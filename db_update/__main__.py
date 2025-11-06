import asyncio
import sys

from db_update.db_pool import db_pool
from db_update.logger import setup_logging


async def main():
    setup_logging()

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <sport>")
        return

    pool = await db_pool()

    match sys.argv[1]:
        case "mlb":
            from db_update.runners import mlb
            await mlb.run(pool)
        case "nba":
            from db_update.runners import nba
            await nba.run(pool)
        case "wnba":
            from db_update.runners import wnba
            await wnba.run(pool)
        case sport:
            print(f"Unknown sport: {sport}. Usage: {sys.argv[0]} <sport>")
            return


if __name__ == "__main__":
    asyncio.run(main())
