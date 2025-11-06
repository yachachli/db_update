import asyncio
import sys

from db_update.db_pool import db_pool
from db_update.logger import setup_logging
from db_update.runners import mlb, wnba, nba


async def main():
    setup_logging()

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <sport>")
        return

    pool = await db_pool()

    match sys.argv[1]:
        case "mlb":
            await mlb.run(pool)
        case "nba":
            await nba.run(pool)
        case "wnba":
            await wnba.run(pool)
        case sport:
            print(f"Unknown sport: {sport}. Usage: {sys.argv[0]} <sport>")
            return


if __name__ == "__main__":
    asyncio.run(main())
