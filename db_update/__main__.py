import asyncio

from db_update import mlb
from db_update.db_pool import db_pool
from db_update.logger import setup_logging


async def main():
    setup_logging()
    pool = await db_pool()

    await mlb.run(pool)


if __name__ == "__main__":
    asyncio.run(main())
