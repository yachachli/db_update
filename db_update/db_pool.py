import typing as t
from os import environ

import asyncpg

if t.TYPE_CHECKING:
    DBPool = asyncpg.Pool[asyncpg.Record]
    DBConnection = (
        asyncpg.Connection[asyncpg.Record]
        | asyncpg.pool.PoolConnectionProxy[asyncpg.Record]
    )
else:
    DBPool = asyncpg.Pool
    DBConnection = asyncpg.Connection | asyncpg.pool.PoolConnectionProxy


async def db_pool():
    return await asyncpg.create_pool(
        database=environ["DB_NAME"],
        user=environ["DB_USER"],
        password=environ["DB_PASS"],
        host=environ["DB_HOST"],
    )
