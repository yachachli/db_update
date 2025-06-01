import json
import typing
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


def encode_jsonb(data: typing.Any):
    return b"\x01" + json.dumps(data).encode()


def decode_jsonb(data: bytes):
    return json.loads(data[1:].decode())


def encode_json(data: typing.Any):
    return json.dumps(data).encode()


def decode_json(data: bytes):
    return json.loads(data.decode())


async def init_connection(conn: DBConnection):
    await conn.set_type_codec(
        "jsonb",
        encoder=encode_jsonb,
        decoder=decode_jsonb,
        schema="pg_catalog",
        format="binary",
    )
    await conn.set_type_codec(
        "json",
        encoder=encode_json,
        decoder=decode_json,
        schema="pg_catalog",
        format="binary",
    )


async def db_pool():
    return await asyncpg.create_pool(
        database=environ["DB_NAME"],
        user=environ["DB_USER"],
        password=environ["DB_PASS"],
        host=environ["DB_HOST"],
        init=init_connection,
    )
