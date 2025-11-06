import asyncio
import decimal
import itertools
import typing as t

from db_update.db_pool import DBConnection, DBPool
from db_update.logger import logger


def int_safe(v: t.Any) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


def float_safe(v: t.Any) -> float:
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0


def decimal_safe(v: t.Any) -> decimal.Decimal:
    try:
        return decimal.Decimal(v)
    except (ValueError, TypeError, decimal.InvalidOperation):
        return decimal.Decimal(0)


def bool_maybe(v: t.Any) -> bool | None:
    if v == "True":
        return True
    elif v == "False":
        return False
    else:
        return None


def int_maybe(v: t.Any) -> int | None:
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def float_maybe(v: t.Any) -> float | None:
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


async def batch[R](tasks: t.Iterable[t.Awaitable[R]], batch_size: int = 50) -> list[R]:
    results: list[R] = []
    tasks = list(tasks)
    logger.info(f"batching {len(tasks)} tasks")
    total = 0
    for batch in itertools.batched(tasks, batch_size):
        results.extend(await asyncio.gather(*batch))
        total += len(batch)
        logger.info(f"  {total} of {len(tasks)}")
    return results


async def batch_db[R](
    pool: DBPool,
    tasks: t.Iterable[t.Callable[[DBConnection], t.Awaitable[R]]],
    batch_size: int = 50,
) -> list[R]:
    """Batches database operations. DO NOT USE `lambda conn: ...` BECAUSE OF HOW PYTHON CAPTURES LAMBDA VARIABLES. Use `functools.partial` instead.
    - https://www.youtube.com/watch?v=fZE6ZWde-Os
    - https://www.youtube.com/watch?v=jXugs4B3lwU
    """
    done = 0
    tasks = list(tasks)

    async def wrapper(
        pool: DBPool, ts: t.Iterable[t.Callable[[DBConnection], t.Awaitable[R]]]
    ):
        results: list[R] = []
        processed = 0
        async with pool.acquire() as conn:
            for task in ts:
                coro = task(conn)
                results.append(await coro)

                processed += 1

        nonlocal done
        done += processed
        logger.info(f"  {done}")
        return results

    ret = await asyncio.gather(
        *[wrapper(pool, tasks2) for tasks2 in itertools.batched(tasks, batch_size)]
    )
    return list(itertools.chain.from_iterable(ret))
