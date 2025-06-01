import asyncio
import itertools
import typing as t
from datetime import datetime

import httpx

from db_update import api
from db_update.db import mlb_db as db
from db_update.db_pool import DBConnection, DBPool
from db_update.logger import logger


def int_safe(v: str) -> int:
    try:
        return int(v)
    except ValueError:
        return 0


def float_safe(v: str) -> float:
    try:
        return float(v)
    except ValueError:
        return 0


def bool_maybe(v: str) -> bool | None:
    if v == "True":
        return True
    elif v == "False":
        return False
    else:
        return None


def int_maybe(v: str) -> int | None:
    try:
        return int(v)
    except ValueError:
        return None


def float_maybe(v: str) -> float | None:
    try:
        return float(v)
    except ValueError:
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
    done = 0

    async def wrapper(
        pool: DBPool, tasks2: t.Iterable[t.Callable[[DBConnection], t.Awaitable[R]]]
    ):
        results: list[R] = []
        processed = 0
        async with pool.acquire() as conn:
            for task in tasks2:
                processed += 1
                results.append(await task(conn))

        nonlocal done
        done += processed
        logger.info(f"  {done}")
        return results

    ret = await asyncio.gather(
        *[wrapper(pool, tasks2) for tasks2 in itertools.batched(tasks, batch_size)]
    )
    return list(itertools.chain.from_iterable(ret))


async def run(pool: DBPool):
    logger.info("Fetching MLB teams and players")
    async with httpx.AsyncClient(timeout=30) as client:
        teams, players = await asyncio.gather(
            api.get_mlb_teams(client),
            api.get_mlb_players(client),
        )
    logger.info(f"Fetched {len(teams)} teams and {len(players)} players")

    logger.info("Upserting MLB teams")
    await batch_db(
        pool,
        (
            lambda conn: db.mlb_teams_upsert(
                conn,
                team_abv=team.teamAbv,
                team_city=team.teamCity,
                team_name=team.teamName,
                conference=team.conference,
                division=team.division,
                rs=int_safe(team.RS),
                ra=int_safe(team.RA),
                wins=int_safe(team.wins),
                losses=int_safe(team.loss),
                run_diff=int_safe(team.DIFF),
            )
            for team in teams
        ),
    )

    logger.info("Fetching MLB player info")
    async with httpx.AsyncClient(timeout=30) as client:
        players_details = await batch(
            (api.get_mlb_player_info(client, player.playerID) for player in players),
        )
    logger.info(f"Fetched {len(players_details)} player info")

    logger.info(f"Upserting MLB {len(players_details)} players")
    await batch_db(
        pool,
        (
            lambda conn: db.mlb_players_upsert(
                conn,
                player_id=int(player.player_id),
                long_name=player.long_name,
                team_abv=player.team_abv,
                pos=player.pos,
                height=player.height,
                weight=int_safe(player.weight),
                bat=player.bat,
                throw=player.throw,
                b_day=datetime.strptime(player.b_day, "%m/%d/%Y").date()
                if player.b_day
                else None,
                mlb_headshot=player.mlb_headshot,
                espn_headshot=player.espn_headshot,
                espn_status=player.espn_status,
                injury_description=player.injury.description,
                injury_return=player.injury.inj_return_date,
            )
            for player in players_details
        ),
    )

    logger.info("Fetching MLB game stats")
    async with httpx.AsyncClient(timeout=30) as client:
        games_stats_list = await batch(
            (
                api.get_mlb_games_for_player(client, player.player_id)
                for player in players_details
            ),
        )
    logger.info(f"Fetched {len(games_stats_list)} game stats")

    logger.info(f"Upserting MLB {sum(len(gs) for gs in games_stats_list)} game stats")
    await batch_db(
        pool,
        (
            itertools.chain.from_iterable(
                (
                    lambda conn: db.mlb_player_game_stats_upsert(
                        conn,
                        player_id=int(gs.player_id),
                        game_id=game_id,
                        team=gs.team,
                        started=bool_maybe(gs.started),
                        starting_position=gs.starting_position,
                        all_positions_played=gs.all_positions_played,
                        bb=int_maybe(gs.hitting.bb),
                        ab=int_maybe(gs.hitting.ab),
                        h=int_maybe(gs.hitting.h),
                        hr=int_maybe(gs.hitting.hr),
                        rbi=int_maybe(gs.hitting.rbi),
                        so=int_maybe(gs.hitting.so),
                        avg=float_maybe(gs.hitting.avg),
                        tb=int_maybe(gs.hitting.tb),
                        doubles=None,
                        triples=None,
                        # doubles=int_maybe(gs.hitting.double),
                        # triples=int_maybe(gs.hitting.triple),
                        r=int_maybe(gs.hitting.r),
                        ibb=int_maybe(gs.hitting.ibb),
                        sf=int_maybe(gs.hitting.sf),
                        sac=int_maybe(gs.hitting.sac),
                        hbp=int_maybe(gs.hitting.hbp),
                        gidp=int_maybe(gs.hitting.gidp),
                        p_bb=int_maybe(gs.pitching.bb),
                        p_h=int_maybe(gs.pitching.h),
                        p_hr=int_maybe(gs.pitching.hr),
                        p_er=int_maybe(gs.pitching.er),
                        p_so=int_maybe(gs.pitching.so),
                        win=None,
                        loss=None,
                        save=None,
                        era=float_maybe(gs.pitching.era),
                        innings_pitched=float_maybe(gs.pitching.innings_pitched),
                        fielding_e=int_maybe(gs.fielding.e),
                        passed_ball=int_maybe(gs.fielding.passed_ball),
                        of_assists=int_maybe(gs.fielding.outfield_assists),
                        pickoffs=int_maybe(gs.fielding.pickoffs),
                        sb=int_maybe(gs.base_running.sb),
                        cs=int_maybe(gs.base_running.cs),
                        po=int_maybe(gs.base_running.po),
                    )
                    for game_id, gs in games_stats.items()
                )
                for games_stats in games_stats_list
            )
        ),
    )
