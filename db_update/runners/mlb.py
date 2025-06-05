import asyncio
import functools
import itertools
from datetime import datetime

from db_update.api import mlb_api
from db_update.async_caching_client import AsyncCachingClient
from db_update.db import mlb_db as db
from db_update.db_pool import DBPool
from db_update.logger import logger
from db_update.utils import (
    batch,
    batch_db,
    bool_maybe,
    float_maybe,
    int_maybe,
    int_safe,
)


async def run(pool: DBPool):
    logger.info("Fetching MLB teams and players")
    async with AsyncCachingClient(timeout=30) as client:
        teams, players = await asyncio.gather(
            mlb_api.get_mlb_teams(client),
            mlb_api.get_mlb_players(client),
        )
    logger.info(f"Fetched {len(teams)} teams and {len(players)} players")

    logger.info("Upserting MLB teams")
    await batch_db(
        pool,
        (
            functools.partial(
                db.mlb_teams_upsert,
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
    async with AsyncCachingClient(timeout=30) as client:
        players_details = await batch(
            (
                mlb_api.get_mlb_player_info(client, player.playerID)
                for player in players
            ),
        )
    logger.info(f"Fetched {len(players_details)} player info")

    logger.info(f"Upserting MLB {len(players_details)} players")
    await batch_db(
        pool,
        (
            functools.partial(
                db.mlb_players_upsert,
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
    async with AsyncCachingClient(timeout=30) as client:
        games_stats_list = await batch(
            (
                mlb_api.get_mlb_games_for_player(client, player.player_id)
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
                    functools.partial(
                        db.mlb_player_game_stats_upsert,
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
