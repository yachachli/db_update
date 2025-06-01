import asyncio
import functools
import itertools
from datetime import datetime

from db_update.api import wnba_api
from db_update.async_caching_client import AsyncCachingClient
from db_update.db import wnba_db as db
from db_update.db_pool import DBConnection, DBPool
from db_update.logger import logger
from db_update.utils import batch, batch_db, decimal_safe, float_safe, int_safe


async def run(pool: DBPool):
    logger.info("Fetching WNBA teams and players")
    async with AsyncCachingClient(timeout=30) as client, pool.acquire() as conn:
        teams, players, season_id_maybe = await asyncio.gather(
            wnba_api.get_wnba_teams(client),
            wnba_api.get_wnba_players(client),
            db.wnba_season_id(conn, season_year=str(datetime.now().year)),
        )

    if not season_id_maybe:
        raise ValueError(f"Season {datetime.now().year} not found")
    season_id = season_id_maybe
    logger.info(f"Fetched {len(teams)} teams and {len(players)} players")

    logger.info("Upserting WNBA teams")
    await batch_db(
        pool,
        (
            functools.partial(
                db.wnba_team_upsert,
                name=team.team_name,
                team_city=team.team_city,
                team_abv=team.team_abv,
                conference=team.conference,
                ppg=float_safe(team.ppg),
                oppg=float_safe(team.oppg),
                wins=int_safe(team.wins),
                loss=int_safe(team.loss),
                team_bpg=float_safe(team.defensive_stats.blk.total),
                team_spg=float_safe(team.defensive_stats.stl.total),
                team_apg=float_safe(team.offensive_stats.ast.total),
                team_fga=float_safe(team.offensive_stats.fga.total),
                team_fgm=float_safe(team.offensive_stats.fgm.total),
                team_fta=float_safe(team.offensive_stats.fta.total),
                team_tov=float_safe(team.defensive_stats.tov.total),
            )
            for team in teams
        ),
    )

    logger.info("Fetching WNBA player info")
    async with AsyncCachingClient(timeout=30) as client:
        players_details = await batch(
            (
                wnba_api.get_wnba_player_info(client, player.player_id)
                for player in players
            ),
        )

    logger.info(f"Fetched {len(players_details)} player info")
    logger.info(f"Upserting WNBA {len(players_details)} players")
    await batch_db(
        pool,
        (
            functools.partial(
                db.wnba_player_upsert,
                name=player.long_name,
                position=player.pos,
                team_id=int(player.team_id),
                player_id=int(player.player_id),
            )
            for player in players_details
        ),
    )

    logger.info(f"Upserting WNBA {len(players_details)} players season stats")
    await batch_db(
        pool,
        [
            functools.partial(
                db.wnba_player_season_stats_upsert,
                player_id=print("inner", player.player_id) or int(player.player_id),
                season_id=season_id,
                games_played=int_safe(player.stats.games_played),
                points_per_game=decimal_safe(player.stats.pts),
                rebounds_per_game=decimal_safe(player.stats.reb),
                assists_per_game=decimal_safe(player.stats.ast),
                steals_per_game=decimal_safe(player.stats.stl),
                blocks_per_game=decimal_safe(player.stats.blk),
                turnovers_per_game=decimal_safe(player.stats.tov),
                field_goal_percentage=decimal_safe(player.stats.fgp),
                three_point_percentage=decimal_safe(player.stats.tptfgp),
                free_throw_percentage=decimal_safe(player.stats.ftp),
                minutes_per_game=decimal_safe(player.stats.mins),
                offensive_rebounds_per_game=decimal_safe(player.stats.off_reb),
                defensive_rebounds_per_game=decimal_safe(player.stats.def_reb),
                field_goals_made_per_game=decimal_safe(player.stats.fgm),
                field_goals_attempted_per_game=decimal_safe(player.stats.fga),
                three_pointers_made_per_game=decimal_safe(player.stats.tptfgm),
                three_pointers_attempted_per_game=decimal_safe(player.stats.tptfga),
                free_throws_made_per_game=decimal_safe(player.stats.ftm),
                free_throws_attempted_per_game=decimal_safe(player.stats.fta),
            )
            for player in players_details
        ],
    )

    logger.info("Fetching WNBA game stats")
    async with AsyncCachingClient(timeout=30) as client:
        games_stats_list = await batch(
            (
                wnba_api.get_wnba_games_for_player(client, player.player_id)
                for player in players_details
            ),
        )
    logger.info(f"Fetched {len(games_stats_list)} game stats")

    logger.info(f"Upserting WNBA {sum(len(gs) for gs in games_stats_list)} game stats")

    async def wnba_player_game_stats_upsert(
        conn: DBConnection, game_id: str, gs: wnba_api.WnbaGame
    ):
        date_string, teams_string = game_id.split("_")
        game_date = datetime.strptime(date_string, "%Y%m%d").date()
        away, home = teams_string.split("@")
        home_away, opponent = ("Home", away) if gs.team_abv == home else ("Away", home)

        return await db.wnba_player_game_stats_upsert(
            conn,
            player_id=int(gs.player_id),
            game_id=game_id,
            team_id=int_safe(gs.team_id),
            minutes_played=decimal_safe(gs.mins),
            points=int_safe(gs.pts),
            rebounds=int_safe(gs.reb),
            assists=int_safe(gs.ast),
            steals=int_safe(gs.stl),
            blocks=int_safe(gs.blk),
            turnovers=int_safe(gs.blk),
            offensive_rebounds=int_safe(gs.off_reb),
            defensive_rebounds=int_safe(gs.def_reb),
            free_throw_percentage=decimal_safe(gs.ftp),
            plus_minus=decimal_safe(gs.plus_minus),
            technical_fouls=int_safe(gs.tech),
            field_goal_attempts=int_safe(gs.fga),
            three_point_fg_percentage=decimal_safe(gs.tptfgp),
            field_goals_made=int_safe(gs.fgm),
            field_goal_percentage=decimal_safe(gs.fgp),
            three_point_fg_made=int_safe(gs.tptfgm),
            free_throw_attempts=int_safe(gs.fta),
            three_point_fg_attempts=int_safe(gs.tptfga),
            personal_fouls=int_safe(gs.pf),
            free_throws_made=int_safe(gs.ftm),
            fantasy_points=decimal_safe(gs.fantasy_points),
            home_away=home_away,
            opponent=opponent,
            game_date=game_date,
            team_abv=gs.team_abv,
        )

    await batch_db(
        pool,
        (
            itertools.chain.from_iterable(
                (
                    functools.partial(
                        wnba_player_game_stats_upsert, game_id=game_id, gs=gs
                    )
                    for game_id, gs in games_stats.items()
                )
                for games_stats in games_stats_list
            )
        ),
    )
