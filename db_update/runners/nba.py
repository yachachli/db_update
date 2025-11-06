import asyncio
import functools
from collections import defaultdict
from datetime import date, datetime
from typing import Any

import httpx

from db_update.db_pool import DBConnection, DBPool
from db_update.logger import logger
from db_update.utils import batch, batch_db, decimal_safe, float_safe, int_safe


RAPIDAPI_HOST = "tank01-fantasy-stats.p.rapidapi.com"


async def _fetch_json(client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    r = await client.get(
        url,
        params=params,
        headers={
            "x-rapidapi-host": RAPIDAPI_HOST,
            "x-rapidapi-key": client.headers.get("x-rapidapi-key", ""),
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


async def _get_player_ids(conn: DBConnection) -> list[int]:
    rows = await conn.fetch("SELECT player_id FROM nba_players")
    return [int(r["player_id"]) for r in rows]


async def _upsert_player_game_stat(conn: DBConnection, player_id: int, game_id: str, stats: dict[str, Any]) -> None:
    # Derive opponent/home_away from game_id and teamAbv
    home_away = ""
    opponent = ""
    game_date: date | None = None
    team_abv = stats.get("teamAbv", "")
    team_id = int_safe(stats.get("teamID"))

    if game_id:
        try:
            date_str, game = game_id.split("_")
            away_team, home_team = game.split("@")
            game_date = datetime.strptime(date_str, "%Y%m%d").date()
            if team_abv == away_team:
                opponent = home_team
                home_away = "Away"
            elif team_abv == home_team:
                opponent = away_team
                home_away = "Home"
        except Exception:
            pass

    query = """
    INSERT INTO nba_player_game_stats
    (player_id, game_id, team_id, minutes_played, points, rebounds, assists, steals, blocks, turnovers,
     offensive_rebounds, defensive_rebounds, free_throw_percentage, plus_minus, technical_fouls,
     field_goal_attempts, three_point_fg_percentage, field_goals_made, field_goal_percentage,
     three_point_fg_made, free_throw_attempts, three_point_fg_attempts, personal_fouls,
     free_throws_made, fantasy_points, home_away, opponent, game_date, team_abv)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
    ON CONFLICT (player_id, game_id) DO UPDATE SET
      team_id = EXCLUDED.team_id,
      minutes_played = EXCLUDED.minutes_played,
      points = EXCLUDED.points,
      rebounds = EXCLUDED.rebounds,
      assists = EXCLUDED.assists,
      steals = EXCLUDED.steals,
      blocks = EXCLUDED.blocks,
      turnovers = EXCLUDED.turnovers,
      offensive_rebounds = EXCLUDED.offensive_rebounds,
      defensive_rebounds = EXCLUDED.defensive_rebounds,
      free_throw_percentage = EXCLUDED.free_throw_percentage,
      plus_minus = EXCLUDED.plus_minus,
      technical_fouls = EXCLUDED.technical_fouls,
      field_goal_attempts = EXCLUDED.field_goal_attempts,
      three_point_fg_percentage = EXCLUDED.three_point_fg_percentage,
      field_goals_made = EXCLUDED.field_goals_made,
      field_goal_percentage = EXCLUDED.field_goal_percentage,
      three_point_fg_made = EXCLUDED.three_point_fg_made,
      free_throw_attempts = EXCLUDED.free_throw_attempts,
      three_point_fg_attempts = EXCLUDED.three_point_fg_attempts,
      personal_fouls = EXCLUDED.personal_fouls,
      free_throws_made = EXCLUDED.free_throws_made,
      fantasy_points = EXCLUDED.fantasy_points,
      home_away = EXCLUDED.home_away,
      opponent = EXCLUDED.opponent,
      game_date = EXCLUDED.game_date,
      team_abv = EXCLUDED.team_abv
    """
    await conn.execute(
        query,
        player_id,
        game_id,
        team_id,
        float_safe(stats.get("mins")),
        int_safe(stats.get("pts")),
        int_safe(stats.get("reb")),
        int_safe(stats.get("ast")),
        int_safe(stats.get("stl")),
        int_safe(stats.get("blk")),
        int_safe(stats.get("TOV")),
        int_safe(stats.get("OffReb")),
        int_safe(stats.get("DefReb")),
        decimal_safe(stats.get("ftp")),
        decimal_safe(stats.get("plusMinus")),
        int_safe(stats.get("tech")),
        int_safe(stats.get("fga")),
        decimal_safe(stats.get("tptfgp")),
        int_safe(stats.get("fgm")),
        decimal_safe(stats.get("fgp")),
        int_safe(stats.get("tptfgm")),
        int_safe(stats.get("fta")),
        int_safe(stats.get("tptfga")),
        int_safe(stats.get("PF")),
        int_safe(stats.get("ftm")),
        decimal_safe(stats.get("fantasyPoints")),
        home_away,
        opponent,
        game_date,
        team_abv,
    )


async def _update_injuries(pool: DBPool, client: httpx.AsyncClient) -> None:
    url = f"https://{RAPIDAPI_HOST}/getNBAInjuryList"
    data = await _fetch_json(client, url)
    injury_list: list[dict[str, Any]] = data.get("body", []) if data.get("statusCode") == 200 else []

    def is_current(injury: dict[str, Any]) -> bool:
        today = date.today().strftime("%Y%m%d")
        x = injury.get("injReturnDate")
        return (not x) or (x >= today)

    # pick most recent injury per player
    latest: dict[str, dict[str, Any]] = {}
    for inj in injury_list:
        if not is_current(inj):
            continue
        pid = inj.get("playerID")
        if not pid:
            continue
        prev = latest.get(pid)
        if prev is None or (inj.get("injDate") or "") > (prev.get("injDate") or ""):
            latest[pid] = inj

    players_with_injury = [int(pid) for pid in latest.keys()]

    async def upsert_injury(conn: DBConnection, pid: int, inj: dict[str, Any]):
        await conn.execute(
            """
            UPDATE nba_players
            SET injury = $1::jsonb
            WHERE player_id = $2
            """,
            __import__("json").dumps([inj]),
            int(pid),
        )

    await batch_db(
        pool,
        (
            functools.partial(upsert_injury, pid=pid, inj=inj)
            for pid, inj in latest.items()
        ),
    )

    if players_with_injury:
        async def clear_others(conn: DBConnection):
            await conn.execute(
                """
                UPDATE nba_players
                SET injury = NULL
                WHERE player_id <> ALL($1::int[])
                """,
                players_with_injury,
            )
        await batch_db(pool, (functools.partial(clear_others),))


async def _update_player_info_and_season(pool: DBPool, client: httpx.AsyncClient) -> None:
    # Fetch first names and group
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT SPLIT_PART(name, ' ', 1) AS first_name, name
            FROM nba_players
            ORDER BY first_name
            """
        )
    grouped: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        grouped[r["first_name"]].append(r["name"])

    async def fetch_players(first_name: str) -> tuple[str, list[dict[str, Any]]]:
        url = f"https://{RAPIDAPI_HOST}/getNBAPlayerInfo"
        data = await _fetch_json(client, url, {"playerName": first_name, "statsToGet": "averages"})
        if data.get("statusCode") == 200 and data.get("body"):
            return first_name, list(data["body"])
        return first_name, []

    fetched = await batch((fetch_players(fn) for fn in grouped.keys()))
    now_year = str(datetime.now().year)

    async def upsert_info(conn: DBConnection, p: dict[str, Any]) -> None:
        await conn.execute(
            """
            UPDATE nba_players
            SET player_pic = $1,
                team_id = $2
            WHERE player_id = $3
            """,
            p.get("nbaComHeadshot"),
            p.get("teamID"),
            int_safe(p.get("playerID")),
        )

        # Upsert season stats
        season_id_row = await conn.fetchrow(
            "SELECT id FROM nba_seasons WHERE season_year = $1",
            now_year,
        )
        season_id = season_id_row[0] if season_id_row else None
        if not season_id:
            return
        s = p.get("stats") or {}
        await conn.execute(
            """
            INSERT INTO nba_player_season_stats
            (player_id, season_id, games_played, points_per_game, rebounds_per_game,
             assists_per_game, steals_per_game, blocks_per_game, turnovers_per_game,
             field_goal_percentage, three_point_percentage, free_throw_percentage,
             minutes_per_game, offensive_rebounds_per_game, defensive_rebounds_per_game,
             field_goals_made_per_game, field_goals_attempted_per_game,
             three_pointers_made_per_game, three_pointers_attempted_per_game,
             free_throws_made_per_game, free_throws_attempted_per_game)
            VALUES (
              (SELECT id FROM nba_players WHERE player_id = $1),
              $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            )
            ON CONFLICT (player_id, season_id) DO UPDATE SET
              games_played = EXCLUDED.games_played,
              points_per_game = EXCLUDED.points_per_game,
              rebounds_per_game = EXCLUDED.rebounds_per_game,
              assists_per_game = EXCLUDED.assists_per_game,
              steals_per_game = EXCLUDED.steals_per_game,
              blocks_per_game = EXCLUDED.blocks_per_game,
              turnovers_per_game = EXCLUDED.turnovers_per_game,
              field_goal_percentage = EXCLUDED.field_goal_percentage,
              three_point_percentage = EXCLUDED.three_point_percentage,
              free_throw_percentage = EXCLUDED.free_throw_percentage,
              minutes_per_game = EXCLUDED.minutes_per_game,
              offensive_rebounds_per_game = EXCLUDED.offensive_rebounds_per_game,
              defensive_rebounds_per_game = EXCLUDED.defensive_rebounds_per_game,
              field_goals_made_per_game = EXCLUDED.field_goals_made_per_game,
              field_goals_attempted_per_game = EXCLUDED.field_goals_attempted_per_game,
              three_pointers_made_per_game = EXCLUDED.three_pointers_made_per_game,
              three_pointers_attempted_per_game = EXCLUDED.three_pointers_attempted_per_game,
              free_throws_made_per_game = EXCLUDED.free_throws_made_per_game,
              free_throws_attempted_per_game = EXCLUDED.free_throws_attempted_per_game
            """,
            int_safe(p.get("playerID")),
            season_id,
            int_safe(s.get("gamesPlayed")),
            float_safe(s.get("pts")),
            float_safe(s.get("reb")),
            float_safe(s.get("ast")),
            float_safe(s.get("stl")),
            float_safe(s.get("blk")),
            float_safe(s.get("TOV")),
            float_safe(s.get("fgp")),
            float_safe(s.get("tptfgp")),
            float_safe(s.get("ftp")),
            float_safe(s.get("mins")),
            float_safe(s.get("OffReb")),
            float_safe(s.get("DefReb")),
            float_safe(s.get("fgm")),
            float_safe(s.get("fga")),
            float_safe(s.get("tptfgm")),
            float_safe(s.get("tptfga")),
            float_safe(s.get("ftm")),
            float_safe(s.get("fta")),
        )

    updates: list[functools.partial] = []
    for first_name, players_data in fetched:
        full_names = grouped[first_name]
        full_name_set = {n.lower() for n in full_names}
        for p in players_data:
            api_name = (p.get("longName") or "").strip().lower()
            # keep sync’s ignore quirk
            if api_name == "jaylin williams" and p.get("team") == "DEN":
                continue
            if api_name in full_name_set:
                updates.append(functools.partial(upsert_info, p=p))

    await batch_db(pool, updates)


async def _update_teams(pool: DBPool, client: httpx.AsyncClient) -> None:
    async with pool.acquire() as conn:
        team_rows = await conn.fetch("SELECT name FROM nba_teams")
        team_names = [r["name"] for r in team_rows]

    url = f"https://{RAPIDAPI_HOST}/getNBATeams"
    data = await _fetch_json(
        client,
        url,
        {
            "schedules": "false",
            "rosters": "false",
            "topPerformers": "true",
            "teamStats": "true",
            "statsToGet": "averages",
        },
    )
    teams = data.get("body", []) if data.get("statusCode") == 200 else []

    teams_by_name = {t.get("teamName", ""): t for t in teams}

    async def update_one(conn: DBConnection, team_name: str):
        t = teams_by_name.get(team_name)
        if not t:
            return
        await conn.execute(
            """
            UPDATE nba_teams
            SET ppg = $1, oppg = $2, wins = $3, loss = $4,
                team_bpg = $5, team_spg = $6, team_apg = $7,
                team_fga = $8, team_fgm = $9, team_fta = $10, team_tov = $11
            WHERE LOWER(name) = LOWER($12)
            """,
            t.get("ppg"),
            t.get("oppg"),
            t.get("wins"),
            t.get("loss"),
            (t.get("defensiveStats", {}).get("blk", {}) or {}).get("Total"),
            (t.get("defensiveStats", {}).get("stl", {}) or {}).get("Total"),
            (t.get("offensiveStats", {}).get("ast", {}) or {}).get("Total"),
            (t.get("offensiveStats", {}).get("fga", {}) or {}).get("Total"),
            (t.get("offensiveStats", {}).get("fgm", {}) or {}).get("Total"),
            (t.get("offensiveStats", {}).get("fta", {}) or {}).get("Total"),
            (t.get("defensiveStats", {}).get("TOV", {}) or {}).get("Total"),
            team_name,
        )

    await batch_db(pool, (functools.partial(update_one, team_name=name) for name in team_names))


async def run(pool: DBPool):
    logger.info("Starting async NBA update")
    # httpx client with key from env (Actions will pass RAPIDAPI_KEY)
    async with httpx.AsyncClient(headers={"x-rapidapi-key": ""}) as client:
        # Inject key from env read by httpx at request time; leave header holder
        import os
        client.headers["x-rapidapi-key"] = os.environ.get("RAPIDAPI_KEY", "")

        # Block 1: Player game stats
        logger.info("[1] fetching player ids")
        async with pool.acquire() as conn:
            player_ids = await _get_player_ids(conn)
        logger.info(f"[1] got {len(player_ids)} player ids")

        season_year = str(datetime.now().year)

        async def fetch_player_games(pid: int) -> tuple[int, dict[str, dict[str, Any]]]:
            url = f"https://{RAPIDAPI_HOST}/getNBAGamesForPlayer"
            data = await _fetch_json(client, url, {"playerID": pid, "statsToGet": season_year})
            body = data.get("body") if data.get("statusCode") == 200 else None
            return pid, body or {}

        games_results = await batch((fetch_player_games(pid) for pid in player_ids), batch_size=50)

        logger.info("[1] upserting player game stats")
        async def upsert_for_player(conn: DBConnection, pid: int, stats_map: dict[str, Any]):
            for game_id, stats in stats_map.items():
                if not isinstance(stats, dict):
                    continue
                await _upsert_player_game_stat(conn, pid, game_id, stats)

        await batch_db(
            pool,
            (
                functools.partial(upsert_for_player, pid=pid, stats_map=stats_map)
                for pid, stats_map in games_results
            ),
        )

        # Block 2: Injuries
        logger.info("[2] updating injuries")
        await _update_injuries(pool, client)

        # Block 3: Player info and season stats
        logger.info("[3] updating player info and season stats")
        await _update_player_info_and_season(pool, client)

        # Block 4: Team stats
        logger.info("[4] updating team stats")
        await _update_teams(pool, client)

    logger.info("Async NBA update done")


