import asyncio
import functools
from collections import defaultdict
from datetime import date, datetime
from typing import Any

import httpx
import random
import time

from db_update.db_pool import DBConnection, DBPool
from db_update.logger import logger
from db_update.utils import batch, batch_db, decimal_safe, float_safe, int_safe


RAPIDAPI_HOST = "tank01-fantasy-stats.p.rapidapi.com"

# Semaphore to limit concurrent API requests globally
# Reduced to 1 to be very conservative with rate limits
_api_semaphore = asyncio.Semaphore(1)


async def _fetch_json(client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    # Use semaphore to limit concurrent requests
    async with _api_semaphore:
        # Small delay before each request to space them out
        await asyncio.sleep(0.5)
        # Basic retry with exponential backoff and Retry-After support for 429
        attempts = 0
        last_exc: Exception | None = None
        while attempts < 6:
            attempts += 1
            r = await client.get(
                url,
                params=params,
                headers={
                    "x-rapidapi-host": RAPIDAPI_HOST,
                    "x-rapidapi-key": client.headers.get("x-rapidapi-key", ""),
                },
                timeout=60,
            )
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                try:
                    wait_s = float(retry_after) if retry_after is not None else min(2 ** attempts, 30)
                except ValueError:
                    wait_s = min(2 ** attempts, 30)
                # jitter
                wait_s += random.uniform(0, 0.25)
                logger.warning(f"429 from {url}, retrying in {wait_s:.2f}s (attempt {attempts})")
                # Set last_exc in case we exhaust all retries
                last_exc = httpx.HTTPStatusError(
                    f"429 Too Many Requests after {attempts} attempts",
                    request=r.request,
                    response=r,
                )
                if attempts >= 6:
                    break
                await asyncio.sleep(wait_s)
                continue
            if r.status_code == 403:
                # 403 Forbidden - likely due to rate limit violations, use longer backoff
                wait_s = min(60 * attempts, 300) + random.uniform(0, 10)  # Up to 5 minutes
                logger.warning(f"403 from {url}, retrying in {wait_s:.2f}s (attempt {attempts})")
                # Set last_exc in case we exhaust all retries
                last_exc = httpx.HTTPStatusError(
                    f"403 Forbidden after {attempts} attempts",
                    request=r.request,
                    response=r,
                )
                if attempts >= 6:
                    break
                await asyncio.sleep(wait_s)
                continue
            if 500 <= r.status_code < 600:
                wait_s = min(2 ** attempts, 20) + random.uniform(0, 0.25)
                logger.warning(f"{r.status_code} from {url}, retrying in {wait_s:.2f}s (attempt {attempts})")
                # Set last_exc in case we exhaust all retries
                last_exc = httpx.HTTPStatusError(
                    f"{r.status_code} Server Error after {attempts} attempts",
                    request=r.request,
                    response=r,
                )
                if attempts >= 6:
                    break
                await asyncio.sleep(wait_s)
                continue
            try:
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                break
            except Exception as exc:
                last_exc = exc
                wait_s = min(2 ** attempts, 10) + random.uniform(0, 0.25)
                logger.warning(f"Error fetching {url}: {exc}, retrying in {wait_s:.2f}s (attempt {attempts})")
                await asyncio.sleep(wait_s)
                continue
        assert last_exc is not None
        raise last_exc


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
                WHERE player_id <> ALL($1::bigint[])
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
            int_safe(p.get("teamID")),
            int_safe(p.get("playerID")),
        )

        # Upsert season stats
        season_id_row = await conn.fetchrow(
            "SELECT id FROM nba_seasons WHERE season_year = $1",
            now_year,
        )
        season_id = int_safe(season_id_row[0]) if season_id_row else None
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
            float_safe(t.get("ppg")),
            float_safe(t.get("oppg")),
            int_safe(t.get("wins")),
            int_safe(t.get("loss")),
            float_safe((t.get("defensiveStats", {}).get("blk", {}) or {}).get("Total")),
            float_safe((t.get("defensiveStats", {}).get("stl", {}) or {}).get("Total")),
            float_safe((t.get("offensiveStats", {}).get("ast", {}) or {}).get("Total")),
            float_safe((t.get("offensiveStats", {}).get("fga", {}) or {}).get("Total")),
            float_safe((t.get("offensiveStats", {}).get("fgm", {}) or {}).get("Total")),
            float_safe((t.get("offensiveStats", {}).get("fta", {}) or {}).get("Total")),
            float_safe((t.get("defensiveStats", {}).get("TOV", {}) or {}).get("Total")),
            team_name,
        )

    await batch_db(pool, (functools.partial(update_one, team_name=name) for name in team_names))


async def _update_team_defense_by_position(pool: DBPool) -> None:
    """
    Calculate and update team defense statistics by opponent position.
    This aggregates game stats to show how well each team defends against each position.
    """
    season_year = str(datetime.now().year)
    current_date = datetime.now().date()
    year_start = datetime(current_date.year, 1, 1).date()
    
    logger.info(f"[5] Starting team defense by position update for season {season_year}")
    logger.info(f"[5] Filtering games from {year_start} onwards (current date: {current_date})")
    
    async with pool.acquire() as conn:
        # Check how many game stats rows we have to work with
        stats_count = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM nba_player_game_stats gs
            INNER JOIN nba_players p ON gs.player_id = p.player_id
            WHERE gs.opponent IS NOT NULL 
                AND p.position IS NOT NULL
                AND gs.game_date >= DATE_TRUNC('year', CURRENT_DATE)
                AND gs.points IS NOT NULL
        """)
        logger.info(f"[5] Found {stats_count} player-game stats rows matching criteria")
        
        # Check date range of available data
        date_range = await conn.fetchrow("""
            SELECT 
                MIN(gs.game_date) AS min_date,
                MAX(gs.game_date) AS max_date,
                COUNT(DISTINCT gs.game_date) AS distinct_dates
            FROM nba_player_game_stats gs
            WHERE gs.game_date >= DATE_TRUNC('year', CURRENT_DATE)
        """)
        if date_range:
            logger.info(f"[5] Available game date range: {date_range['min_date']} to {date_range['max_date']} ({date_range['distinct_dates']} distinct dates)")
        
        # Delete existing data for current season to avoid duplicates
        existing_count = await conn.fetchval("""
            SELECT COUNT(*) FROM nba_team_defense_by_position WHERE season = $1
        """, season_year)
        if existing_count > 0:
            await conn.execute("""
                DELETE FROM nba_team_defense_by_position 
                WHERE season = $1
            """, season_year)
            logger.info(f"[5] Deleted {existing_count} existing rows for season {season_year}")
        else:
            logger.info(f"[5] No existing rows to delete for season {season_year}")
        
        # Query to calculate defense stats by position
        # First aggregate by game to get totals per game, then average across games
        # This ensures we get per-game averages, not per-player averages
        rows = await conn.fetch("""
            WITH game_totals AS (
                SELECT 
                    gs.opponent AS team_abv,
                    gs.game_id,
                    p.position AS vs_position,
                    SUM(gs.points) AS game_points,
                    SUM(gs.rebounds) AS game_rebounds,
                    SUM(gs.assists) AS game_assists
                FROM nba_player_game_stats gs
                INNER JOIN nba_players p ON gs.player_id = p.player_id
                WHERE gs.opponent IS NOT NULL 
                    AND p.position IS NOT NULL
                    AND gs.game_date >= DATE_TRUNC('year', CURRENT_DATE)
                    AND gs.points IS NOT NULL
                GROUP BY gs.opponent, gs.game_id, p.position
            )
            SELECT 
                team_abv,
                $1::VARCHAR AS season,
                vs_position,
                COALESCE(AVG(game_points), 0) AS pts_allowed_per_game,
                COALESCE(AVG(game_rebounds), 0) AS reb_allowed_per_game,
                COALESCE(AVG(game_assists), 0) AS ast_allowed_per_game,
                COUNT(*) AS games_sample_size
            FROM game_totals
            GROUP BY team_abv, vs_position
            HAVING COUNT(*) > 0
        """, season_year)
        
        logger.info(f"[5] Calculated defense-by-position stats for {len(rows)} team-position combinations")
        
        if len(rows) == 0:
            logger.warning(f"[5] WARNING: No defense-by-position stats calculated! This might indicate:")
            logger.warning(f"[5]   - Date filtering issue (games might be in different year)")
            logger.warning(f"[5]   - Missing opponent/position data in game stats")
            logger.warning(f"[5]   - No game data available for current season")
        elif len(rows) < 30:  # Roughly 30 teams * 5 positions = 150, so <30 is suspicious
            logger.warning(f"[5] WARNING: Only {len(rows)} team-position combinations found (expected ~150 for 30 teams * 5 positions)")
            logger.warning(f"[5] This might indicate incomplete data or date filtering issues")
        
        # Log a sample of the data being inserted
        if rows:
            sample = rows[0]
            logger.info(f"[5] Sample row: {sample['team_abv']} vs {sample['vs_position']} - "
                      f"{sample['pts_allowed_per_game']:.1f} pts, {sample['reb_allowed_per_game']:.1f} reb, "
                      f"{sample['ast_allowed_per_game']:.1f} ast ({sample['games_sample_size']} games)")
        
        # Insert each row
        inserted_count = 0
        for row in rows:
            await conn.execute("""
                INSERT INTO nba_team_defense_by_position (
                    team_abv, season, vs_position,
                    pts_allowed_per_game, reb_allowed_per_game, ast_allowed_per_game,
                    games_sample_size
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
                row["team_abv"],
                row["season"],
                row["vs_position"],
                float_safe(row["pts_allowed_per_game"]),
                float_safe(row["reb_allowed_per_game"]),
                float_safe(row["ast_allowed_per_game"]),
                int_safe(row["games_sample_size"]),
            )
            inserted_count += 1
        
        logger.info(f"[5] Successfully inserted {inserted_count} team defense-by-position records")


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

        # Use a smaller batch size to avoid RapidAPI 429 throttling
        # Combined with semaphore in _fetch_json to limit concurrent requests
        # Process batches manually with delays to further reduce rate limit issues
        import itertools
        games_results: list[tuple[int, dict[str, dict[str, Any]]]] = []
        tasks = [fetch_player_games(pid) for pid in player_ids]
        batch_size = 3  # Further reduced batch size
        total = 0
        for batch_tasks in itertools.batched(tasks, batch_size):
            batch_results = await asyncio.gather(*batch_tasks)
            games_results.extend(batch_results)
            total += len(batch_tasks)
            logger.info(f"  {total} of {len(tasks)}")
            # Longer delay between batches to avoid overwhelming the API
            if total < len(tasks):
                await asyncio.sleep(2.0)  # Increased delay between batches

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

        # Block 5: Team defense by position
        logger.info("[5] updating team defense by position")
        await _update_team_defense_by_position(pool)

    logger.info("Async NBA update done")


