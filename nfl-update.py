import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from os import environ
from traceback import format_exception

import asyncpg
import httpx
import polars as pl

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(message)s",
)


async def db_connect():
    return await asyncpg.connect(
        f"postgresql://{environ["DB_USER"]}:{environ["DB_PASSWORD"]}@{environ["DB_HOST"]}/{environ["DB_NAME"]}",
    )


class APIClient:
    def __init__(
        self,
        base_url: str,
        timeout: int = 10,
        headers: dict | None = None,
        save_dir: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers = headers or {}
        self.save_dir = save_dir
        self.client = None  # client will be initialized in __aenter__

        if self.save_dir and not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=self.timeout, headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.client:
            await self.client.aclose()

    async def _save_response(self, json_data, endpoint):
        if self.save_dir:
            import aiofiles

            filename = os.path.join(
                self.save_dir, f"{endpoint.strip('/').replace('/', '_')}.json"
            )
            async with aiofiles.open(filename, "w") as f:
                await f.write(json.dumps(json_data, indent=4))
            # Save the timestamp of the response
            timestamp = datetime.now().isoformat()
            async with aiofiles.open(f"{filename}.timestamp", "w") as f:
                await f.write(timestamp)

    async def _load_cached_response(self, endpoint):
        if not self.save_dir:
            return None

        filename = os.path.join(
            self.save_dir, f"{endpoint.strip('/').replace('/', '_')}.json"
        )
        timestamp_file = f"{filename}.timestamp"

        if os.path.exists(filename) and os.path.exists(timestamp_file):
            import aiofiles

            async with aiofiles.open(timestamp_file, "r") as f:
                cached_time_str = await f.read()

            cached_time = datetime.fromisoformat(cached_time_str)
            if datetime.now() - cached_time < timedelta(hours=1):
                async with aiofiles.open(filename, "r") as f:
                    cached_data = await f.read()
                    return json.loads(cached_data)
        return None

    async def get(
        self, endpoint: str, params: dict | None = None, headers: dict | None = None
    ) -> dict:
        cached_response = await self._load_cached_response(endpoint)
        if cached_response:
            return cached_response

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        assert self.client is not None
        response = await self.client.get(url, params=params, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        await self._save_response(response_json, endpoint)
        return response_json


client = APIClient(
    "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
    headers={
        "x-rapidapi-key": environ["RAPIDAPI_KEY"],
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
    },
    # save_dir="./tmp-save/",
)


def opt_int(val: str | None) -> int | None:
    if not val:
        return None
    return int(val)


def try_float(val: str) -> float:
    try:
        return float(val)
    except ValueError:
        return 0.0


async def main():
    try:
        logging.info("fetching data")
        ################################################################################
        #                                FETCH DATA
        ################################################################################
        async with client:
            data_teams = await client.get(
                "/getNFLTeams",
                params={
                    "sortBy": "standings",
                    "rosters": "false",
                    "schedules": "false",
                    "topPerformers": "false",
                    "teamStats": "true",
                    "teamStatsSeason": 2024,
                },
            )
            logging.info(f"  got {len(data_teams['body'])} teams")
            data_players = await client.get("/getNFLPlayerList")
            logging.info(f"  got {len(data_players['body'])} players")

        logging.info("creating dataframes")
        df_teams = pl.DataFrame(data_teams["body"])
        df_teams = df_teams.with_columns(
            pl.concat_str(
                [
                    pl.col("teamCity"),
                    pl.col("teamName"),
                ],
                separator=" ",
            ).alias("name"),
        )
        df_teams = df_teams.with_columns(
            teamAbv=pl.when(pl.col("teamAbv") == "WSH")
            .then(pl.lit("WAS"))
            .otherwise(pl.col("teamAbv"))
        )

        logging.info("connecting to database")
        conn = await db_connect()

        # logging.info("deleting from tables")
        # await conn.execute("DELETE FROM v3_nfl_game_stats;")
        # await conn.execute("DELETE FROM v3_nfl_games;")
        # await conn.execute("DELETE FROM v3_nfl_players;")
        # await conn.execute("DELETE FROM v3_nfl_teams;")
        # logging.info("deleted from tables")

        ################################################################################
        #                                INSERT TEAMS
        ################################################################################
        logging.info("inserting teams")
        teams = df_teams.to_dicts()
        query = """
        INSERT INTO v3_nfl_teams (
            name, team_code, wins, losses, ties,
            points_for, points_against, total_tackles, fumbles_lost, defensive_touchdowns,
            fumbles_recovered, solo_tackles, defensive_interceptions, qb_hits,
            tackles_for_loss, pass_deflections, sacks, fumbles, passing_td_allowed,
            passing_yards_allowed, rushing_yards_allowed, rushing_td_allowed
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        ON CONFLICT (team_code) DO UPDATE SET
            name = EXCLUDED.name,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            ties = EXCLUDED.ties,
            points_for = EXCLUDED.points_for,
            points_against = EXCLUDED.points_against,
            total_tackles = EXCLUDED.total_tackles,
            fumbles_lost = EXCLUDED.fumbles_lost,
            defensive_touchdowns = EXCLUDED.defensive_touchdowns,
            fumbles_recovered = EXCLUDED.fumbles_recovered,
            solo_tackles = EXCLUDED.solo_tackles,
            defensive_interceptions = EXCLUDED.defensive_interceptions,
            qb_hits = EXCLUDED.qb_hits,
            tackles_for_loss = EXCLUDED.tackles_for_loss,
            pass_deflections = EXCLUDED.pass_deflections,
            sacks = EXCLUDED.sacks,
            fumbles = EXCLUDED.fumbles,
            passing_td_allowed = EXCLUDED.passing_td_allowed,
            passing_yards_allowed = EXCLUDED.passing_yards_allowed,
            rushing_yards_allowed = EXCLUDED.rushing_yards_allowed,
            rushing_td_allowed = EXCLUDED.rushing_td_allowed
        """
        batch_data = [
            (
                team["name"],
                team["teamAbv"],
                int(team["wins"]),
                int(team["loss"]),
                int(team["tie"]),
                int(team["pf"]),
                int(team["pa"]),
                int(team["teamStats"]["Defense"]["totalTackles"]),
                int(team["teamStats"]["Defense"]["fumblesLost"]),
                int(team["teamStats"]["Defense"]["defTD"]),
                int(team["teamStats"]["Defense"]["fumblesRecovered"]),
                int(team["teamStats"]["Defense"]["soloTackles"]),
                int(team["teamStats"]["Defense"]["defensiveInterceptions"]),
                int(team["teamStats"]["Defense"]["qbHits"]),
                int(team["teamStats"]["Defense"]["tfl"]),
                int(team["teamStats"]["Defense"]["passDeflections"]),
                int(team["teamStats"]["Defense"]["sacks"]),
                int(team["teamStats"]["Defense"]["fumbles"]),
                int(team["teamStats"]["Defense"]["passingTDAllowed"]),
                int(team["teamStats"]["Defense"]["passingYardsAllowed"]),
                int(team["teamStats"]["Defense"]["rushingYardsAllowed"]),
                int(team["teamStats"]["Defense"]["rushingTDAllowed"]),
            )
            for team in teams
        ]

        await conn.executemany(query, batch_data)
        logging.info("inserted teams")

        ################################################################################
        #                                INSERT PLAYERS
        ################################################################################
        logging.info("updating players")
        logging.info(" querying teams")
        rows_teams = await conn.fetch("SELECT * FROM v3_nfl_teams")
        logging.info(f" got {len(rows_teams)} teams")

        logging.info(" creating dataframe")
        df_players = pl.DataFrame(data_players["body"])
        df_players = df_players.with_columns(pl.col("espnID").cast(pl.Int64))
        df_players = df_players.with_columns(
            team=pl.when(pl.col("team") == "WSH")
            .then(pl.lit("WAS"))
            .otherwise(pl.col("team"))
        )

        df_team = pl.DataFrame([dict(r) for r in rows_teams])
        before_len = len(df_team)
        df_players = df_players.join(
            df_team, left_on="team", right_on="team_code", how="inner"
        )
        assert before_len == len(df_team)
        df_players = df_players.with_columns(
            pl.col("injury").map_elements(
                lambda x: json.dumps(x), return_dtype=pl.String
            )
        )

        logging.info("  inserting players")
        players = df_players.to_dicts()

        await conn.executemany(
            """
            INSERT INTO v3_nfl_players (
                id, team_id, name, height, position, injuries
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                injuries = EXCLUDED.injuries
            """,
            [
                (
                    player["espnID"],
                    player["id"],
                    player["espnName"],
                    player["height"],
                    player["pos"],
                    player["injury"],
                )
                for player in players
            ],
        )
        logging.info(f"  inserted {len(players)} players")

        ################################################################################
        #                                INSERT GAMES AND GAME STATS
        ################################################################################
        logging.info("updating games")
        logging.info("  querying players")
        rows_players = await conn.fetch("SELECT id FROM v3_nfl_players")
        logging.info(f"  got {len(rows_players)} players")

        await conn.close()
        data_player_stats = []

        async def get_player_games(player_id):
            data = await client.get(f"/getNFLGamesForPlayer?playerID={player_id}")
            return data["body"]

        logging.info("  fetching player stats")
        num_players = len(rows_players)
        fetch_batch_size = 50
        async with client:
            for i in range(0, num_players, fetch_batch_size):
                logging.info(f"    {i}/{num_players}")
                tasks = []
                for row in rows_players[i : i + fetch_batch_size]:
                    if i >= num_players:
                        continue
                    tasks.append(get_player_games(row["id"]))
                data_player_stats.extend(await asyncio.gather(*tasks))

        logging.info("  creating dataframe")
        games = []
        for data in data_player_stats:
            for k, v in data.items():
                date, teams = k.split("_")
                away, home = teams.split("@")
                v["date"] = datetime.strptime(date, "%Y%m%d")
                v["home"] = home if home != "WSH" else "WAS"
                v["away"] = away if away != "WSH" else "WAS"
                Defense = v.get("Defense", {})
                if Defense is not None:
                    v["tfl"] = try_float(Defense.get("tfl", 0.0))
                    v["defTD"] = try_float(Defense.get("defTD", 0.0))
                    v["sacks"] = try_float(Defense.get("sacks", 0.0))
                    v["qbHits"] = try_float(Defense.get("qbHits", 0.0))
                    v["fumbles"] = try_float(Defense.get("fumbles", 0.0))
                    v["fumblesLost"] = try_float(Defense.get("fumblesLost", 0.0))
                    v["soloTackles"] = try_float(Defense.get("soloTackles", 0.0))
                    v["totalTackles"] = try_float(Defense.get("totalTackles", 0.0))
                    v["forcedFumbles"] = try_float(Defense.get("forcedFumbles", 0.0))
                    v["passDeflections"] = try_float(
                        Defense.get("passDeflections", 0.0)
                    )
                    v["fumblesRecovered"] = try_float(
                        Defense.get("fumblesRecovered", 0.0)
                    )
                    v["defensiveInterceptions"] = try_float(
                        Defense.get("defensiveInterceptions", 0.0)
                    )

                Passing = v.get("Passing", {})
                if Passing is not None:
                    v["int"] = try_float(Passing.get("int", 0.0))
                    v["qbr"] = try_float(Passing.get("qbr", 0.0))
                    v["rtg"] = try_float(Passing.get("rtg", 0.0))
                    v["passTD"] = try_float(Passing.get("passTD", 0.0))
                    v["sacked"] = Passing.get("sacked", None)
                    v["passAvg"] = try_float(Passing.get("passAvg", 0.0))
                    v["passYds"] = try_float(Passing.get("passYds", 0.0))
                    v["passAttempts"] = try_float(Passing.get("passAttempts", 0.0))
                    v["passCompletions"] = try_float(
                        Passing.get("passCompletions", 0.0)
                    )

                Rushing = v.get("Rushing", {})
                if Rushing is not None:
                    v["rushTD"] = try_float(Rushing.get("rushTD", 0.0))
                    v["carries"] = try_float(Rushing.get("carries", 0.0))
                    v["rushAvg"] = try_float(Rushing.get("rushAvg", 0.0))
                    v["rushYds"] = try_float(Rushing.get("rushYds", 0.0))
                    v["longRush"] = try_float(Rushing.get("longRush", 0.0))

                Receiving = v.get("Receiving", {})
                if Receiving is not None:
                    v["recTD"] = try_float(Receiving.get("recTD", 0.0))
                    v["recAvg"] = try_float(Receiving.get("recAvg", 0.0))
                    v["recYds"] = try_float(Receiving.get("recYds", 0.0))
                    v["longRec"] = try_float(Receiving.get("longRec", 0.0))
                    v["targets"] = try_float(Receiving.get("targets", 0.0))
                    v["receptions"] = try_float(Receiving.get("receptions", 0.0))

                Kicking = v.get("Kicking", {})
                if Kicking is not None:
                    v["kickReturns"] = try_float(Kicking.get("kickReturns", 0.0))
                    v["kickReturnTD"] = try_float(Kicking.get("kickReturnTD", 0.0))
                    v["kickReturnAvg"] = try_float(Kicking.get("kickReturnAvg", 0.0))
                    v["kickReturnYds"] = try_float(Kicking.get("kickReturnYds", 0.0))
                    v["kickReturnLong"] = try_float(Kicking.get("kickReturnLong", 0.0))
                    v["fgMade"] = try_float(Kicking.get("fgMade", 0.0))
                    v["xpMade"] = try_float(Kicking.get("xpMade", 0.0))
                    v["kickingPts"] = try_float(Kicking.get("kickingPts", 0.0))

                games.append(v)
        df_games = pl.DataFrame(games, infer_schema_length=2000)
        logging.info("  creating games dataframe")

        logging.info("  connecting to database")
        conn = await db_connect()
        logging.info("  querying teasm")
        rows_teams = await conn.fetch("SELECT id, team_code FROM v3_nfl_teams")

        logging.info("  creating teams dataframe")
        df_teams = pl.DataFrame([dict(row) for row in rows_teams])
        df_games = df_games.with_columns(
            pl.arange(0, df_games.height).alias("id")
        )  # dummy id column for forced suffix join naming
        df_games = df_games.join(
            df_teams, left_on="home", right_on="team_code", suffix="_home"
        )
        df_games = df_games.join(
            df_teams, left_on="away", right_on="team_code", suffix="_away"
        )
        df_games = df_games.cast({"playerID": pl.Int64})
        logging.info(f"  raw games count {len(df_games)}")

        unique_games = list(set(df_games["id_home", "id_away", "date"].rows()))
        logging.info(f"  unique games count {len(unique_games)}")

        logging.info("  inserting games")
        await conn.executemany(
            """
        INSERT INTO v3_nfl_games
            (home_id, away_id, date)
        VALUES
            ($1, $2, $3)
        ON CONFLICT 
            DO NOTHING;
        """,
            unique_games,
        )

        logging.info(f"  inserted {len(games)} games")
        ################################################################################
        #                                INSERT GAMES AND GAME STATS
        ################################################################################
        logging.info("updating game stats")
        logging.info("  querying games")
        games_neon = await conn.fetch("SELECT * FROM v3_nfl_games")
        df_games_neon = pl.DataFrame([dict(game) for game in games_neon])
        logging.info(f"  got {len(df_games_neon)} games")

        logging.info("  creating game stats dataframe")
        game_to_gameid = {}
        for g in df_games_neon.iter_rows(named=True):
            game_to_gameid[(g["home_id"], g["away_id"], g["date"])] = g["id"]

        rows_game_stats = []
        for game in df_games.iter_rows(named=True):
            game_id_in_neon = game_to_gameid[
                (game["id_home"], game["id_away"], game["date"].date())
            ]
            row = [game_id_in_neon, int(game["playerID"])]
            row.extend(
                [
                    game["tfl"],
                    game["defTD"],
                    game["sacks"],
                    game["qbHits"],
                    game["fumbles"],
                    game["fumblesLost"],
                    game["soloTackles"],
                    game["totalTackles"],
                    game["forcedFumbles"],
                    game["passDeflections"],
                    game["fumblesRecovered"],
                    game["defensiveInterceptions"],
                    game["int"],
                    game["qbr"],
                    game["rtg"],
                    game["passTD"],
                    game["sacked"],
                    game["passAvg"],
                    game["passYds"],
                    game["passAttempts"],
                    game["passCompletions"],
                    game["rushTD"],
                    game["carries"],
                    game["rushAvg"],
                    game["rushYds"],
                    game["longRush"],
                    game["recTD"],
                    game["recAvg"],
                    game["recYds"],
                    game["longRec"],
                    game["targets"],
                    game["receptions"],
                    game["kickReturns"],
                    game["kickReturnTD"],
                    game["kickReturnAvg"],
                    game["kickReturnYds"],
                    game["kickReturnLong"],
                    game["fgMade"],
                    game["xpMade"],
                    game["kickingPts"],
                ]
            )
            rows_game_stats.append(row)

        logging.info("  connecting to database")
        conn = await db_connect()

        insert_query = """
        INSERT INTO v3_nfl_game_stats (
            game_id,
            player_id,
            tfl,
            def_td,
            sacks,
            qb_hits,
            fumbles,
            fumbles_lost,
            solo_tackles,
            total_tackles,
            forced_fumbles,
            pass_deflections,
            fumbles_recovered,
            defensive_interceptions,
            int,
            qbr,
            rtg,
            pass_td,
            sacked,
            pass_avg,
            pass_yds,
            pass_attempts,
            pass_completions,
            rush_td,
            carries,
            rush_avg,
            rush_yds,
            long_rush,
            rec_td,
            rec_avg,
            rec_yds,
            long_rec,
            targets,
            receptions,
            kick_returns,
            kick_return_td,
            kick_return_avg,
            kick_return_yds,
            kick_return_long,
            kick_fg,
            kick_extra_points,
            kick_points
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42)
        ON CONFLICT DO NOTHING;
        """

        logging.info("  inserting game stats")
        await conn.executemany(insert_query, rows_game_stats)
        logging.info(f"  inserted {len(rows_game_stats)} game stats")
        await conn.close()
    except Exception as e:
        import pdb

        logging.error("".join(format_exception(e)))
        pdb.set_trace()


if __name__ == "__main__":
    asyncio.run(main())
