import json
import logging
import os
import time
from collections import defaultdict
from datetime import date, datetime
from traceback import format_exception

import psycopg2
import requests
from psycopg2 import sql


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(message)s",
)

# Database connection parameters using environment variables
conn_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
}

# API headers using environment variable for API key
headers = {
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
    "x-rapidapi-host": "tank01-fantasy-stats.p.rapidapi.com",
}


# Helper function for DB connection
def get_db_connection():
    logging.info("connecting to db")
    conn = psycopg2.connect(**conn_params)
    logging.info("connected to db")
    return conn


# Block 1: Fetch and update player game stats
def fetch_player_ids():
    logging.info("fetching player ids")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT player_id FROM nba_players")
            rows = cur.fetchall()
    logging.info("fetched player ids")
    return rows


def fetch_player_game_stats(player_id, season_year):
    url = "https://tank01-fantasy-stats.p.rapidapi.com/getNBAGamesForPlayer"
    querystring = {"playerID": player_id, "statsToGet": season_year}
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json()
        if data["statusCode"] == 200 and data["body"]:
            return data["body"]
    logging.warning(f"error fetching: {response.text}")
    return None


def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def update_player_game_stats(stats_dict, player_id):
    if stats_dict:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for game_id, stats in stats_dict.items():
                    if not isinstance(stats, dict):
                        logging.info(
                            f"Stats for game {game_id} is not a dictionary. Skipping."
                        )
                        continue

                    team_abv = stats.get("teamAbv", "")
                    team_id = stats.get("teamID", None)

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
                            else:
                                opponent = ""
                                home_away = ""
                        except ValueError as e:
                            logging.warning(
                                f"Failed to parse gameID '{game_id}' for player ID {player_id}: {e}"
                            )
                            game_date = None
                            opponent = ""
                            home_away = ""
                    else:
                        game_date = None
                        opponent = ""
                        home_away = ""

                    insert_query = """
                    INSERT INTO nba_player_game_stats
                    (player_id, game_id, team_id, minutes_played, points, rebounds, assists, steals, blocks, turnovers,
                    offensive_rebounds, defensive_rebounds, free_throw_percentage, plus_minus, technical_fouls,
                    field_goal_attempts, three_point_fg_percentage, field_goals_made, field_goal_percentage,
                    three_point_fg_made, free_throw_attempts, three_point_fg_attempts, personal_fouls,
                    free_throws_made, fantasy_points, home_away, opponent, game_date, team_abv)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

                    values = (
                        player_id,
                        game_id,
                        team_id,
                        safe_float(stats.get("mins", 0)),
                        safe_int(stats.get("pts", 0)),
                        safe_int(stats.get("reb", 0)),
                        safe_int(stats.get("ast", 0)),
                        safe_int(stats.get("stl", 0)),
                        safe_int(stats.get("blk", 0)),
                        safe_int(stats.get("TOV", 0)),
                        safe_int(stats.get("OffReb", 0)),
                        safe_int(stats.get("DefReb", 0)),
                        safe_float(stats.get("ftp", 0.0)),
                        safe_float(stats.get("plusMinus", 0.0)),
                        safe_int(stats.get("tech", 0)),
                        safe_int(stats.get("fga", 0)),
                        safe_float(stats.get("tptfgp", 0.0)),
                        safe_int(stats.get("fgm", 0)),
                        safe_float(stats.get("fgp", 0.0)),
                        safe_int(stats.get("tptfgm", 0)),
                        safe_int(stats.get("fta", 0)),
                        safe_int(stats.get("tptfga", 0)),
                        safe_int(stats.get("PF", 0)),
                        safe_int(stats.get("ftm", 0)),
                        safe_float(stats.get("fantasyPoints", 0.0)),
                        home_away,
                        opponent,
                        game_date,
                        team_abv,
                    )

                    cur.execute(insert_query, values)
                conn.commit()
    else:
        logging.info(
            f"No stats available for player ID {player_id}. Skipping stats update."
        )


# Block 2: Fetch and update player injuries
def fetch_injury_list():
    url = "https://tank01-fantasy-stats.p.rapidapi.com/getNBAInjuryList"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data["statusCode"] == 200 and data["body"]:
            return data["body"]
    return None


def is_injury_current(injury):
    current_date = date.today().strftime("%Y%m%d")
    if "injReturnDate" in injury and injury["injReturnDate"]:
        return injury["injReturnDate"] >= current_date
    return True


def update_player_injuries(injury_list):
    if injury_list:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                player_injuries = {}
                for injury in injury_list:
                    player_id = injury["playerID"]
                    inj_date = injury["injDate"]

                    if is_injury_current(injury):
                        if (
                            player_id not in player_injuries
                            or inj_date > player_injuries[player_id]["injDate"]
                        ):
                            player_injuries[player_id] = injury

                for player_id, injury in player_injuries.items():
                    cur.execute(
                        """
                        UPDATE nba_players
                        SET injury = %s::jsonb
                        WHERE player_id = %s
                    """,
                        (json.dumps([injury]), player_id),
                    )

                cur.execute(
                    """
                    UPDATE nba_players
                    SET injury = NULL
                    WHERE player_id NOT IN %s
                """,
                    (tuple(player_injuries.keys()) or (None,),),
                )

                conn.commit()
    else:
        print("No injury data available")


# Block 3: Fetch and update player information and season stats
def fetch_player_first_names_with_full_names():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT SPLIT_PART(name, ' ', 1) AS first_name, name
                FROM nba_players
                ORDER BY first_name
            """)
            return cur.fetchall()


def group_full_names_by_first_name(first_names_with_full_names):
    grouped_names = defaultdict(list)
    for first_name, full_name in first_names_with_full_names:
        grouped_names[first_name].append(full_name)
    return grouped_names


def fetch_player_info(first_name):
    url = "https://tank01-fantasy-stats.p.rapidapi.com/getNBAPlayerInfo"
    querystring = {"playerName": first_name, "statsToGet": "averages"}
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json()
        if data["statusCode"] == 200 and data["body"]:
            return data["body"]
    return None


def update_player_info(player_data):
    if "nbaComHeadshot" in player_data and player_data["nbaComHeadshot"]:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE nba_players
                    SET player_pic = %s
                    WHERE name = %s
                """,
                    (player_data["nbaComHeadshot"], player_data["longName"]),
                )
            conn.commit()


def update_player_season_stats(player_data):
    stats = player_data.get("stats")
    if stats:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM nba_seasons WHERE season_year = '2025'")
                season_id_result = cur.fetchone()
                season_id = season_id_result[0] if season_id_result else 2

                update_query = sql.SQL("""
                    INSERT INTO nba_player_season_stats
                    (player_id, season_id, games_played, points_per_game, rebounds_per_game,
                    assists_per_game, steals_per_game, blocks_per_game, turnovers_per_game,
                    field_goal_percentage, three_point_percentage, free_throw_percentage,
                    minutes_per_game, offensive_rebounds_per_game, defensive_rebounds_per_game,
                    field_goals_made_per_game, field_goals_attempted_per_game,
                    three_pointers_made_per_game, three_pointers_attempted_per_game,
                    free_throws_made_per_game, free_throws_attempted_per_game)
                    VALUES (
                        (SELECT id FROM nba_players WHERE name = %s),
                        %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (player_id, season_id) DO UPDATE
                    SET games_played = EXCLUDED.games_played,
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
                """)
                cur.execute(
                    update_query,
                    (
                        player_data["longName"],
                        season_id,
                        stats.get("gamesPlayed", 0),
                        stats.get("pts", 0.0),
                        stats.get("reb", 0.0),
                        stats.get("ast", 0.0),
                        stats.get("stl", 0.0),
                        stats.get("blk", 0.0),
                        stats.get("TOV", 0.0),
                        stats.get("fgp", 0.0),
                        stats.get("tptfgp", 0.0),
                        stats.get("ftp", 0.0),
                        stats.get("mins", 0.0),
                        stats.get("OffReb", 0.0),
                        stats.get("DefReb", 0.0),
                        stats.get("fgm", 0.0),
                        stats.get("fga", 0.0),
                        stats.get("tptfgm", 0.0),
                        stats.get("tptfga", 0.0),
                        stats.get("ftm", 0.0),
                        stats.get("fta", 0.0),
                    ),
                )
            conn.commit()
    else:
        print(
            f"No stats available for {player_data['longName']}. Skipping stats update."
        )


# Block 4: Fetch and update team stats
def fetch_team_names():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM nba_teams;")
            team_names = [name[0] for name in cur.fetchall()]
    return team_names


def fetch_team_data():
    url = "https://tank01-fantasy-stats.p.rapidapi.com/getNBATeams?schedules=false&rosters=false&topPerformers=true&teamStats=true&statsToGet=averages"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        teams = data.get("body", [])
        return teams
    return None


def update_team_stats(team_name, team_data):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            team_ppg = team_data.get("ppg", None)
            team_oppg = team_data.get("oppg", None)
            team_wins = team_data.get("wins", None)
            team_losses = team_data.get("loss", None)
            team_bpg = (
                team_data.get("defensiveStats", {}).get("blk", {}).get("Total", None)
            )
            team_spg = (
                team_data.get("defensiveStats", {}).get("stl", {}).get("Total", None)
            )
            team_apg = (
                team_data.get("offensiveStats", {}).get("ast", {}).get("Total", None)
            )
            team_fga = (
                team_data.get("offensiveStats", {}).get("fga", {}).get("Total", None)
            )
            team_fgm = (
                team_data.get("offensiveStats", {}).get("fgm", {}).get("Total", None)
            )
            team_fta = (
                team_data.get("offensiveStats", {}).get("fta", {}).get("Total", None)
            )
            team_tov = (
                team_data.get("defensiveStats", {}).get("TOV", {}).get("Total", None)
            )

            cur.execute(
                """
                UPDATE nba_teams
                SET ppg = %s, oppg = %s, wins = %s, loss = %s, team_bpg = %s, team_spg = %s, team_apg = %s,
                    team_fga = %s, team_fgm = %s, team_fta = %s, team_tov = %s
                WHERE LOWER(name) = LOWER(%s);
            """,
                (
                    team_ppg,
                    team_oppg,
                    team_wins,
                    team_losses,
                    team_bpg,
                    team_spg,
                    team_apg,
                    team_fga,
                    team_fgm,
                    team_fta,
                    team_tov,
                    team_name,
                ),
            )
            conn.commit()


# Main function to run each block in sequence
def main():
    try:
        # Block 1: Fetch and update player game stats
        logging.info("[1] fetching player stats")
        player_ids = fetch_player_ids()
        logging.info(f"[1] got {len(player_ids)} player stats")
        season_year = 2024
        for (player_id,) in player_ids:
            logging.info(f"[1]   fetching {player_id=} {season_year=}")
            stats_dict = fetch_player_game_stats(player_id, season_year)
            logging.info("[1]   updating db")
            update_player_game_stats(stats_dict, player_id)
            time.sleep(0.05)
    except Exception as e:
        logging.info(f"[1] error: \n{format_exception(e)}")

    try:
        # Block 2: Fetch and update player injuries
        logging.info("[2] fetching player injuries")
        injury_list = fetch_injury_list()
        if injury_list:
            logging.info(f"[2] updating {len(injury_list)} player injuries in db")
            update_player_injuries(injury_list)
        else:
            logging.info("[2] no injury data")
    except Exception as e:
        logging.info(f"[2] error:\n{format_exception(e)}")

    try:
        # Block 3: Fetch and update player info and season stats
        logging.info("[3] fetching player season stats")
        first_names_with_full_names = fetch_player_first_names_with_full_names()
        grouped_names = group_full_names_by_first_name(first_names_with_full_names)
        logging.info(f"[3] got {len(grouped_names)} names")
        for first_name, full_names in grouped_names.items():
            logging.info(f"[3]   info for players with first name: {first_name}")
            players_data = fetch_player_info(first_name)
            if players_data:
                logging.info(f"[3] updating {len(players_data)} players")
                for player_data in players_data:
                    api_full_name = player_data["longName"].strip()
                    if api_full_name.lower() in [name.lower() for name in full_names]:
                        update_player_info(player_data)
                        update_player_season_stats(player_data)
                        logging.info(f"[3]     Processed {api_full_name}")
                    else:
                        logging.info(
                            f"[3]   Player {api_full_name} not found in database for first name {first_name}"
                        )
            else:
                logging.warning(
                    f"[3] Failed to fetch info for players with first name: {first_name}"
                )
            time.sleep(0.05)
    except Exception as e:
        logging.error(f"[3] error:\n{format_exception(e)}")

    try:
        # Block 4: Fetch and update team stats
        logging.info("[4] fetching team stats")
        team_names = fetch_team_names()
        teams_data = fetch_team_data()
        if teams_data:
            logging.info(f"[4] got {len(teams_data)} teams")
            for team_name in team_names:
                team_data = next(
                    (
                        team
                        for team in teams_data
                        if team["teamName"].lower() == team_name.lower()
                    ),
                    None,
                )
                if team_data:
                    update_team_stats(team_name, team_data)
                else:
                    print(f"Skipping update for {team_name} (not found in API)")
        else:
            logging.warning("[4] no team data available")
    except Exception as e:
        logging.error(f"[4] error:\n{format_exception(e)}")


if __name__ == "__main__":
    main()
