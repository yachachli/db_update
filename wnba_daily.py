#!/usr/bin/env python3
"""
Nightly WNBA database refresh
─────────────────────────────
  • wnba_teams
  • wnba_players
  • wnba_player_game_stats     (PK: player_id , game_id)
  • wnba_player_season_stats   (PK: player_id , season_id)
"""

import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any, Dict

import psycopg2
import requests
from psycopg2.extras import execute_values

# ───────────────────────────── Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ───────────────────────────── DB + API
DB = dict(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
)

API_HOST = "tank01-wnba-live-in-game-real-time-statistics-wnba.p.rapidapi.com"
HEADERS = {
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
    "x-rapidapi-host": API_HOST,
}

def conn():
    return psycopg2.connect(**DB)

# ───────────────────────────── Helpers
f = lambda x: float(x) if x not in (None, "", "null") else 0.0
i = lambda x: int(float(x)) if x not in (None, "", "null") else 0

def every_player_id():
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT player_id FROM wnba_players;")
        return [row[0] for row in cur.fetchall()]

def team_id_map():
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT id, team_abv FROM wnba_teams;")
        return {abv: _id for _id, abv in cur.fetchall()}

# ───────────────────────────── Block 1 – teams (info + stats)
def refresh_teams():
    logging.info("[1] upserting teams + bulk stats")
    url = f"https://{API_HOST}/getWNBATeams"
    qs  = dict(rosters="true", teamStats="true", topPerformers="true",
               statsToGet="averages", schedules="false")
    r = requests.get(url, headers=HEADERS, params=qs).json()["body"]

    basic_cols = [(t["teamName"], t["teamCity"], t["teamAbv"], t.get("conference",""))
                  for t in r]

    with conn() as c, c.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO wnba_teams (name, team_city, team_abv, conference)
               VALUES %s
               ON CONFLICT (team_abv) DO UPDATE
                 SET name=EXCLUDED.name,
                     team_city=EXCLUDED.team_city,
                     conference=EXCLUDED.conference;""",
            basic_cols,
        )

        stats_sql = """
        UPDATE wnba_teams SET
            ppg=%(ppg)s, oppg=%(oppg)s, wins=%(wins)s, loss=%(loss)s,
            team_bpg=%(bpg)s, team_spg=%(spg)s, team_apg=%(apg)s,
            team_fga=%(fga)s, team_fgm=%(fgm)s, team_fta=%(fta)s,
            team_tov=%(tov)s
        WHERE team_abv=%(abv)s;"""
        for t in r:
            cur.execute(
                stats_sql,
                dict(
                    ppg=t.get("ppg"), oppg=t.get("oppg"),
                    wins=t.get("wins"), loss=t.get("loss"),
                    bpg=t["defensiveStats"]["blk"]["Total"],
                    spg=t["defensiveStats"]["stl"]["Total"],
                    apg=t["offensiveStats"]["ast"]["Total"],
                    fga=t["offensiveStats"]["fga"]["Total"],
                    fgm=t["offensiveStats"]["fgm"]["Total"],
                    fta=t["offensiveStats"]["fta"]["Total"],
                    tov=t["defensiveStats"]["TOV"]["Total"],
                    abv=t["teamAbv"],
                ),
            )
        c.commit()
    logging.info("[1] teams done")

# ───────────────────────────── Block 2 – players (roster + headshots)
def refresh_player_roster():
    logging.info("[2] syncing player list / headshots")
    r = requests.get(f"https://{API_HOST}/getWNBAPlayerList", headers=HEADERS).json()["body"]
    tmap = team_id_map()
    rows = [(p["longName"], p["pos"], tmap.get(p["team"]), int(p["playerID"])) for p in r]

    with conn() as c, c.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO wnba_players (name, position, team_id, player_id)
               VALUES %s
               ON CONFLICT (player_id) DO UPDATE
                 SET name=EXCLUDED.name,
                     position=EXCLUDED.position,
                     team_id=EXCLUDED.team_id;""",
            rows,
        )
        c.commit()

    # headshots by first name (saves API calls)
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT DISTINCT split_part(name,' ',1) FROM wnba_players;")
        first_names = [n[0] for n in cur.fetchall()]

    updates = []
    for first in first_names:
        info = requests.get(
            f"https://{API_HOST}/getWNBAPlayerInfo",
            headers=HEADERS,
            params=dict(playerName=first, statsToGet="averages"),
        ).json().get("body", [])
        for p in info:
            if not p.get("espnHeadshot"): continue
            updates.append((p["espnHeadshot"],
                            tmap.get(p["team"]), int(p["playerID"])))

    if updates:
        with conn() as c, c.cursor() as cur:
            execute_values(
                cur,
                """UPDATE wnba_players AS wp
                     SET player_pic=data.pic,
                         team_id=COALESCE(data.team_id, wp.team_id)
                    FROM (VALUES %s) AS data(pic,team_id,player_id)
                   WHERE wp.player_id=data.player_id;""",
                updates,
            )
            c.commit()
    logging.info(f"[2] players done ({len(rows)} roster rows, {len(updates)} pics)")

# ───────────────────────────── Block 3 – season averages
def refresh_player_season():
    logging.info("[3] season-avg stats → wnba_player_season_stats")
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT id FROM wnba_seasons WHERE season_year='2025';")
        season_id = cur.fetchone()[0]

    up_sql = """
    INSERT INTO wnba_player_season_stats (
        player_id, season_id, games_played, points_per_game, rebounds_per_game,
        assists_per_game, steals_per_game, blocks_per_game, turnovers_per_game,
        field_goal_percentage, three_point_percentage, free_throw_percentage,
        minutes_per_game, offensive_rebounds_per_game, defensive_rebounds_per_game,
        field_goals_made_per_game, field_goals_attempted_per_game,
        three_pointers_made_per_game, three_pointers_attempted_per_game,
        free_throws_made_per_game, free_throws_attempted_per_game)
    VALUES (
        (SELECT id FROM wnba_players WHERE player_id=%(pid)s), %(sid)s,
        %(gp)s, %(pts)s, %(reb)s, %(ast)s, %(stl)s, %(blk)s, %(tov)s,
        %(fgp)s, %(tpfgp)s, %(ftp)s, %(mins)s, %(oreb)s, %(dreb)s,
        %(fgm)s, %(fga)s, %(tpfgm)s, %(tp fga)s, %(ftm)s, %(fta)s)
    ON CONFLICT (player_id, season_id) DO UPDATE
      SET games_played=EXCLUDED.games_played,
          points_per_game=EXCLUDED.points_per_game,
          rebounds_per_game=EXCLUDED.rebounds_per_game,
          assists_per_game=EXCLUDED.assists_per_game,
          steals_per_game=EXCLUDED.steals_per_game,
          blocks_per_game=EXCLUDED.blocks_per_game,
          turnovers_per_game=EXCLUDED.turnovers_per_game,
          field_goal_percentage=EXCLUDED.field_goal_percentage,
          three_point_percentage=EXCLUDED.three_point_percentage,
          free_throw_percentage=EXCLUDED.free_throw_percentage,
          minutes_per_game=EXCLUDED.minutes_per_game,
          offensive_rebounds_per_game=EXCLUDED.offensive_rebounds_per_game,
          defensive_rebounds_per_game=EXCLUDED.defensive_rebounds_per_game,
          field_goals_made_per_game=EXCLUDED.field_goals_made_per_game,
          field_goals_attempted_per_game=EXCLUDED.field_goals_attempted_per_game,
          three_pointers_made_per_game=EXCLUDED.three_pointers_made_per_game,
          three_pointers_attempted_per_game=EXCLUDED.three_pointers_attempted_per_game,
          free_throws_made_per_game=EXCLUDED.free_throws_made_per_game,
          free_throws_attempted_per_game=EXCLUDED.free_throws_attempted_per_game;
    """

    firsts = {name.split()[0] for name, *_ in
              execute_values.__self__(conn(), "SELECT name FROM wnba_players")}

    for first in firsts:
        info = requests.get(
            f"https://{API_HOST}/getWNBAPlayerInfo",
            headers=HEADERS,
            params=dict(playerName=first, statsToGet="averages"),
        ).json().get("body", [])
        for p in info:
            s = p.get("stats") or {}
            if not s: 
                continue
            data = dict(
                pid=int(p["playerID"]), sid=season_id, gp=i(s["gamesPlayed"]),
                pts=f(s["pts"]), reb=f(s["reb"]), ast=f(s["ast"]), stl=f(s["stl"]),
                blk=f(s["blk"]), tov=f(s["TOV"]), fgp=f(s["fgp"]),
                tpfgp=f(s["tptfgp"]), ftp=f(s["ftp"]), mins=f(s["mins"]),
                oreb=f(s["OffReb"]), dreb=f(s["DefReb"]),
                fgm=f(s["fgm"]), fga=f(s["fga"]),
                tpfgm=f(s["tptfgm"]), tpfga=f(s["tptfga"]),
                ftm=f(s["ftm"]), fta=f(s["fta"]),
            )
            with conn() as c, c.cursor() as cur:
                cur.execute(up_sql, data)
                c.commit()
    logging.info("[3] season averages done")

# ───────────────────────────── Block 4 – game logs
def refresh_player_games():
    logging.info("[4] per-game stats (this part is slower)")
    pids  = every_player_id()
    tmap  = team_id_map()
    ins   = """
    INSERT INTO wnba_player_game_stats (
        player_id, game_id, team_id, minutes_played, points, rebounds, assists,
        steals, blocks, turnovers, offensive_rebounds, defensive_rebounds,
        free_throw_percentage, plus_minus, technical_fouls, field_goal_attempts,
        three_point_fg_percentage, field_goals_made, field_goal_percentage,
        three_point_fg_made, free_throw_attempts, three_point_fg_attempts,
        personal_fouls, free_throws_made, fantasy_points, home_away, opponent,
        game_date, team_abv)
    VALUES (%(player_id)s,%(game_id)s,%(team_id)s,%(mins)s,%(pts)s,%(reb)s,
        %(ast)s,%(stl)s,%(blk)s,%(tov)s,%(oreb)s,%(dreb)s,%(ftp)s,%(pm)s,
        %(tech)s,%(fga)s,%(tpfgp)s,%(fgm)s,%(fgp)s,%(tpfgm)s,%(fta)s,
        %(tp fga)s,%(pf)s,%(ftm)s,%(fp)s,%(ha)s,%(opp)s,%(gdate)s,%(abv)s)
    ON CONFLICT (player_id, game_id) DO UPDATE
        SET minutes_played=EXCLUDED.minutes_played,
            points=EXCLUDED.points, rebounds=EXCLUDED.rebounds,
            assists=EXCLUDED.assists, steals=EXCLUDED.steals,
            blocks=EXCLUDED.blocks, turnovers=EXCLUDED.turnovers,
            offensive_rebounds=EXCLUDED.offensive_rebounds,
            defensive_rebounds=EXCLUDED.defensive_rebounds,
            free_throw_percentage=EXCLUDED.free_throw_percentage,
            plus_minus=EXCLUDED.plus_minus, technical_fouls=EXCLUDED.technical_fouls,
            field_goal_attempts=EXCLUDED.field_goal_attempts,
            three_point_fg_percentage=EXCLUDED.three_point_fg_percentage,
            field_goals_made=EXCLUDED.field_goals_made,
            field_goal_percentage=EXCLUDED.field_goal_percentage,
            three_point_fg_made=EXCLUDED.three_point_fg_made,
            free_throw_attempts=EXCLUDED.free_throw_attempts,
            three_point_fg_attempts=EXCLUDED.three_point_fg_attempts,
            personal_fouls=EXCLUDED.personal_fouls,
            free_throws_made=EXCLUDED.free_throws_made,
            fantasy_points=EXCLUDED.fantasy_points,
            home_away=EXCLUDED.home_away, opponent=EXCLUDED.opponent,
            game_date=EXCLUDED.game_date, team_abv=EXCLUDED.team_abv;
    """

    for pid in pids:
        gl = requests.get(
            f"https://{API_HOST}/getWNBAGamesForPlayer",
            headers=HEADERS, params=dict(playerID=pid, fantasyPoints="true")
        ).json().get("body", {})
        rows = []
        for gid, g in gl.items():
            try:
                ds, teams = gid.split("_")
                gdate = datetime.strptime(ds, "%Y%m%d").date()
                away, home = teams.split("@")
                abv  = g["teamAbv"]
                ha, opp = ("Home", away) if abv == home else ("Away", home)
            except Exception:
                continue
            tid = tmap.get(abv)
            if not tid:
                continue
            rows.append(dict(
                player_id=pid, game_id=gid, team_id=tid,
                mins=f(g["mins"]), pts=i(g["pts"]), reb=i(g["reb"]),
                ast=i(g["ast"]), stl=i(g["stl"]), blk=i(g["blk"]),
                tov=i(g["TOV"]), oreb=i(g["OffReb"]), dreb=i(g["DefReb"]),
                ftp=f(g["ftp"]), pm=f(g["plusMinus"]), tech=i(g["tech"]),
                fga=i(g["fga"]), tpfgp=f(g["tptfgp"]),
                fgm=i(g["fgm"]), fgp=f(g["fgp"]), tpfgm=i(g["tptfgm"]),
                fta=i(g["fta"]), tpfga=i(g["tptfga"]),
                pf=i(g["PF"]), ftm=i(g["ftm"]), fp=f(g["fantasyPoints"]),
                ha=ha, opp=opp, gdate=gdate, abv=abv,
            ))
        if not rows: continue
        with conn() as c, c.cursor() as cur:
            execute_values(cur, ins, rows)
            c.commit()
        time.sleep(0.05)
    logging.info("[4] game logs done")

# ───────────────────────────── orchestrator
def main():
    try:
        refresh_teams()
        refresh_player_roster()
        refresh_player_season()
        refresh_player_games()
        logging.info("🏁 WNBA refresh complete")
    except Exception as e:
        logging.exception("WNBA nightly job failed: %s", e)
        raise

if __name__ == "__main__":
    main()
