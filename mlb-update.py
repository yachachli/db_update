"""
Daily MLB database refresh

Tables touched
──────────────
  • mlb_teams
  • mlb_players
  • mlb_player_game_stats         (PK:  player_id , game_id)

All inserts are **idempotent**:
    teams / players use ON CONFLICT … DO UPDATE  
    game-stats use ON CONFLICT … DO NOTHING

Environment variables expected
──────────────────────────────
  DB_NAME         bestbetdb
  DB_USER         …
  DB_PASSWORD     …
  DB_HOST         …
  RAPIDAPI_KEY    …
"""

import json
import logging
import os
import time
import traceback
from datetime import datetime
from typing import Any
from os import environ

import psycopg2
import requests

# ────────────────────────────────────────────────────────────────────────────
#  Logging
# ────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ────────────────────────────────────────────────────────────────────────────
#  Config & helpers
# ────────────────────────────────────────────────────────────────────────────
API_HOST = "tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com"
HEADERS  = {
    "x-rapidapi-host": API_HOST,
    "x-rapidapi-key":  environ["RAPIDAPI_KEY"],
}

DB_CFG = dict(
    dbname   = os.environ["DB_NAME"],
    user     = os.environ["DB_USER"],
    password = os.environ["DB_PASSWORD"],
    host     = os.environ["DB_HOST"],
)

BATCH  = 50          # commit after N inserts
SEASON = 2025        # ← adjust yearly


# adding a comment for pushing sakes
# ────────────────────────────────────────────────────────────────────────────
#  Low-level helpers
# ────────────────────────────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(**DB_CFG)


def get_json(endpoint: str, params: dict[str, Any] | None = None) -> Any:
    url = f"https://{API_HOST}{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params or {})
    r.raise_for_status()
    data = r.json()
    if data.get("statusCode") != 200:
        raise RuntimeError(f"API error: {data}")
    return data["body"]


def safe_int(v):   # strings → int | None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
    

def clean_abv(raw: str | None) -> str | None: # for the abreviation skip I was getting
    """Normalize teamAbv; blank → None."""
    return (raw or "").strip().upper() or None

def load_valid_abvs(cur) -> set[str]:
    cur.execute("SELECT team_abv FROM mlb_teams")
    return {row[0] for row in cur.fetchall()}



# ────────────────────────────────────────────────────────────────────────────
#  Insert helpers
# ────────────────────────────────────────────────────────────────────────────
def insert_mlb_team(cur, team: dict):
    q = """
    INSERT INTO mlb_teams (
        team_abv, team_city, team_name, conference, division,
        rs, ra, wins, losses, run_diff
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (team_abv) DO UPDATE SET
        team_city = EXCLUDED.team_city,
        team_name = EXCLUDED.team_name,
        conference = EXCLUDED.conference,
        division   = EXCLUDED.division,
        rs         = EXCLUDED.rs,
        ra         = EXCLUDED.ra,
        wins       = EXCLUDED.wins,
        losses     = EXCLUDED.losses,
        run_diff   = EXCLUDED.run_diff
    """
    cur.execute(
        q,
        (
            team["teamAbv"],
            team["teamCity"],
            team["teamName"],
            team["conference"],
            team["division"],
            safe_int(team.get("RS")),
            safe_int(team.get("RA")),
            safe_int(team.get("wins")),
            safe_int(team.get("loss")),
            safe_int(team.get("DIFF")),
        ),
    )


def insert_mlb_player(cur, p: dict):
    injury = p.get("injury", {})
    q = """
    INSERT INTO mlb_players (
        player_id, long_name, team_abv, pos,
        height, weight, bat, throw, b_day,
        mlb_headshot, espn_headshot, espn_status,
        injury_description, injury_return
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (player_id) DO UPDATE SET
        long_name           = EXCLUDED.long_name,
        team_abv            = EXCLUDED.team_abv,
        pos                 = EXCLUDED.pos,
        height              = EXCLUDED.height,
        weight              = EXCLUDED.weight,
        bat                 = EXCLUDED.bat,
        throw               = EXCLUDED.throw,
        b_day               = EXCLUDED.b_day,
        mlb_headshot        = EXCLUDED.mlb_headshot,
        espn_headshot       = EXCLUDED.espn_headshot,
        espn_status         = EXCLUDED.espn_status,
        injury_description  = EXCLUDED.injury_description,
        injury_return       = EXCLUDED.injury_return
    """
    cur.execute(
        q,
        (
            p["playerID"],
            p["longName"],
            p["teamAbv"],
            # p["teamID"],
            p["pos"],
            p.get("height"),
            p.get("weight"),
            p.get("bat"),
            p.get("throw"),
            p.get("bDay"),
            p.get("mlbHeadshot"),
            p.get("espnHeadshot"),
            p.get("espnStatus"),
            injury.get("description"),
            injury.get("injReturnDate"),
        ),
    )


def insert_mlb_game_stats(cur, pid: str, g: dict):
    H, P, F, B = (
        g.get("Hitting", {}),
        g.get("Pitching", {}),
        g.get("Fielding", {}),
        g.get("BaseRunning", {}),
    )
    q = """
    INSERT INTO mlb_player_game_stats (
        player_id, game_id, team, started, starting_position, all_positions_played,
        bb, ab, h, hr, rbi, so, avg, tb, doubles, triples, r, ibb, sf, sac, hbp, gidp,
        p_bb, p_h, p_hr, p_er, p_so, win, loss, save, era, innings_pitched,
        fielding_e, passed_ball, of_assists, pickoffs,
        sb, cs, po
    )
    VALUES (
        %(player_id)s, %(game_id)s, %(team)s, %(started)s, %(starting_position)s, %(all_pos)s,
        %(bb)s, %(ab)s, %(h)s, %(hr)s, %(rbi)s, %(so)s, %(avg)s, %(tb)s, %(dbl)s, %(trp)s,
        %(r)s, %(ibb)s, %(sf)s, %(sac)s, %(hbp)s, %(gidp)s,
        %(p_bb)s, %(p_h)s, %(p_hr)s, %(p_er)s, %(p_so)s, %(win)s, %(loss)s, %(save)s,
        %(era)s, %(ip)s,
        %(fe)s, %(pb)s, %(ofa)s, %(pk)s,
        %(sb)s, %(cs)s, %(po)s
    )
    ON CONFLICT (player_id, game_id) DO NOTHING
    """
    cur.execute(
        q,
        dict(
            player_id=pid,
            game_id=g["gameID"],
            team=g["team"],
            started=g.get("started"),
            starting_position=g.get("startingPosition"),
            all_pos=g.get("allPositionsPlayed"),
            # hitting
            bb=H.get("BB"),
            ab=H.get("AB"),
            h=H.get("H"),
            hr=H.get("HR"),
            rbi=H.get("RBI"),
            so=H.get("SO"),
            avg=H.get("AVG"),
            tb=H.get("TB"),
            dbl=H.get("2B"),
            trp=H.get("3B"),
            r=H.get("R"),
            ibb=H.get("IBB"),
            sf=H.get("SF"),
            sac=H.get("SAC"),
            hbp=H.get("HBP"),
            gidp=H.get("GIDP"),
            # pitching
            p_bb=P.get("BB"),
            p_h=P.get("H"),
            p_hr=P.get("HR"),
            p_er=P.get("ER"),
            p_so=P.get("SO"),
            win=P.get("Win"),
            loss=P.get("Loss"),
            save=P.get("Save"),
            era=P.get("ERA"),
            ip=P.get("InningsPitched"),
            # fielding
            fe=F.get("E"),
            pb=F.get("Passed Ball"),
            ofa=F.get("Outfield assists"),
            pk=F.get("Pickoffs"),
            # baserunning
            sb=B.get("SB"),
            cs=B.get("CS"),
            po=B.get("PO"),
        ),
    )


# ────────────────────────────────────────────────────────────────────────────
#  Main
# ────────────────────────────────────────────────────────────────────────────
def main():
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        # ——— TEAMS ———
        logging.info("Fetching teams …")
        teams = get_json("/getMLBTeams")
        logging.info(f"Upserting {len(teams)} teams")
        for t in teams:
            insert_mlb_team(cur, t)
        conn.commit()

        valid_abvs = load_valid_abvs(cur)

        # ——— PLAYERS ———
        logging.info("Fetching players list …")
        players = get_json("/getMLBPlayerList")
        logging.info(f"Processing {len(players)} players")

        player_info_cache = []
        for i, p in enumerate(players, start=1):
            pid = p["playerID"]
            try:
                info = get_json(
                    "/getMLBPlayerInfo",
                    params={"playerID": pid, "getStats": "false", "statsSeason": SEASON},
                )
            except Exception as e:
                logging.warning(f"player {pid}: {e}")
                continue

            # --- filter out free agents / bad codes ---
            team_abv = clean_abv(info.get("teamAbv"))
            if not team_abv or team_abv not in valid_abvs:
                continue
            info["teamAbv"] = team_abv

            # --- insert player ---
            try:
                insert_mlb_player(cur, info)
                player_info_cache.append(info)
            except Exception as e:
                logging.warning("insert %s: %s", pid, e)

            if i % BATCH == 0:
                conn.commit()
                logging.info(f" … committed {i} players so far")
            time.sleep(0.4)   # be nice to RapidAPI rate-limit

        conn.commit()
        logging.info("Finished players")

        # ——— GAME LOGS ———
        logging.info("Fetching game logs …")
        g_batch = 0
        for i, p in enumerate(player_info_cache, start=1):
            pid = p["playerID"]
            try:
                games = get_json(
                    "/getMLBGamesForPlayer",
                    params={"playerID": pid, "season": SEASON},
                )
            except Exception as e:
                logging.warning(f"games for {pid}: {e}")
                continue

            for g in games.values():
                insert_mlb_game_stats(cur, pid, g)
                g_batch += 1
                if g_batch >= BATCH:
                    conn.commit()
                    logging.info(" … committed game batch")
                    g_batch = 0

            if i % 25 == 0:
                logging.info(f"  processed {i}/{len(player_info_cache)} players")
            time.sleep(0.4)

        if g_batch:
            conn.commit()

        cur.close()
        conn.close()
        logging.info("✅ MLB refresh complete")

    except Exception:
        logging.error("UNCAUGHT ERROR:\n" + traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
