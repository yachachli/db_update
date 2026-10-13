"""Player and team directory with resolved image URLs.

ESPN publishes college headshots and NCAA logos at deterministic paths keyed by
the ESPN id, and CFBD ids *are* ESPN ids, so none of this scrapes a page: the
URL for any player is known the moment the roster is. What is not known is
whether ESPN actually holds a portrait for him, and a URL that 404s is worse
than no URL at all, because the app renders a broken frame instead of its
placeholder. So each path is probed once and the answer stored.

Rosters and team metadata already sit in ``identity_sources``; building the
directory spends no API budget. Only the image probes touch the network, and
they are bounded per run so a weekly job drains the backlog politely rather
than firing sixteen thousand requests at a CDN in one burst.
"""
import json
from concurrent.futures import ThreadPoolExecutor

import requests

from cfb.data import Client, connect

ESPN_HEADSHOT = ("https://a.espncdn.com/combiner/i?img=/i/headshots/"
                 "college-football/players/full/{player_id}.png")
ESPN_TEAM_LOGO = "https://a.espncdn.com/i/teamlogos/ncaa/500/{team_id}.png"
#: The app only ever shows a portrait for someone who can hold a yardage prop.
SKILL_POSITIONS = ("QB", "RB", "WR", "TE", "FB")
PROBE_WORKERS = 8
PROBE_TIMEOUT = (5, 10)


def _payloads(conn, endpoint):
    rows = conn.execute(
        "SELECT payload FROM cfb_model_v1.identity_sources WHERE endpoint=%s",
        (endpoint,)).fetchall()
    for (payload,) in rows:
        yield payload if isinstance(payload, (list, dict)) else json.loads(payload)


def build_teams():
    """Team rows and logo URLs from the stored /teams/fbs payload."""
    with connect() as conn:
        teams = {}
        for payload in _payloads(conn, "/teams/fbs"):
            for team in payload:
                teams[int(team["id"])] = team
        for team_id, team in teams.items():
            conn.execute(
                """INSERT INTO cfb_model_v1.team_directory
                   (team_id,school,abbreviation,conference,color,alternate_color,logo_url,updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,now())
                   ON CONFLICT(team_id) DO UPDATE SET school=excluded.school,
                     abbreviation=excluded.abbreviation,conference=excluded.conference,
                     color=excluded.color,alternate_color=excluded.alternate_color,
                     logo_url=excluded.logo_url,updated_at=now()""",
                (team_id, team["school"], team.get("abbreviation"), team.get("conference"),
                 team.get("color"), team.get("alternateColor"),
                 ESPN_TEAM_LOGO.format(team_id=team_id)))
    print(json.dumps({"teams": len(teams)}), flush=True)
    return len(teams)


def fetch_missing_rosters(season, limit=150):
    """Pull rosters for schools the identity audit has never sampled.

    That audit only fetches rosters for teams it happens to see in a prop
    capture, so it had covered 45 of 138 schools. The directory needs all of
    them, and a roster is one call per team.
    """
    with connect() as conn:
        schools = {r[0] for r in conn.execute(
            "SELECT school FROM cfb_model_v1.team_directory").fetchall()}
        have = set()
        for payload in _payloads(conn, "/roster"):
            for athlete in payload:
                if athlete.get("team"):
                    have.add(athlete["team"])
    missing = sorted(schools - have)[:limit]
    if not missing:
        print(json.dumps({"rosters_fetched": 0, "missing": 0}), flush=True)
        return 0
    client = Client(max_calls=len(missing) + 5)
    fetched = 0
    for school in missing:
        try:
            payload = client.get("/roster", {"team": school, "year": season})
        except RuntimeError:
            break  # budget or provider failure: keep what we have
        if not payload:
            continue
        with connect() as conn:
            conn.execute(
                """INSERT INTO cfb_model_v1.identity_sources(observed_at,season,endpoint,parameters,payload)
                   VALUES (now(),%s,%s,%s,%s)""",
                (season, "/roster", json.dumps({"team": school, "year": season}),
                 json.dumps(payload)))
        fetched += 1
    print(json.dumps({"rosters_fetched": fetched,
                      "still_missing": len(sorted(schools - have)) - fetched}), flush=True)
    return fetched


def build_players(skill_only=True):
    """Directory rows from the stored /roster payloads. Spends no API budget."""
    with connect() as conn:
        schools = {r[1]: r[0] for r in conn.execute(
            "SELECT team_id,school FROM cfb_model_v1.team_directory").fetchall()}
        players = {}
        for payload in _payloads(conn, "/roster"):
            for athlete in payload:
                position = athlete.get("position")
                if skill_only and position not in SKILL_POSITIONS:
                    continue
                pid = str(athlete["id"])
                first, last = athlete.get("firstName") or "", athlete.get("lastName") or ""
                players[pid] = (
                    pid, f"{first} {last}".strip() or pid, first, last, position,
                    schools.get(athlete.get("team")), athlete.get("team"),
                    athlete.get("jersey"))
        with conn.pipeline():
            for row in players.values():
                conn.execute(
                    """INSERT INTO cfb_model_v1.player_directory
                       (player_id,player_name,first_name,last_name,position,team_id,team,jersey,updated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())
                       ON CONFLICT(player_id) DO UPDATE SET player_name=excluded.player_name,
                         first_name=excluded.first_name,last_name=excluded.last_name,
                         position=excluded.position,team_id=excluded.team_id,
                         team=excluded.team,jersey=excluded.jersey,updated_at=now()""", row)
    print(json.dumps({"players": len(players), "skill_only": skill_only}), flush=True)
    return len(players)


def _probe(session, player_id):
    """True when ESPN holds a portrait. A missing player 404s rather than
    returning a silhouette, so the status code is the whole answer."""
    try:
        response = session.head(ESPN_HEADSHOT.format(player_id=player_id),
                                timeout=PROBE_TIMEOUT, allow_redirects=True)
        return response.status_code == 200
    except requests.RequestException:
        return None  # transient: leave unchecked rather than record a false absence


def verify_headshots(limit=500, recheck_missing=False):
    """Probe up to ``limit`` unresolved players, most relevant first.

    Ordering puts players who actually carry recent production ahead of a
    fourth-string tight end, so a bounded run resolves the portraits the app is
    most likely to need next.
    """
    if not 1 <= limit <= 5000:
        raise ValueError("limit must be 1..5000")
    condition = ("headshot_checked_at IS NULL OR headshot_url IS NULL"
                 if recheck_missing else "headshot_checked_at IS NULL")
    with connect() as conn:
        targets = conn.execute(
            f"""SELECT d.player_id FROM cfb_model_v1.player_directory d
                LEFT JOIN (SELECT player_id, sum(volume) v FROM cfb_model_v1.player_offense
                           GROUP BY player_id) u ON u.player_id = d.player_id
                WHERE {condition}
                ORDER BY coalesce(u.v,0) DESC, d.player_id
                LIMIT %s""", (limit,)).fetchall()  # noqa: S608 - fixed branches
    ids = [r[0] for r in targets]
    if not ids:
        print(json.dumps({"checked": 0, "remaining": 0}), flush=True)
        return {"checked": 0, "found": 0, "missing": 0, "remaining": 0}
    with requests.Session() as session:
        session.headers["User-Agent"] = "bestbet-cfb-directory/1.0"
        with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
            results = list(pool.map(lambda pid: (pid, _probe(session, pid)), ids))
    found = [(ESPN_HEADSHOT.format(player_id=p), p) for p, ok in results if ok is True]
    missing = [(None, p) for p, ok in results if ok is False]
    with connect() as conn:
        with conn.pipeline():
            for url, pid in found + missing:
                conn.execute(
                    """UPDATE cfb_model_v1.player_directory
                       SET headshot_url=%s, headshot_checked_at=now() WHERE player_id=%s""",
                    (url, pid))
        remaining = conn.execute(
            "SELECT count(*) FROM cfb_model_v1.player_directory WHERE headshot_checked_at IS NULL"
        ).fetchone()[0]
    report = {"checked": len(found) + len(missing), "found": len(found),
              "missing": len(missing), "transient_errors": sum(1 for _, ok in results if ok is None),
              "remaining": remaining}
    print(json.dumps(report), flush=True)
    return report


def run(limit=500, skill_only=True):
    build_teams()
    build_players(skill_only=skill_only)
    return verify_headshots(limit=limit)
