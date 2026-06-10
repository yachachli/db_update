"""Scrape FotMob Player traits for WC 2026 — one row per team.

Usage:
    py -3 scripts/fotmob_traits/scrape_wc2026_traits.py

Reads data/wc2026_top3_players.csv, writes data/wc2026_player_traits.csv.
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from extract_traits import extract_traits, traits_to_row_fields  # noqa: E402
from fotmob_client import FotmobClient  # noqa: E402
from apply_manual_traits import apply_manual_traits  # noqa: E402
from resolve_player import ResolvedPlayer, load_overrides, resolve_player_id  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RANK_FALLBACK = True
INPUT_CSV = ROOT / "data" / "wc2026_top3_players.csv"
OUTPUT_CSV = ROOT / "data" / "fotmob_player_trait_ratings.csv"
OVERRIDES_PATH = ROOT / "data" / "fotmob_id_overrides.json"
BACKUPS_PATH = ROOT / "data" / "fotmob_traits_backups.json"
CACHE_DIR = ROOT / "data" / "fotmob_cache"

CSV_COLUMNS = [
    "team",
    "group",
    "player_name",
    "player_rank_used",
    "fotmob_id",
    "fotmob_url",
    "compared_to",
    "has_traits",
    *[
        col
        for i in range(1, 7)
        for col in (f"trait{i}_name", f"trait{i}_pct")
    ],
    "traits_json",
    "scraped_at",
    "data_source",
]


def load_team_players(path: Path) -> dict[str, dict[str, object]]:
    by_team: dict[str, dict[str, object]] = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]
        for row in reader:
            team = row["team"].strip()
            if team not in by_team:
                by_team[team] = {"group": row["group"].strip(), "ranks": {}}
            by_team[team]["ranks"][int(row["rank"])] = row["player"].strip()
    return by_team


def load_backups(path: Path) -> dict[str, dict[str, object]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def pick_player_for_team(
    client: FotmobClient,
    team: str,
    group: str,
    ranks: dict[int, str],
    overrides: dict[str, int],
    backups: dict[str, dict[str, object]],
    scraped_at: str,
) -> dict[str, object]:
    rank_order = [1, 2, 3] if RANK_FALLBACK else [1]
    chosen_rank = rank_order[0]
    chosen_resolution = None
    chosen_pdata = None
    chosen_traits = None

    for rank in rank_order:
        player_name = ranks[rank]
        resolution = resolve_player_id(client, team, player_name, overrides)
        if resolution.fotmob_id is None:
            if chosen_resolution is None:
                chosen_rank = rank
                chosen_resolution = resolution
            continue
        pdata = client.player_data(resolution.fotmob_id)
        traits = extract_traits(pdata) if pdata else None
        chosen_rank = rank
        chosen_resolution = resolution
        chosen_pdata = pdata
        chosen_traits = traits
        if traits:
            break

    # Optional team backup when CSV ranks 1–3 have no traits chart.
    if chosen_traits is None and team in backups:
        backup = backups[team]
        pid = int(backup["fotmob_id"])
        pdata = client.player_data(pid)
        traits = extract_traits(pdata) if pdata else None
        if traits:
            chosen_rank = 3
            chosen_resolution = ResolvedPlayer(
                fotmob_id=pid,
                display_name=pdata.get("name") if pdata else backup.get("player_name"),
                confidence="backup",
                search_term=None,
                candidate_count=0,
                note=str(backup.get("note") or "team backup player"),
            )
            chosen_pdata = pdata
            chosen_traits = traits

    assert chosen_resolution is not None
    trait_fields = traits_to_row_fields(chosen_traits)
    fotmob_id = chosen_resolution.fotmob_id
    display_name = (
        chosen_pdata.get("name") if chosen_pdata else chosen_resolution.display_name
    )

    row: dict[str, object] = {
        "team": team,
        "group": group,
        "player_name": display_name or ranks[chosen_rank],
        "player_rank_used": chosen_rank,
        "fotmob_id": fotmob_id,
        "fotmob_url": (
            f"https://www.fotmob.com/players/{fotmob_id}" if fotmob_id else None
        ),
        "scraped_at": scraped_at,
        "data_source": "fotmob_playerData",
        **trait_fields,
    }
    row["_confidence"] = chosen_resolution.confidence
    row["_note"] = chosen_resolution.note
    return row


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def print_summary(rows: list[dict[str, object]], network_calls: int) -> None:
    resolved = sum(1 for r in rows if r.get("fotmob_id"))
    with_traits = sum(1 for r in rows if r.get("has_traits"))
    no_traits = [r for r in rows if r.get("fotmob_id") and not r.get("has_traits")]
    unresolved = [r for r in rows if not r.get("fotmob_id")]
    manual_backup = [r for r in rows if not r.get("has_traits")]

    print("\n" + "=" * 72)
    print("SCRAPE SUMMARY")
    print("=" * 72)
    print(f"Teams:           {len(rows)}")
    print(f"Resolved IDs:    {resolved}/{len(rows)}")
    print(f"With traits:     {with_traits}/{len(rows)}")
    print(f"Network calls:   {network_calls}")
    print()
    print(f"{'Team':<28} {'Rank':<5} {'Traits':<7} {'Player':<24} ID")
    print("-" * 72)
    for r in sorted(rows, key=lambda x: (x["group"], x["team"])):
        flag = "yes" if r.get("has_traits") else "NO"
        print(
            f"{r['team']:<28} {r['player_rank_used']:<5} {flag:<7} "
            f"{str(r['player_name'])[:24]:<24} {r.get('fotmob_id') or '-'}"
        )

    if unresolved:
        print("\nUnresolved (need override):")
        for r in unresolved:
            print(f"  - {r['team']}")

    if manual_backup:
        print("\nTeams needing manual backup player (no traits on ranks 1-3):")
        for r in manual_backup:
            print(f"  - {r['team']} (used rank {r['player_rank_used']}, id={r.get('fotmob_id')})")

    if no_traits and not unresolved:
        print("\nResolved but no traits chart:")
        for r in no_traits:
            print(f"  - {r['team']} / {r['player_name']} (id={r.get('fotmob_id')})")


def main() -> int:
    if not INPUT_CSV.exists():
        logger.error("Missing input CSV: %s", INPUT_CSV)
        return 1

    scraped_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    teams = load_team_players(INPUT_CSV)
    overrides = load_overrides(OVERRIDES_PATH)
    backups = load_backups(BACKUPS_PATH)
    client = FotmobClient(CACHE_DIR)

    rows: list[dict[str, object]] = []
    for team in sorted(teams.keys()):
        info = teams[team]
        try:
            row = pick_player_for_team(
                client,
                team,
                str(info["group"]),
                info["ranks"],  # type: ignore[arg-type]
                overrides,
                backups,
                scraped_at,
            )
            rows.append(row)
            status = "traits" if row.get("has_traits") else "no-traits"
            logger.info(
                "%s: %s rank=%s id=%s",
                team,
                status,
                row["player_rank_used"],
                row.get("fotmob_id"),
            )
        except Exception as exc:
            logger.exception("FAILED %s: %s", team, exc)
            rows.append(
                {
                    "team": team,
                    "group": info["group"],
                    "player_name": info["ranks"].get(1),
                    "player_rank_used": 1,
                    "fotmob_id": None,
                    "fotmob_url": None,
                    "has_traits": False,
                    "scraped_at": scraped_at,
                    "data_source": "fotmob_playerData",
                    **traits_to_row_fields(None),
                }
            )

    rows.sort(key=lambda r: (r["group"], r["team"]))
    rows = apply_manual_traits(rows)
    rows.sort(key=lambda r: (r["group"], r["team"]))
    write_csv(OUTPUT_CSV, rows)
    print_summary(rows, client.network_calls)
    print(f"\nWrote {OUTPUT_CSV}")
    return 0 if all(r.get("has_traits") for r in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
