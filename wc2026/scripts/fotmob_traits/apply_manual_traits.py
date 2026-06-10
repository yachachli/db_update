"""Merge data/fotmob_manual_traits.json into the traits CSV."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANUAL_PATH = ROOT / "data" / "fotmob_manual_traits.json"

CSV_COLUMNS = [
    "team", "group", "player_name", "player_rank_used", "fotmob_id", "fotmob_url",
    "compared_to", "has_traits",
    *[col for i in range(1, 7) for col in (f"trait{i}_name", f"trait{i}_pct")],
    "traits_json", "scraped_at", "data_source",
]


def manual_to_row(team: str, group: str, manual: dict, scraped_at: str) -> dict[str, object]:
    traits = manual["traits"]
    row: dict[str, object] = {
        "team": team,
        "group": group,
        "player_name": manual["player_name"],
        "player_rank_used": manual.get("player_rank_used", 1),
        "fotmob_id": manual["fotmob_id"],
        "fotmob_url": f"https://www.fotmob.com/players/{manual['fotmob_id']}",
        "compared_to": manual.get("compared_to"),
        "has_traits": True,
        "scraped_at": scraped_at,
        "data_source": manual.get("data_source", "manual_trait_estimate"),
    }
    for i, t in enumerate(traits[:6], start=1):
        row[f"trait{i}_name"] = t["title"]
        row[f"trait{i}_pct"] = int(t["pct"])
    for i in range(len(traits) + 1, 7):
        row[f"trait{i}_name"] = None
        row[f"trait{i}_pct"] = None
    row["traits_json"] = json.dumps(
        {
            "key": "manual_trait_estimate",
            "title": f"Stats compared to other {manual.get('compared_to', '')}",
            "items": [
                {
                    "key": t["key"],
                    "title": t["title"],
                    "value": round(int(t["pct"]) / 100, 2),
                    "basis": t.get("basis"),
                }
                for t in traits
            ],
            "notes": manual.get("notes"),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return row


def apply_manual_traits(
    rows: list[dict[str, object]],
    *,
    manual_path: Path = MANUAL_PATH,
) -> list[dict[str, object]]:
    if not manual_path.exists():
        return rows
    manual_all = json.loads(manual_path.read_text(encoding="utf-8"))
    scraped_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    by_team = {str(r["team"]): r for r in rows}
    for team, manual in manual_all.items():
        if team not in by_team:
            continue
        group = str(by_team[team]["group"])
        by_team[team] = manual_to_row(team, group, manual, scraped_at)
    return list(by_team.values())
