"""Extract Player traits block from FotMob playerData."""

from __future__ import annotations

import json
import re
from typing import Any


def parse_compared_to(title: str | None) -> str | None:
    if not title:
        return None
    m = re.search(r"compared to other (.+)$", title, re.I)
    if m:
        return m.group(1).strip().lower()
    return title.strip().lower()


def extract_traits(player_data: dict[str, Any]) -> dict[str, Any] | None:
    traits = player_data.get("traits")
    if not isinstance(traits, dict):
        return None
    items = traits.get("items")
    if not isinstance(items, list) or not items:
        return None
    return traits


def traits_to_row_fields(traits: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "has_traits": False,
        "compared_to": None,
        "traits_json": None,
    }
    for i in range(1, 7):
        out[f"trait{i}_name"] = None
        out[f"trait{i}_pct"] = None

    if not traits:
        return out

    out["has_traits"] = True
    out["compared_to"] = parse_compared_to(traits.get("title"))
    out["traits_json"] = json.dumps(traits, ensure_ascii=False, separators=(",", ":"))

    for i, item in enumerate(traits.get("items", [])[:6], start=1):
        out[f"trait{i}_name"] = item.get("title")
        raw = item.get("value")
        if raw is not None:
            out[f"trait{i}_pct"] = int(round(float(raw) * 100))
    return out
