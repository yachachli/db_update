"""Name -> FotMob ID resolver with team-aware disambiguation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fotmob_client import FotmobClient
from team_country_codes import SEARCH_ALIASES, country_matches_team, player_country_code

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ResolvedPlayer:
    fotmob_id: int | None
    display_name: str | None
    confidence: str  # override | high | medium | low | unresolved
    search_term: str | None
    candidate_count: int
    note: str | None = None


def load_overrides(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): int(v) for k, v in raw.items()}


def _search_terms(player_name: str) -> list[str]:
    terms = [player_name]
    terms.extend(SEARCH_ALIASES.get(player_name, []))
    seen: set[str] = set()
    out: list[str] = []
    for t in terms:
        key = t.strip().lower()
        if key and key not in seen:
            seen.add(key)
            out.append(t)
    return out


def resolve_player_id(
    client: FotmobClient,
    team: str,
    player_name: str,
    overrides: dict[str, int],
) -> ResolvedPlayer:
    override_key = f"{team}::{player_name}"
    if override_key in overrides:
        pid = overrides[override_key]
        pdata = client.player_data(pid)
        return ResolvedPlayer(
            fotmob_id=pid,
            display_name=pdata.get("name") if pdata else None,
            confidence="override",
            search_term=None,
            candidate_count=0,
            note="manual override",
        )

    all_candidates: list[dict[str, Any]] = []
    used_term: str | None = None
    for term in _search_terms(player_name):
        hits = client.search_players(term)
        if hits:
            all_candidates = hits
            used_term = term
            break

    if not all_candidates:
        logger.warning("UNRESOLVED search: %s / %s", team, player_name)
        return ResolvedPlayer(
            fotmob_id=None,
            display_name=None,
            confidence="unresolved",
            search_term=used_term,
            candidate_count=0,
            note="no search hits",
        )

    scored: list[tuple[dict[str, Any], dict[str, Any] | None, str | None]] = []
    for cand in all_candidates[:8]:
        pdata = client.player_data(int(cand["id"]))
        cc = player_country_code(pdata) if pdata else None
        scored.append((cand, pdata, cc))

    country_matches = [
        (c, p, cc)
        for c, p, cc in scored
        if p and country_matches_team(team, cc)
    ]
    if country_matches:
        cand, pdata, _ = country_matches[0]
        conf = "high" if len(country_matches) == 1 else "medium"
        return ResolvedPlayer(
            fotmob_id=int(cand["id"]),
            display_name=pdata.get("name") if pdata else cand.get("name"),
            confidence=conf,
            search_term=used_term,
            candidate_count=len(all_candidates),
            note="country code match" if conf == "high" else "country match among duplicates",
        )

    # Fall back to top search score; flag if multiple distinct IDs.
    cand, pdata, cc = scored[0]
    conf = "medium" if len(all_candidates) == 1 else "low"
    if pdata and cc:
        note = f"top search hit; country={cc}, expected {team}"
    else:
        note = "top search hit; country unknown"
    if conf == "low":
        logger.warning(
            "LOW_CONFIDENCE %s / %s -> id=%s (%s)",
            team,
            player_name,
            cand["id"],
            note,
        )
    return ResolvedPlayer(
        fotmob_id=int(cand["id"]),
        display_name=pdata.get("name") if pdata else cand.get("name"),
        confidence=conf,
        search_term=used_term,
        candidate_count=len(all_candidates),
        note=note,
    )
