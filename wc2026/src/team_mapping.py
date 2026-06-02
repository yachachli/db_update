"""Bridge SportMonks team objects to FIFA ranking entries.

Every match-quality weight in the model depends on knowing an opponent's FIFA
points, so this small module is central: it turns a raw SportMonks team object
into the matching :class:`~src.fifa_rankings.FifaRankingEntry`.

The resolution cascade was verified empirically (see
``scripts/probe_sportmonks_team_fields*.py``). Across 10 diverse teams every
one matched on the primary key, ``country.fifa_name`` -> FIFA ``country_code``.
The fallbacks and override file exist purely to absorb future upstream drift
(e.g. Wales: ``iso3`` is ``WLS`` but FIFA uses ``WAL`` -- only ``fifa_name``
bridges the two, which is why it is primary and ``iso3`` is a last resort).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.fifa_rankings import (
    FifaRankingEntry,
    FifaRankingRelease,
    get_current_rankings,
)

__all__ = [
    "TeamFifaMapping",
    "UnresolvedTeam",
    "TeamFifaMapper",
    "TeamMappingError",
    "get_fifa_points_for_team",
]

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_OVERRIDES_PATH = _PROJECT_ROOT / "data" / "team_mapping_overrides.json"


class TeamMappingError(RuntimeError):
    """Raised when a team cannot be resolved and no default was supplied."""


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class TeamFifaMapping:
    """Result of resolving a SportMonks team to its FIFA ranking."""

    sportmonks_team_id: int
    sportmonks_name: str
    fifa_country_code: str
    fifa_name: str
    fifa_points: float
    fifa_rank: int
    resolution_method: str  # "fifa_name", "short_code", "iso3", "override"


@dataclass(frozen=True, slots=True)
class UnresolvedTeam:
    """A SportMonks team we could NOT match to FIFA.

    The caller decides whether to fail loudly, use a fallback value, or skip
    the team. ``attempted_keys`` records every identifier we tried.
    """

    sportmonks_team_id: int
    sportmonks_name: str
    attempted_keys: dict[str, str]


# ---------------------------------------------------------------------------
# Mapper
# ---------------------------------------------------------------------------
class TeamFifaMapper:
    """Resolves SportMonks teams to FIFA entries via the verified cascade."""

    def __init__(
        self,
        fifa_release: FifaRankingRelease,
        overrides_path: Path | None = None,
    ) -> None:
        """
        Args:
            fifa_release: the FifaRankingRelease to resolve against (typically
                the latest, from ``get_current_rankings()``).
            overrides_path: optional path to the overrides JSON file. Defaults
                to ``data/team_mapping_overrides.json``.
        """
        self.fifa_release = fifa_release
        self._overrides_path = overrides_path or _DEFAULT_OVERRIDES_PATH
        self.overrides = self._load_overrides(self._overrides_path)

    # -- public --------------------------------------------------------------
    def resolve(self, sportmonks_team: dict[str, Any]) -> TeamFifaMapping | UnresolvedTeam:
        """Resolve one SportMonks team object to a FIFA mapping.

        Cascade (short-circuits on first success):
          1. overrides keyed by ``sportmonks_team["id"]``
          2. ``country.fifa_name``  (primary)
          3. ``short_code``         (fallback 1)
          4. ``country.iso3``       (fallback 2)
          5. otherwise -> :class:`UnresolvedTeam`

        Logs each resolution; emits a WARNING whenever a fallback past the
        primary key is used, since that may signal upstream data drift.
        """
        team_id = sportmonks_team.get("id")
        name = str(sportmonks_team.get("name") or "")
        country = sportmonks_team.get("country") or {}
        fifa_name = _clean(country.get("fifa_name"))
        short_code = _clean(sportmonks_team.get("short_code"))
        iso3 = _clean(country.get("iso3"))

        override_code = self._override_code_for(team_id)

        attempted: dict[str, str] = {
            "override": override_code or "",
            "fifa_name": fifa_name or "",
            "short_code": short_code or "",
            "iso3": iso3 or "",
        }

        # 1. Override (intentional, authoritative).
        if override_code:
            entry = self.fifa_release.lookup_by_code(override_code)
            if entry is not None:
                logger.info(
                    "Resolved '%s' (id=%s) via OVERRIDE -> %s [%s]",
                    name, team_id, entry.name, entry.country_code,
                )
                return self._mapping(team_id, name, entry, "override")
            logger.warning(
                "Override for team id=%s points to FIFA code %r, which is not "
                "present in release %s; falling through to auto-cascade.",
                team_id, override_code, self.fifa_release.date_id,
            )

        # 2-4. Cascade.
        for method, key, is_fallback in (
            ("fifa_name", fifa_name, False),
            ("short_code", short_code, True),
            ("iso3", iso3, True),
        ):
            if not key:
                continue
            entry = self.fifa_release.lookup_by_code(key)
            if entry is None:
                continue
            if is_fallback:
                logger.warning(
                    "Resolved '%s' (id=%s) via FALLBACK '%s' (=%s) -> %s [%s]. "
                    "Primary key country.fifa_name=%r did not match; check for "
                    "upstream data drift.",
                    name, team_id, method, key, entry.name, entry.country_code,
                    fifa_name,
                )
            else:
                logger.info(
                    "Resolved '%s' (id=%s) via %s (=%s) -> %s [%s]",
                    name, team_id, method, key, entry.name, entry.country_code,
                )
            return self._mapping(team_id, name, entry, method)

        # 5. Nothing matched.
        logger.warning(
            "UNRESOLVED team '%s' (id=%s); attempted keys: %s",
            name, team_id, attempted,
        )
        return UnresolvedTeam(
            sportmonks_team_id=int(team_id) if team_id is not None else -1,
            sportmonks_name=name,
            attempted_keys=attempted,
        )

    def resolve_many(
        self, sportmonks_teams: list[dict[str, Any]]
    ) -> tuple[list[TeamFifaMapping], list[UnresolvedTeam]]:
        """Bulk-resolve. Returns ``(resolved, unresolved)`` and logs a summary."""
        resolved: list[TeamFifaMapping] = []
        unresolved: list[UnresolvedTeam] = []
        for team in sportmonks_teams:
            result = self.resolve(team)
            if isinstance(result, TeamFifaMapping):
                resolved.append(result)
            else:
                unresolved.append(result)

        total = len(sportmonks_teams)
        if unresolved:
            names = [f"{u.sportmonks_name} (id={u.sportmonks_team_id})"
                     for u in unresolved]
            logger.warning(
                "Resolved %d/%d teams. %d unresolved: %s",
                len(resolved), total, len(unresolved), names,
            )
        else:
            logger.info("Resolved %d/%d teams. 0 unresolved.", len(resolved), total)
        return resolved, unresolved

    # -- internals -----------------------------------------------------------
    def _override_code_for(self, team_id: Any) -> str | None:
        if team_id is None:
            return None
        entry = self.overrides.get(str(team_id))
        if isinstance(entry, dict):
            return _clean(entry.get("fifa_country_code"))
        return None

    @staticmethod
    def _mapping(
        team_id: Any, name: str, entry: FifaRankingEntry, method: str
    ) -> TeamFifaMapping:
        return TeamFifaMapping(
            sportmonks_team_id=int(team_id) if team_id is not None else -1,
            sportmonks_name=name,
            fifa_country_code=entry.country_code,
            fifa_name=entry.name,
            fifa_points=entry.points,
            fifa_rank=entry.rank,
            resolution_method=method,
        )

    @staticmethod
    def _load_overrides(path: Path) -> dict[str, Any]:
        if not path.exists():
            logger.info("No overrides file at %s; using empty overrides.", path)
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError) as exc:
            logger.warning("Could not read overrides file %s: %s", path, exc)
            return {}
        overrides = payload.get("overrides")
        return overrides if isinstance(overrides, dict) else {}


# ---------------------------------------------------------------------------
# Convenience module-level function
# ---------------------------------------------------------------------------
def get_fifa_points_for_team(
    sportmonks_team: dict[str, Any],
    fifa_release: FifaRankingRelease | None = None,
    default_on_miss: float | None = None,
) -> float:
    """Resolve a SportMonks team object straight to its FIFA points.

    The simplest interface for the rest of the model. If ``fifa_release`` is
    ``None``, the latest rankings are fetched via ``get_current_rankings()``.

    On an unresolvable team:
      - if ``default_on_miss`` is not None: log a warning and return it;
      - otherwise raise :class:`TeamMappingError` with diagnostic info.

    Most callers should leave ``default_on_miss=None`` (fail loudly). The
    aggregation pipeline may pass ``REFERENCE_FIFA_POINTS`` for graceful
    degradation, but strict is the default.
    """
    release = fifa_release if fifa_release is not None else get_current_rankings()
    result = TeamFifaMapper(release).resolve(sportmonks_team)

    if isinstance(result, TeamFifaMapping):
        return result.fifa_points

    if default_on_miss is not None:
        logger.warning(
            "Could not resolve team '%s' (id=%s) to FIFA; using default %.2f. "
            "Attempted keys: %s",
            result.sportmonks_name, result.sportmonks_team_id,
            default_on_miss, result.attempted_keys,
        )
        return default_on_miss

    raise TeamMappingError(
        f"Could not resolve SportMonks team '{result.sportmonks_name}' "
        f"(id={result.sportmonks_team_id}) to a FIFA ranking entry. "
        f"Attempted keys: {result.attempted_keys}. "
        f"Add an entry to data/team_mapping_overrides.json to fix this."
    )


def _clean(value: Any) -> str | None:
    """Normalize an identifier to a non-empty stripped string, or None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None
