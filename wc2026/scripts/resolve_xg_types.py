"""Resolve every type_id in the xGFixture include and map its data shape.

Builds on the earlier xG diagnostic: we confirmed ``include=xGFixture`` returns
expected-stats data. This script enumerates every type_id in that payload,
resolves each to a human-readable name (from the cached /types catalog or by
fetching it), records the values/shape per type, and writes a resolution map
we can use to write a correct parser.

Purely diagnostic -- NO parser or business logic.

Run from the project root (cached where possible):

    python scripts/resolve_xg_types.py
"""

from __future__ import annotations

import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

# Allow direct execution by putting the project root on sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

logger = logging.getLogger("resolve_xg_types")

DIAGNOSTIC_PATH = _PROJECT_ROOT / "data" / "xg_diagnostic_responses.json"
STAT_TYPES_PATH = _PROJECT_ROOT / "data" / "stat_types.json"
RESOLUTIONS_PATH = _PROJECT_ROOT / "data" / "xg_type_resolutions.json"

CORE_BASE_URL = "https://api.sportmonks.com/v3/core"

# Type metadata fields we want to capture for each resolved type.
TYPE_FIELDS = ("name", "code", "developer_name", "model_type", "stat_group")

# Keywords -> the model stat each maps to, for the final cross-reference.
MODEL_STAT_KEYWORDS: dict[str, list[str]] = {
    "xG (expected goals)": ["expected goals"],
    "xGOT (expected goals on target)": ["expected goals on target", "xgot", "on target"],
    "Big chances created": ["big chance"],
}


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------
def load_xgfixture_entries() -> list[dict[str, Any]]:
    """Find the xGFixture array in the cached diagnostic response."""
    if not DIAGNOSTIC_PATH.exists():
        raise FileNotFoundError(
            f"{DIAGNOSTIC_PATH} not found. Run inspect_recent_qualifier_xg.py first."
        )
    diagnostic = json.loads(DIAGNOSTIC_PATH.read_text(encoding="utf-8"))

    strategies = diagnostic.get("phase2_strategies", {})
    for label, entry in strategies.items():
        if entry.get("status") != "200 OK":
            continue
        data = (entry.get("response") or {}).get("data") or {}
        xgfixture = data.get("xgfixture")
        if isinstance(xgfixture, list) and xgfixture:
            logger.info("Using xGFixture data from strategy '%s' (%d entries).", label, len(xgfixture))
            return xgfixture

    raise ValueError("No populated xgfixture array found in the diagnostic file.")


def summarize_entries(
    entries: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Group entries by type_id, collecting values, participants, and fields."""
    by_type: dict[int, dict[str, Any]] = {}
    for entry in entries:
        type_id = entry.get("type_id")
        if not isinstance(type_id, int):
            continue
        bucket = by_type.setdefault(
            type_id,
            {
                "values": [],
                "by_participant": defaultdict(list),
                "entry_fields": set(),
                "locations": set(),
            },
        )
        value = (entry.get("data") or {}).get("value")
        bucket["values"].append(value)
        bucket["by_participant"][entry.get("participant_id")].append(value)
        bucket["entry_fields"].update(entry.keys())
        if "location" in entry:
            bucket["locations"].add(entry.get("location"))
    return by_type


def classify_value_kind(values: list[Any]) -> str:
    """Describe whether a type's values are integers, decimals, or mixed."""
    has_decimal = False
    has_integer = False
    for v in values:
        if isinstance(v, bool):
            continue
        if isinstance(v, float):
            if v != int(v):
                has_decimal = True
            else:
                has_integer = True
        elif isinstance(v, int):
            has_integer = True
    if has_decimal and has_integer:
        return "mixed"
    if has_decimal:
        return "decimal"
    if has_integer:
        return "integer"
    return "unknown"


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------
def load_local_type_map() -> dict[int, dict[str, Any]]:
    """Load the cached /types catalog into {id: type_obj}, if present."""
    if not STAT_TYPES_PATH.exists():
        return {}
    try:
        types = json.loads(STAT_TYPES_PATH.read_text(encoding="utf-8"))
    except ValueError:
        return {}
    return {int(t["id"]): t for t in types if "id" in t}


def resolve_type(
    type_id: int,
    local_map: dict[int, dict[str, Any]],
    core_client: SportmonksClient,
) -> dict[str, Any]:
    """Return type metadata, preferring the local catalog, else fetching it."""
    source = "stat_types.json"
    type_obj = local_map.get(type_id)
    if type_obj is None:
        source = f"core/types/{type_id}"
        try:
            response = core_client.get(f"types/{type_id}")
            type_obj = response.get("data") or {}
        except SportmonksError as exc:
            logger.warning("Could not resolve type %s: %s", type_id, exc)
            type_obj = {}
            source = f"UNRESOLVED ({exc})"

    resolved = {field: type_obj.get(field) for field in TYPE_FIELDS}
    resolved["source"] = source
    return resolved


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def build_resolution_map(
    by_type: dict[int, dict[str, Any]],
    local_map: dict[int, dict[str, Any]],
    core_client: SportmonksClient,
) -> dict[str, Any]:
    resolutions: dict[str, Any] = {}
    for type_id in sorted(by_type):
        bucket = by_type[type_id]
        meta = resolve_type(type_id, local_map, core_client)
        resolutions[str(type_id)] = {
            "name": meta.get("name"),
            "code": meta.get("code"),
            "developer_name": meta.get("developer_name"),
            "model_type": meta.get("model_type"),
            "stat_group": meta.get("stat_group"),
            "source": meta.get("source"),
            "value_kind": classify_value_kind(bucket["values"]),
            "values": bucket["values"],
            "by_participant": {
                str(pid): vals for pid, vals in bucket["by_participant"].items()
            },
            "entry_fields": sorted(bucket["entry_fields"]),
            "locations": sorted(str(loc) for loc in bucket["locations"]),
        }
    return resolutions


def print_table(resolutions: dict[str, Any]) -> None:
    print("\n" + "=" * 90)
    print("RESOLVED xGFixture TYPE_IDS")
    print("=" * 90)
    print(f"{'type_id':>8} | {'name':<34} | {'kind':<8} | {'model_type':<12} | values")
    print("-" * 90)
    for type_id, info in resolutions.items():
        vals = info["values"]
        preview = ", ".join(str(v) for v in vals[:4])
        if len(vals) > 4:
            preview += ", ..."
        print(
            f"{type_id:>8} | {str(info['name'])[:34]:<34} | "
            f"{info['value_kind']:<8} | {str(info['model_type'])[:12]:<12} | {preview}"
        )


def print_model_crossref(resolutions: dict[str, Any]) -> None:
    print("\n" + "=" * 90)
    print("MODEL STAT CROSS-REFERENCE")
    print("=" * 90)
    for label, needles in MODEL_STAT_KEYWORDS.items():
        matches = [
            (tid, info)
            for tid, info in resolutions.items()
            if info.get("name")
            and any(n in str(info["name"]).lower() for n in needles)
        ]
        if matches:
            for tid, info in matches:
                print(f"  {label:<35} -> type_id={tid} ({info['name']}, {info['value_kind']})")
        else:
            print(f"  {label:<35} -> NOT FOUND in xGFixture types")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 90)
    print("Resolve xGFixture type_ids")
    print("=" * 90)

    try:
        entries = load_xgfixture_entries()
    except (FileNotFoundError, ValueError) as exc:
        print(f"\n[ERROR] {exc}")
        return 1

    by_type = summarize_entries(entries)
    print(f"\nUnique type_ids in xGFixture: {len(by_type)}")
    print(f"Total entries: {len(entries)}")

    local_map = load_local_type_map()
    logger.info("Local /types catalog has %d entries.", len(local_map))

    try:
        core_client = SportmonksClient(base_url=CORE_BASE_URL)
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] {exc}")
        return 1

    resolutions = build_resolution_map(by_type, local_map, core_client)

    RESOLUTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESOLUTIONS_PATH.write_text(
        json.dumps(resolutions, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print_table(resolutions)
    print_model_crossref(resolutions)

    print(f"\nSaved resolution map to {RESOLUTIONS_PATH}")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
