"""Generate a PDF of projected XIs for every team in the WC 2026 roster.

Requires Neon (squad/id_map), SportMonks API, and fpdf2.

Run from project root:

    py -3 scripts/generate_projected_lineups_pdf.py
    py -3 scripts/generate_projected_lineups_pdf.py --output reports/my_lineups.pdf
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fpdf import FPDF

from src.player_ratings import (
    build_team_player_display_for_code,
    resolve_team_code_for_id,
)
from src.sportmonks_client import SportmonksClient

_ROSTER_PATH = _ROOT / "data" / "wc2026_teams.json"
_DEFAULT_OUTPUT_DIR = _ROOT / "reports"
_CONFED_ORDER = ("CONCACAF", "UEFA", "CONMEBOL", "CAF", "AFC", "OFC")
_FORMATION_LABEL = "4-3-3"
_BENCH_LIMIT = 7


def _load_roster() -> list[dict]:
    data = json.loads(_ROSTER_PATH.read_text(encoding="utf-8"))
    teams = [row for row in data.get("teams", []) if row.get("sportmonks_team_id")]
    confed_rank = {name: idx for idx, name in enumerate(_CONFED_ORDER)}
    teams.sort(
        key=lambda row: (
            confed_rank.get(str(row.get("confederation", "")), 99),
            str(row.get("search_name", "")).lower(),
        )
    )
    return teams


def _windows_fonts() -> tuple[Path, Path | None]:
    regular_candidates = [
        Path(r"C:\Windows\Fonts\arial.ttf"),
        Path(r"C:\Windows\Fonts\segoeui.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/System/Library/Fonts/Supplemental/Arial.ttf"),
    ]
    bold_candidates = [
        Path(r"C:\Windows\Fonts\arialbd.ttf"),
        Path(r"C:\Windows\Fonts\segoeuib.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
    ]
    regular = next((path for path in regular_candidates if path.is_file()), None)
    if regular is None:
        raise RuntimeError("No Unicode TTF font found for PDF output.")
    bold = next((path for path in bold_candidates if path.is_file()), regular)
    return regular, bold


def _fmt_mins(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.0%}"


def _fmt_rating(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}"


class LineupPDF(FPDF):
    def __init__(self, regular_font: Path, bold_font: Path) -> None:
        super().__init__(orientation="P", unit="mm", format="A4")
        self.add_font("Body", "", str(regular_font))
        self.add_font("Body", "B", str(bold_font))
        self.set_auto_page_break(auto=True, margin=14)

    def _set_body(self, size: float = 10, bold: bool = False) -> None:
        style = "B" if bold else ""
        self.set_font("Body", style=style, size=size)

    def cover_page(self, generated_at: str, team_count: int) -> None:
        self.add_page()
        self._set_body(18, bold=True)
        self.cell(0, 14, "World Cup 2026", ln=True)
        self._set_body(14, bold=True)
        self.cell(0, 10, "Projected Lineups", ln=True)
        self.ln(4)
        self._set_body(10)
        self.multi_cell(
            0,
            6,
            (
                f"Generated: {generated_at}\n"
                f"Teams: {team_count}\n"
                f"Formation template: {_FORMATION_LABEL}\n"
                "Selection: minutes share in last 5 competitive matches "
                "(qualifiers + WC finals when played).\n"
                "Manual overrides: data/xi_overrides.json"
            ),
        )

    def summary_page(self, rows: list[dict]) -> None:
        self.add_page()
        self._set_body(14, bold=True)
        self.cell(0, 10, "Coverage Summary", ln=True)
        self.ln(2)
        self._set_body(9)

        by_status: dict[str, list[str]] = {}
        for row in rows:
            by_status.setdefault(str(row["status"]), []).append(row["label"])

        for status in ("ok", "partial", "no_qualifier_data", "manual_ratings_no_xi"):
            teams = by_status.get(status, [])
            if not teams:
                continue
            self._set_body(10, bold=True)
            self.cell(0, 7, f"{status} ({len(teams)})", ln=True)
            self._set_body(9)
            self.multi_cell(0, 5, ", ".join(teams))
            self.ln(2)

        missing = [row["label"] for row in rows if not row.get("team_code")]
        if missing:
            self._set_body(10, bold=True)
            self.cell(0, 7, f"no FIFA code in Neon ({len(missing)})", ln=True)
            self._set_body(9)
            self.multi_cell(0, 5, ", ".join(missing))

    def confederation_header(self, confederation: str) -> None:
        if self.get_y() > 250:
            self.add_page()
        self.ln(3)
        self._set_body(12, bold=True)
        self.set_fill_color(230, 230, 230)
        self.cell(0, 8, confederation, ln=True, fill=True)
        self.ln(1)

    def team_block(self, label: str, block: dict, *, team_code: str | None) -> None:
        if self.get_y() > 210:
            self.add_page()

        self._set_body(11, bold=True)
        code_suffix = f" [{team_code}]" if team_code else ""
        status = block.get("status", "?")
        self.cell(0, 7, f"{label}{code_suffix}  -  {status}", ln=True)

        xi = block.get("projected_xi") or []
        if not xi:
            self._set_body(9)
            self.multi_cell(
                0,
                5,
                "No projected XI (missing squad, id_map, or qualifier ratings).",
            )
            squad = block.get("squad") or []
            if squad:
                self.cell(0, 5, f"Squad rows in Neon: {len(squad)}", ln=True)
            self.ln(2)
            return

        col_w = (10, 12, 78, 18, 16, 18)
        headers = ("#", "Pos", "Player", "Rating", "Mins", "Matches")
        self._set_body(8, bold=True)
        for header, width in zip(headers, col_w, strict=True):
            self.cell(width, 6, header, border=1)
        self.ln()

        self._set_body(8)
        for player in xi:
            self._player_row(player, col_w)

        bench = block.get("bench") or []
        if bench:
            self.ln(1)
            self._set_body(8, bold=True)
            self.cell(0, 5, f"Bench ({len(bench)} rated)", ln=True)
            self._set_body(8)
            for player in bench[:_BENCH_LIMIT]:
                self._player_row(player, col_w)
            if len(bench) > _BENCH_LIMIT:
                self.cell(0, 5, f"... +{len(bench) - _BENCH_LIMIT} more", ln=True)
        self.ln(3)

    def _player_row(self, player: dict, col_w: tuple[int, ...]) -> None:
        values = (
            str(player.get("squad_no", "")),
            str(player.get("position", "")),
            str(player.get("player_name", ""))[:36],
            _fmt_rating(player.get("avg_rating")),
            _fmt_mins(player.get("minutes_share")),
            str(player.get("matches_counted", "")),
        )
        for value, width in zip(values, col_w, strict=True):
            self.cell(width, 5.5, value, border=1)
        self.ln()


def _collect_team_blocks(client: SportmonksClient) -> list[dict]:
    rows: list[dict] = []
    for entry in _load_roster():
        team_id = int(entry["sportmonks_team_id"])
        name = str(entry["search_name"])
        confederation = str(entry.get("confederation", ""))
        team_code = resolve_team_code_for_id(team_id)
        label = name

        if not team_code:
            rows.append(
                {
                    "team_id": team_id,
                    "label": label,
                    "confederation": confederation,
                    "team_code": None,
                    "status": "no_fifa_code",
                    "block": {
                        "projected_xi": [],
                        "bench": [],
                        "status": "no_fifa_code",
                        "squad": [],
                    },
                }
            )
            print(f"  skip {name}: no FIFA code in Neon")
            continue

        print(f"  {team_code} {name}")
        block = build_team_player_display_for_code(team_code, team_id, client)
        rows.append(
            {
                "team_id": team_id,
                "label": label,
                "confederation": confederation,
                "team_code": team_code,
                "status": block.get("status", "?"),
                "block": block,
            }
        )
    return rows


def generate_pdf(output_path: Path) -> Path:
    regular_font, bold_font = _windows_fonts()

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("Fetching lineups for all roster teams...")
    client = SportmonksClient()
    rows = _collect_team_blocks(client)

    pdf = LineupPDF(regular_font, bold_font)
    pdf.cover_page(generated_at, len(rows))

    current_confed = ""
    for row in rows:
        confed = row["confederation"]
        if confed != current_confed:
            pdf.confederation_header(confed)
            current_confed = confed
        pdf.team_block(row["label"], row["block"], team_code=row.get("team_code"))

    pdf.summary_page(rows)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(output_path))
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PDF path (default: reports/projected_lineups_YYYYMMDD.pdf)",
    )
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    output = args.output or (_DEFAULT_OUTPUT_DIR / f"projected_lineups_{stamp}.pdf")

    try:
        path = generate_pdf(output.resolve())
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    print(f"\nWrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
