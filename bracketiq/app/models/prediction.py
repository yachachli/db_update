"""
Task 4: Core Prediction Model (BracketIQ).
KenPom-based margin/score/win probability with user-tunable weights and recency.
"""

from typing import Optional

from scipy.stats import norm

from app.models.schemas import (
    TeamProfile,
    MatchupPredictionResponse,
    KeyFactor,
    KenPomBaseline,
)

# KenPom constants (HCA 5.0 from 2026 season data: actual avg home margin +5.24)
HOME_COURT_ADVANTAGE = 5.0
LEAGUE_AVG_TEMPO = 67.0  # approximate
MARGIN_SIGMA = 11.5  # ~MAE 9.15 -> sigma ~11.5
PYTHAG_EXPONENT = 10.25


class BracketIQModel:
    """
    Prediction engine with user-adjustable weights.
    Base prediction uses KenPom AdjEM methodology.
    """

    def __init__(self, weights: Optional[dict] = None):
        self.default_weights = {
            "offense": 1.0,
            "defense": 1.0,
            "three_point": 1.0,
            "tempo": 1.0,
            "experience": 1.0,
            "recency": 0.6,
            "clutch": 1.0,
            "injury": 1.0,
        }
        self.weights = {**self.default_weights, **(weights or {})}

    def predict_matchup(
        self,
        team_a: TeamProfile,
        team_b: TeamProfile,
        neutral: bool = True,
    ) -> MatchupPredictionResponse:
        """Run full prediction and return structured response."""
        # 1) KenPom baseline margin (per 100 poss)
        adj_oe_a = team_a.adj_oe
        adj_de_a = team_a.adj_de
        adj_oe_b = team_b.adj_oe
        adj_de_b = team_b.adj_de
        tempo_a = team_a.adj_tempo or LEAGUE_AVG_TEMPO
        tempo_b = team_b.adj_tempo or LEAGUE_AVG_TEMPO

        # Recency blend if we have recent metrics
        recency_w = self.weights.get("recency", 0.6)
        if team_a.recent_adj_oe is not None and team_a.recent_adj_de is not None:
            adj_oe_a = (1 - recency_w) * team_a.adj_oe + recency_w * team_a.recent_adj_oe
            adj_de_a = (1 - recency_w) * team_a.adj_de + recency_w * team_a.recent_adj_de
        if team_b.recent_adj_oe is not None and team_b.recent_adj_de is not None:
            adj_oe_b = (1 - recency_w) * team_b.adj_oe + recency_w * team_b.recent_adj_oe
            adj_de_b = (1 - recency_w) * team_b.adj_de + recency_w * team_b.recent_adj_de

        base_margin = (adj_oe_a - adj_de_b) - (adj_oe_b - adj_de_a)
        if not neutral:
            base_margin += HOME_COURT_ADVANTAGE

        # Pure KenPom baseline (no recency) for comparison
        kp_margin = (team_a.adj_oe - team_b.adj_de) - (team_b.adj_oe - team_a.adj_de)
        if not neutral:
            kp_margin += HOME_COURT_ADVANTAGE
        kp_win_prob_a = float(norm.cdf(kp_margin / MARGIN_SIGMA))
        kenpom_baseline = KenPomBaseline(predicted_margin=kp_margin, win_prob_a=kp_win_prob_a)

        # 2) User weight modifiers
        off_w = self.weights.get("offense", 1.0)
        def_w = self.weights.get("defense", 1.0)
        off_diff = (adj_oe_a - adj_de_b) - (adj_oe_b - adj_de_a)
        # Scale the offensive component
        margin = base_margin
        if off_diff != 0:
            margin = margin + (off_w - 1.0) * (off_diff / 2)
        if def_w != 1.0:
            margin = margin * (0.5 + 0.5 * def_w)

        # Three-point differential (eFG / 3P rate)
        three_w = self.weights.get("three_point", 1.0)
        if three_w != 1.0 and (team_a.off_efg or team_b.off_efg or team_a.three_pt_rate or team_b.three_pt_rate):
            efg_diff = (team_a.off_efg - team_b.def_efg) - (team_b.off_efg - team_a.def_efg)
            three_impact = efg_diff * 25  # rough pts per 100 poss
            margin = margin + (three_w - 1.0) * three_impact

        # Tempo advantage
        tempo_w = self.weights.get("tempo", 1.0)
        if tempo_w != 1.0 and tempo_a and tempo_b:
            tempo_diff = (tempo_a - LEAGUE_AVG_TEMPO) - (tempo_b - LEAGUE_AVG_TEMPO)
            margin = margin + (tempo_w - 1.0) * (tempo_diff * 0.1)

        # Experience
        exp_w = self.weights.get("experience", 1.0)
        if exp_w != 1.0 and (team_a.experience or team_b.experience):
            exp_diff = (team_a.experience or 0) - (team_b.experience or 0)
            margin = margin + (exp_w - 1.0) * (exp_diff * 0.5)

        # 3) Predicted score from tempo + efficiency
        pred_poss = (tempo_a * tempo_b) / LEAGUE_AVG_TEMPO if (tempo_a and tempo_b) else LEAGUE_AVG_TEMPO
        pts_a_per_100 = adj_oe_a
        pts_b_per_100 = adj_oe_b
        # Opponent adjustment: A's pts = poss * (A_OE/100), B's pts = poss * (B_OE/100) but we use def for opponent
        score_a_raw = pred_poss * (adj_oe_a / 100)
        score_b_raw = pred_poss * (adj_oe_b / 100)
        # Scale so margin matches our predicted margin (preserve total if desired)
        total_raw = score_a_raw + score_b_raw
        if total_raw > 0:
            # Distribute so score_a - score_b = margin and score_a + score_b ≈ total_raw
            score_a = (total_raw / 2) + (margin / 2)
            score_b = (total_raw / 2) - (margin / 2)
        else:
            score_a = 70 + margin / 2
            score_b = 70 - margin / 2
        predicted_score_a = max(0, round(score_a))
        predicted_score_b = max(0, round(score_b))

        # 4) Win probability from normal CDF
        win_prob_a = float(norm.cdf(margin / MARGIN_SIGMA))
        win_prob_a = max(0.0, min(1.0, win_prob_a))
        win_prob_b = 1.0 - win_prob_a

        # 5) Key factors (four factors differentials)
        factors = []
        # Offensive efficiency edge
        oe_edge = (adj_oe_a - adj_de_b) - (adj_oe_b - adj_de_a)
        if abs(oe_edge) > 0.5:
            team = team_a.name if oe_edge > 0 else team_b.name
            factors.append(KeyFactor(factor="Offensive Efficiency Edge", team=team, magnitude=f"{'+' if oe_edge > 0 else ''}{oe_edge:.1f}"))
        # eFG
        efg_a = team_a.off_efg - team_b.def_efg
        efg_b = team_b.off_efg - team_a.def_efg
        efg_net = efg_a - efg_b
        if abs(efg_net) > 0.01:
            team = team_a.name if efg_net > 0 else team_b.name
            factors.append(KeyFactor(factor="Effective FG%", team=team, magnitude=f"{'+' if efg_net > 0 else ''}{(efg_net*100):.1f}%"))
        # Turnover
        to_net = (team_b.def_to - team_a.off_to) - (team_a.def_to - team_b.off_to)
        if abs(to_net) > 0.005:
            team = team_a.name if to_net > 0 else team_b.name
            factors.append(KeyFactor(factor="Turnover Rate", team=team, magnitude=f"{'+' if to_net > 0 else ''}{(to_net*100):.1f}%"))
        # Tempo
        if abs(tempo_a - tempo_b) > 0.5:
            team = team_a.name if tempo_a > tempo_b else team_b.name
            factors.append(KeyFactor(factor="Tempo Advantage", team=team, magnitude=f"{abs(tempo_a - tempo_b):.1f} poss"))
        factors = sorted(factors, key=key_factor_sort_key, reverse=True)[:3]

        return MatchupPredictionResponse(
            team_a=team_a.name,
            team_b=team_b.name,
            predicted_score_a=predicted_score_a,
            predicted_score_b=predicted_score_b,
            predicted_margin=round(margin, 1),
            win_prob_a=round(win_prob_a, 2),
            win_prob_b=round(win_prob_b, 2),
            key_factors=factors,
            weights_applied=self.weights,
            kenpom_baseline=kenpom_baseline,
        )


def key_factor_sort_key(f: KeyFactor) -> float:
    s = f.magnitude.replace("%", "").replace("+", "").replace("poss", "").strip().split()
    if not s:
        return 0.0
    try:
        return abs(float(s[0]))
    except ValueError:
        return 0.0
