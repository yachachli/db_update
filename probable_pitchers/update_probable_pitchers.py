
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime

# 1) SCRAPE
def scrape_probable_pitchers(date_str=None) -> pd.DataFrame:
    if date_str:
        url = f"https://baseballsavant.mlb.com/probable-pitchers?date={date_str}"
        scrape_date = date_str
    else:
        url = "https://baseballsavant.mlb.com/probable-pitchers"
        scrape_date = datetime.today().strftime("%Y-%m-%d")

    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")

    rows = []
    for pi_div in soup.find_all("div", class_="player-info"):
        h3 = pi_div.find("h3")
        if not h3:
            continue
        a = h3.find("a")
        name = a.text.strip() if a else h3.get_text(strip=True).split("#")[0].strip()

        block = pi_div.find_parent("div", class_=lambda c: c and c.startswith("col"))
        if block is None:
            continue

        data = {"date_scraped": scrape_date, "name": name}
        career_map = {
            "PA": "career_vs_opp_pa",
            "K%": "career_vs_opp_kpct",
            "BB%": "career_vs_opp_bbpct",
            "AVG": "career_vs_opp_avg",
            "wOBA": "career_vs_opp_woba",
        }
        statcast_map = {
            "Exit Velo": "exit_velo",
            "Launch Angle": "launch_angle",
            "xBA": "xba",
            "xSLG": "xslg",
            "xwOBA": "xwoba",
        }

        for tbl in block.find_all("table", class_="pitcher-stats"):
            hdrs = [th.get_text(strip=True) for th in tbl.select("thead th.block")]
            vals = [td.get_text(strip=True) for td in tbl.select("tbody td.data")]
            for h, v in zip(hdrs, vals):
                key = career_map.get(h) or statcast_map.get(h)
                if key:
                    data[key] = v.replace("MPH", "").strip()

        rows.append(data)

    return pd.DataFrame(rows)


# 2) CLEAN & GRADE
MIN_PA = 20
STATS_CFG = {
    "xwoba": {"dir": -1, "wt": 30},
    "exit_velo": {"dir": -1, "wt": 20},
    "xslg": {"dir": -1, "wt": 10},
    "career_vs_opp_woba": {"dir": -1, "wt": 10, "cond": "career_vs_opp_pa"},
    "career_vs_opp_kpct": {"dir": 1, "wt": 10, "cond": "career_vs_opp_pa"},
    "career_vs_opp_bbpct": {"dir": -1, "wt": 5, "cond": "career_vs_opp_pa"},
    "log_career_vs_opp_pa": {"dir": 1, "wt": 15},
}
GRADE_LABELS = ["F", "D", "C", "B", "A"]
ESSENTIAL = ["xwoba", "exit_velo", "xslg"]

def calculate_pitcher_grades(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # numeric conversions
    df["exit_velo"] = pd.to_numeric(df.get("exit_velo"), errors="coerce")
    for c in ["career_vs_opp_kpct", "career_vs_opp_bbpct"]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce") / 100.0
    for c in [
        "career_vs_opp_pa",
        "career_vs_opp_avg",
        "career_vs_opp_woba",
        "launch_angle",
        "xba",
        "xslg",
        "xwoba",
    ]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    df["log_career_vs_opp_pa"] = np.log1p(df["career_vs_opp_pa"].fillna(0))

    # z‐scores
    zcols = {}
    for stat, cfg in STATS_CFG.items():
        series = df.get(stat, pd.Series(dtype=float)).copy()
        cond = cfg.get("cond")
        if cond and cond in df:
            series[df[cond] < MIN_PA] = np.nan
        if series.notna().any():
            m, s = series.mean(), series.std()
            z = (series - m) / s if s and not np.isnan(s) else 0
        else:
            z = 0
        df[f"z_{stat}"] = cfg["dir"] * pd.Series(z).fillna(0)
        zcols[stat] = f"z_{stat}"

    # weighted sum
    df["probable_pitcher_score"] = sum(
        df[zcols[s]] * cfg["wt"] for s, cfg in STATS_CFG.items()
    )

    # quintile grades
    df["probable_pitcher_grade"] = "N/A"
    mask = df[ESSENTIAL].notna().all(axis=1)
    if mask.any():
        try:
            df.loc[mask, "probable_pitcher_grade"] = pd.qcut(
                df.loc[mask, "probable_pitcher_score"], 5, labels=GRADE_LABELS
            ).astype(str)
        except ValueError:
            pass

    return df


# 3) UPSERT
def upsert_to_db(df: pd.DataFrame):
    # match your daily_task pattern:
    conn_params = {
        "dbname":   os.getenv("DB_NAME"),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host":     os.getenv("DB_HOST"),
    }
    # sanity check
    if not all(conn_params.values()):
        raise RuntimeError("DB_NAME/DB_USER/DB_PASSWORD/DB_HOST must all be set")

    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()

    sql = """
    INSERT INTO public.mlb_probable_pitchers
      (date_scraped, player_id, name, team_abv,
       career_vs_opp_pa, career_vs_opp_kpct, career_vs_opp_bbpct,
       career_vs_opp_avg, career_vs_opp_woba, exit_velo,
       launch_angle, xba, xslg, xwoba,
       probable_pitcher_score, probable_pitcher_grade)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (date_scraped, name) DO UPDATE SET
      player_id = EXCLUDED.player_id,
      team_abv = EXCLUDED.team_abv,
      career_vs_opp_pa = EXCLUDED.career_vs_opp_pa,
      career_vs_opp_kpct = EXCLUDED.career_vs_opp_kpct,
      career_vs_opp_bbpct = EXCLUDED.career_vs_opp_bbpct,
      career_vs_opp_avg = EXCLUDED.career_vs_opp_avg,
      career_vs_opp_woba = EXCLUDED.career_vs_opp_woba,
      exit_velo = EXCLUDED.exit_velo,
      launch_angle = EXCLUDED.launch_angle,
      xba = EXCLUDED.xba,
      xslg = EXCLUDED.xslg,
      xwoba = EXCLUDED.xwoba,
      probable_pitcher_score = EXCLUDED.probable_pitcher_score,
      probable_pitcher_grade = EXCLUDED.probable_pitcher_grade;
    """

    for _, row in df.iterrows():
        cur.execute(
            "SELECT player_id, team_abv FROM public.mlb_players WHERE long_name=%s",
            (row["name"],),
        )
        pid, team = cur.fetchone() or (None, None)

        pa = None
        if not pd.isna(row.get("career_vs_opp_pa")):
            pa = int(row["career_vs_opp_pa"])

        vals = (
            row["date_scraped"], pid, row["name"], team,
            pa,
            row.get("career_vs_opp_kpct"),
            row.get("career_vs_opp_bbpct"),
            row.get("career_vs_opp_avg"),
            row.get("career_vs_opp_woba"),
            row.get("exit_velo"),
            row.get("launch_angle"),
            row.get("xba"),
            row.get("xslg"),
            row.get("xwoba"),
            row["probable_pitcher_score"],
            row["probable_pitcher_grade"],
        )
        cur.execute(sql, vals)

    cur.close()
    conn.close()


if __name__ == "__main__":
    raw = scrape_probable_pitchers()
    raw = raw.drop_duplicates(subset=["date_scraped", "name"])
    graded = calculate_pitcher_grades(raw)
    upsert_to_db(graded)
    print(f"✅ Upserted {len(graded)} pitchers for {graded['date_scraped'].iat[0]}")
