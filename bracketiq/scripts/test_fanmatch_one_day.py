"""
Test FanMatch scrape for a single day. Run from backend dir:
  py -m scripts.test_fanmatch_one_day
  py -m scripts.test_fanmatch_one_day 2026-02-15
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.scrapers.kenpom_scraper import get_kenpom_browser, get_fanmatch_for_date


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-03-02"  # one day, adjust if no games
    print(f"Testing FanMatch for one day: {date_str}")
    print("Logging in to KenPom...")
    try:
        browser = get_kenpom_browser()
        print("Login OK.")
    except Exception as e:
        print(f"Login failed: {e}")
        return 1

    print(f"Fetching FanMatch for {date_str} (one request, ~10s delay)...")
    fm = get_fanmatch_for_date(browser, date_str)

    if fm is None:
        print("Result: FanMatch object is None (often means 'no games today' for that date).")
        return 0
    if fm.fm_df is None:
        print("Result: fm_df is None (page may have said 'no games today' or parsing failed).")
        print(f"  Other attributes: ppg={fm.ppg}, avg_eff={fm.avg_eff}, mean_abs_err_pred_mov={fm.mean_abs_err_pred_mov}")
        return 0

    df = fm.fm_df
    print(f"Result: got DataFrame with {len(df)} rows, {len(df.columns)} columns")
    print("Columns:", list(df.columns))
    if len(df) > 0:
        print("First row (as dict):")
        for k, v in df.iloc[0].items():
            print(f"  {k}: {v}")
    print("Diagnostics: mean_abs_err_pred_mov =", fm.mean_abs_err_pred_mov)
    return 0


if __name__ == "__main__":
    sys.exit(main())
