"""
Refresh all KenPom cache tables (rate-limited). Run from backend dir:
  python -m scripts.refresh_kenpom_cache
"""

import sys
from pathlib import Path

# Ensure backend root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.scrapers.kenpom_scraper import get_kenpom_browser, verify_kenpom_login, refresh_all


def main():
    print("Logging in to KenPom...")
    browser = get_kenpom_browser()
    print("Verifying KenPom access...")
    verify_kenpom_login(browser)
    print("Refreshing all tables (8s+ delay between each)...")
    refresh_all(browser)
    print("Done. Cache updated in app/data/cache/")


if __name__ == "__main__":
    main()
