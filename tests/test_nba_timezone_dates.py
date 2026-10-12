import unittest
from datetime import datetime, timezone

from nba_time import daily_fetch_dates, game_date_from_commence


class NbaTimezoneDateTests(unittest.TestCase):
    def test_daily_fetch_dates_honors_pacific_daylight_time(self):
        # 00:37 PDT is still 23:37 on the prior day under a fixed UTC-8 offset.
        now = datetime(2026, 10, 12, 7, 37, tzinfo=timezone.utc)

        now_pacific, yesterday, today = daily_fetch_dates(now)

        self.assertEqual(now_pacific.isoformat(), "2026-10-12T00:37:00-07:00")
        self.assertEqual(yesterday, "20261011")
        self.assertEqual(today, "20261012")

    def test_daily_fetch_dates_honors_pacific_standard_time(self):
        now = datetime(2026, 12, 12, 8, 37, tzinfo=timezone.utc)

        now_pacific, yesterday, today = daily_fetch_dates(now)

        self.assertEqual(now_pacific.isoformat(), "2026-12-12T00:37:00-08:00")
        self.assertEqual(yesterday, "20261211")
        self.assertEqual(today, "20261212")

    def test_odds_date_uses_eastern_daylight_time(self):
        # 04:30 UTC is 00:30 EDT, not the prior date as a fixed UTC-5 offset implies.
        self.assertEqual(
            game_date_from_commence("2026-10-12T04:30:00Z"),
            "2026-10-12",
        )

    def test_odds_date_uses_eastern_standard_time(self):
        self.assertEqual(
            game_date_from_commence("2026-12-12T04:30:00Z"),
            "2026-12-11",
        )


if __name__ == "__main__":
    unittest.main()
