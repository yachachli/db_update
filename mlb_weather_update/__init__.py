"""mlb_weather_update — game-time weather forecasts for the MLB slate.

Runs a couple of times a day on GitHub Actions. Populates
propgpt_mlb.weather_observations with an hour-of-first-pitch forecast (from Open-Meteo,
no API key) for every game, using park coordinates. Fixed-dome parks get a flagged row
with weather NULL. Self-contained — no imports from sibling folders.

Entry point: `python -m mlb_weather_update`
"""
