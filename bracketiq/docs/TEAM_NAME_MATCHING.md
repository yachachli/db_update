# Team Name Matching (No FanMatch Match Issues)

All matching between **FanMatch** (KenPom Game string), **slate** (Odds API + KenPom), and **cache** uses a single system so the same matchup always resolves to the same key.

## How it works

1. **Canonical name**  
   Every team has one **canonical** name (e.g. the KenPom-style name: `"Fresno St."`, `"Gonzaga"`, `"Tennessee Martin"`).

2. **Aliases**  
   Every other way the team can appear (Odds API, FanMatch HTML, “St.” vs “State”, mascots, etc.) is an **alias** that maps to that canonical name.

3. **Files**  
   - **`data/kenpom_aliases.json`** – `{ "Canonical Name": ["alias1", "alias2", ...] }`.  
   - Loaded by `app/services/team_name_resolver.py`: `resolve_to_canonical_kenpom(any_name)` returns the canonical name; `fanmatch_match_key(name)` returns the normalized key used for FanMatch ↔ slate matching.

4. **Match key**  
   For FanMatch lookup we use:  
   `fanmatch_match_key(resolve_to_canonical_kenpom(name))`.  
   So “Fresno St Bulldogs”, “Fresno State”, “Fresno St.” all become the same key (e.g. `"fresno state"` after St.→State and lowercasing).

When **every** variant is in the alias list, FanMatch and slate always match.

## Building aliases from a CSV

If you have a CSV of every college (and optionally all name variants), you can regenerate or merge into `kenpom_aliases.json`:

```bash
cd /path/to/bracketiq
python -m scripts.build_team_aliases_from_csv path/to/teams.csv
```

- **Overwrite**: writes `data/kenpom_aliases.json` (or `-o path`).
- **Merge** with existing aliases: add `--merge`.

### CSV format

**Option A – Long (one row per alias)**  
Best when you have many variants per team.

| canonical | alias        |
|-----------|--------------|
| Gonzaga   | Gonzaga      |
| Gonzaga   | Gonzaga Bulldogs |
| Fresno St. | Fresno St.   |
| Fresno St. | Fresno State |
| Fresno St. | Fresno St Bulldogs |

Header can be `canonical,alias` or `canonical_name,alias`. First column = canonical, second = one alias per row.

**Option B – Wide (one row per team)**  
One row per team; first column = canonical, rest = aliases.

| canonical | alias_1           | alias_2              |
|-----------|-------------------|----------------------|
| Gonzaga   | Gonzaga Bulldogs  | Gonzaga University   |
| Fresno St. | Fresno State     | Fresno St Bulldogs   |

- **Canonical** = the name you use everywhere (e.g. KenPom style: “Fresno St.”, “NC State”, “St. John’s”).
- **Aliases** = every other spelling you might see: Odds API (“Fresno St Bulldogs”), FanMatch (“Fresno State”), “St.” vs “State”, mascots, etc.

Include the **canonical** itself as one of the aliases (script does this for long format; for wide, the first column is the canonical and is added automatically).

## What to put in the CSV

For “never have matching issues”:

1. **One row per team** (or per alias in long format).
2. **Canonical** = KenPom-style name (match what’s in your KenPom cache / `pomeroy_ratings`).
3. **Aliases** for each team should include:
   - Odds API names (e.g. “Fresno St Bulldogs”, “Gonzaga Bulldogs”).
   - FanMatch / KenPom variants (“Fresno State”, “Gonzaga”).
   - “St.” vs “State” (e.g. “Oregon St.” and “Oregon State”).
   - A&M vs AM, Saint vs St., abbreviations (UConn, BYU, USC, etc.).
   - Any other spelling you’ve seen in logs or “No FanMatch row” games.

After you run the script, **restart or re-run** any process that loads `kenpom_aliases.json` (e.g. `slate_today`); the resolver loads the file once per process.

## Quick check

After updating aliases:

```bash
python -m scripts.slate_today
```

In the JSON, games that have a FanMatch row for that date should show `kenpom_fanmatch_margin_home_pov` and `kenpom_fanmatch_total` non-null and `sanity_note` “Adjacent to KenPom” (or a sanity message), not “No FanMatch row for today”.
