"""Cron entrypoint: bounded refresh, data quality gates, and persisted status."""
from datetime import datetime, timezone
import json
import os
from cfb.data import connect, ingest_games
from cfb.partitions import recent


def season_for(now):
    # January bowls/playoffs belong to the preceding fall season.
    return now.year - 1 if now.month < 7 else now.year


def plan(mode, season=None, lookback=3, analyses=0):
    if mode not in ('weekly', 'markets') or not 1 <= lookback <= 6 or not 0 <= analyses <= 10:
        raise ValueError('Invalid pipeline options')
    year = season if season is not None else season_for(datetime.now(timezone.utc))
    if not 2021 <= year <= datetime.now(timezone.utc).year:
        raise ValueError('Unsupported season')
    return dict(mode=mode, season=year, recent_partitions=lookback,
                analyses=analyses, retrain=False, market_ready=False,
                stages=['schedule', 'team_stats', 'player_stats', 'coverage', 'repair', 'outcomes'] if mode == 'weekly'
                else ['schedule', 'coverage', 'props', 'identities'])


def coverage(year, lookback):
    """Split gaps the bounded refresh can heal from a backlog it structurally cannot."""
    with connect() as conn:
        history = conn.execute('''SELECT season,count(*) FROM cfb_model_v1.games
            WHERE completed AND season BETWEEN %s AND %s
            AND home_points IS NOT NULL AND away_points IS NOT NULL
            AND home_classification='fbs' AND away_classification='fbs' GROUP BY season''', (year-2, year-1)).fetchall()
        if any(dict(history).get(season, 0) < 100 for season in (year-2, year-1)):
            raise ValueError('Historical bootstrap required before weekly operation')
        # Window selection must match what team_stats/players actually refresh.
        schedule = conn.execute('''SELECT game_id,week,season_type,kickoff FROM cfb_model_v1.games
            WHERE season=%s AND completed''', (year,)).fetchall()
        rows = conn.execute('''SELECT game_id,week,season_type,kickoff FROM cfb_model_v1.games
            WHERE season=%s AND completed AND home_classification='fbs' AND away_classification='fbs' ''', (year,)).fetchall()
        ids = [r[0] for r in rows]
        incomplete = conn.execute('''SELECT g.game_id FROM cfb_model_v1.games g WHERE g.game_id=ANY(%s)
            AND (EXISTS(SELECT 1 FROM (VALUES(g.home_id),(g.away_id)) AS t(team_id)
                WHERE NOT EXISTS(SELECT 1 FROM cfb_model_v1.team_game_stats s
                   WHERE s.game_id=g.game_id AND s.team_id=t.team_id
                     AND s.pass_yards IS NOT NULL AND s.pass_attempts IS NOT NULL
                     AND s.rush_yards IS NOT NULL AND s.rush_attempts IS NOT NULL)
                OR NOT EXISTS(SELECT 1 FROM cfb_model_v1.player_offense p
                   WHERE p.game_id=g.game_id AND p.team_id=t.team_id))) ORDER BY g.game_id''', (ids,)).fetchall()
    missing = {r[0] for r in incomplete}
    window = {r[0] for r in recent(schedule, lookback)}
    partition = {r[0]: [r[1], r[2]] for r in rows}
    blocking = sorted(missing & window)
    backlog = sorted(missing - window)
    report = dict(completed_games_checked=len(ids), refresh_window_partitions=lookback,
                  incomplete_game_ids=blocking, stale_backlog_game_ids=backlog,
                  stale_backlog_partitions=sorted({tuple(partition[g]) for g in backlog}))
    report['stale_backlog_partitions'] = [list(p) for p in report['stale_backlog_partitions']]
    if blocking or backlog:
        print(json.dumps(report), flush=True)
    if blocking:
        # The refresh just ran over these partitions, so a gap here is a real failure.
        raise ValueError('Coverage incomplete inside the refresh window; downstream stages blocked')
    return report


def repair(year, partitions, limit=2):
    """Re-ingest a bounded slice of the out-of-window backlog so it drains over runs."""
    targets = [tuple(p) for p in partitions][:max(0, limit)]
    if not targets:
        return dict(repaired_partitions=[], remaining_partitions=0)
    from cfb.team_stats import ingest as team_ingest
    from cfb.players import ingest as player_ingest
    team_ingest([year], partitions=targets, refresh=True)
    player_ingest([year], partitions=targets, refresh=True)
    return dict(repaired_partitions=[list(t) for t in targets],
                remaining_partitions=max(0, len(partitions) - len(targets)))


def run(mode='weekly', season=None, lookback=3, analyses=0, dry_run=False):
    config = plan(mode, season, lookback, analyses)
    if dry_run:
        print(json.dumps(config, indent=2))
        return config
    required = ['DATABASE_URL', 'CFBD_API_KEY'] + (['ODDS_API_KEY'] if mode == 'markets' else [])
    if any(not os.environ.get(k) for k in required):
        raise ValueError('Required environment variables missing (values withheld)')
    if analyses:
        if mode != 'markets':
            raise ValueError('Analyses are only supported in markets mode')
        from cfb.analyze import load_artifacts
        for category in ('passing', 'rushing', 'receiving'):
            manifest, _, _, _ = load_artifacts(category)
            if manifest['analysis_season'] != config['season']:
                raise ValueError('Frozen model does not support this season')
    # Transaction-scoped lock is safe for Neon transaction pooling. Keep this
    # transaction open; status updates commit on independent connections.
    with connect() as guard:
        if not guard.execute('SELECT pg_try_advisory_xact_lock(719104211)').fetchone()[0]:
            raise RuntimeError('Another CFB pipeline is already running')
        with connect() as conn:
            # Holding the lock proves nothing else is running, so any surviving
            # 'running' row is from a run the job timeout killed before its handler.
            abandoned = conn.execute('''UPDATE cfb_model_v1.pipeline_runs SET status='abandoned',
                finished_at=clock_timestamp(), error_type='process_terminated'
                WHERE status='running' RETURNING id''').fetchall()
            if abandoned:
                print(json.dumps(dict(abandoned_runs=[r[0] for r in abandoned])), flush=True)
        with connect() as conn:
            run_id = conn.execute('''INSERT INTO cfb_model_v1.pipeline_runs(mode,season,status,details)
                VALUES (%s,%s,'running',%s) RETURNING id''',
                (mode, config['season'], json.dumps(config))).fetchone()[0]
        details = dict(config)
        stage = None
        try:
            for stage in config['stages']:
                with connect() as conn:
                    conn.execute('UPDATE cfb_model_v1.pipeline_runs SET current_stage=%s WHERE id=%s', (stage, run_id))
                print(json.dumps(dict(run_id=run_id, stage=stage, status='starting')), flush=True)
                year = config['season']
                if stage == 'schedule':
                    ingest_games([year], refresh=True)
                elif stage == 'team_stats':
                    from cfb.team_stats import ingest
                    ingest([year], recent_partitions=lookback, refresh=True)
                elif stage == 'player_stats':
                    from cfb.players import ingest
                    ingest([year], recent_partitions=lookback, refresh=True)
                elif stage == 'coverage':
                    details['coverage'] = coverage(year, lookback)
                elif stage == 'repair':
                    details['repair'] = repair(year, details.get('coverage', {}).get('stale_backlog_partitions', []))
                elif stage == 'outcomes':
                    from cfb.tracking import run as track
                    details['outcomes'] = track()
                elif stage == 'props':
                    from cfb.markets import collect
                    details['props'] = collect(10, analyses, new_only=True, per_game_limit=1)
                elif stage == 'identities':
                    from cfb.identity import run as identities
                    details['identities'] = identities(7)
                # Persist each completed stage, not only the eventual success.
                with connect() as conn:
                    conn.execute('UPDATE cfb_model_v1.pipeline_runs SET details=%s WHERE id=%s', (json.dumps(details), run_id))
        except Exception as exc:
            with connect() as conn:
                conn.execute('''UPDATE cfb_model_v1.pipeline_runs SET status='failed',finished_at=clock_timestamp(),
                    error_type=%s WHERE id=%s''', (type(exc).__name__, run_id))
            raise RuntimeError(f'Pipeline {run_id} failed at {stage}: {type(exc).__name__}; credentials withheld') from None
        with connect() as conn:
            conn.execute("UPDATE cfb_model_v1.pipeline_runs SET status='success',finished_at=clock_timestamp() WHERE id=%s", (run_id,))
    print(json.dumps(dict(run_id=run_id, status='success', market_ready=False)))
    return details
