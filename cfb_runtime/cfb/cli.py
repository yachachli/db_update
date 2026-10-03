import argparse
from cfb.data import migrate, ingest_games, ingest_lines, ingest_players, snapshot_odds


def main():
    parser=argparse.ArgumentParser()
    sub=parser.add_subparsers(dest='command',required=True)
    sub.add_parser('migrate')
    games=sub.add_parser('games'); games.add_argument('--years',type=int,nargs='+',required=True)
    teams=sub.add_parser('team-stats'); teams.add_argument('--years',type=int,nargs='+',required=True)
    advanced=sub.add_parser('advanced-stats'); advanced.add_argument('--years',type=int,nargs='+',required=True)
    advanced_test=sub.add_parser('advanced-backtest'); advanced_test.add_argument('--years',type=int,nargs='+',default=[2023,2024,2025])
    advanced_test.add_argument('--allow-missing-games',action='store_true',help='Retain evaluation targets with missing advanced observations; maximum 1 percent of games')
    efficiency=sub.add_parser('efficiency-backtest'); efficiency.add_argument('--years',type=int,nargs='+',default=[2023,2024,2025])
    efficiency.add_argument('--allow-missing-stats',action='store_true',help='Explicitly skip missing efficiency observations, retaining all evaluation games; maximum 1 percent of team rows')
    players=sub.add_parser('players'); players.add_argument('--year',type=int,required=True); players.add_argument('--week',type=int,required=True)
    sub.add_parser('odds')
    lines=sub.add_parser('lines'); lines.add_argument('--years',type=int,nargs='+',required=True)
    props=sub.add_parser('collect-props'); props.add_argument('--limit',type=int,default=5)
    props.add_argument('--analyze-limit',type=int,default=3)
    props.add_argument('--new-only',action='store_true',help='Skip previously analyzed game/player/market/line combinations')
    props.add_argument('--per-game-limit',type=int,default=10)
    sub.add_parser('train-release')
    sub.add_parser('track-results')
    pipeline=sub.add_parser('pipeline')
    pipeline.add_argument('--mode',choices=['weekly','markets'],default='weekly')
    pipeline.add_argument('--season',type=int)
    pipeline.add_argument('--lookback',type=int,default=3)
    pipeline.add_argument('--analyses',type=int,default=0)
    pipeline.add_argument('--dry-run',action='store_true')
    identity=sub.add_parser('audit-identities'); identity.add_argument('--limit',type=int,default=7)
    analyze=sub.add_parser('analyze'); analyze.add_argument('--request',required=True,help='JSON request, not credentials')
    serve=sub.add_parser('serve'); serve.add_argument('--port',type=int,default=8088)
    audit=sub.add_parser('outcome-audit'); audit.add_argument('--run-ids',type=int,nargs='+',required=True)
    intervals=sub.add_parser('interval-backtest'); intervals.add_argument('--run-ids',type=int,nargs='+',required=True)
    pilot=sub.add_parser('play-label-pilot'); pilot.add_argument('--limit',type=int,default=60)
    history=sub.add_parser('player-history'); history.add_argument('--years',type=int,nargs='+',required=True)
    pou=sub.add_parser('pou-backtest'); pou.add_argument('--years',type=int,nargs='+',default=[2023,2024,2025])
    pou.add_argument('--allow-missing-games',action='store_true',help='Record missing offensive-history games without filling zeros; maximum 1 percent')
    backtest=sub.add_parser('backtest'); backtest.add_argument('--test-year',type=int,default=2025)
    args=parser.parse_args()
    if args.command=='migrate': migrate(); print('Schema ready')
    elif args.command=='games': ingest_games(args.years)
    elif args.command=='team-stats':
        from cfb.team_stats import ingest
        ingest(args.years)
    elif args.command=='advanced-stats':
        from cfb.advanced import ingest
        ingest(args.years)
    elif args.command=='advanced-backtest':
        from cfb.advanced import run
        run(args.years,args.allow_missing_games)
    elif args.command=='efficiency-backtest':
        from cfb.efficiency import run
        run(args.years,args.allow_missing_stats)
    elif args.command=='players': ingest_players(args.year,args.week)
    elif args.command=='train-release':
        from cfb.release import train
        train()
    elif args.command=='track-results':
        from cfb.tracking import run
        run()
    elif args.command=='pipeline':
        from cfb.pipeline import run
        try:
            run(args.mode,args.season,args.lookback,args.analyses,args.dry_run)
        except Exception as exc:
            # Cron logs must never expose DB URLs or request URLs through tracebacks.
            parser.exit(1,f'CFB pipeline failed ({type(exc).__name__}); inspect pipeline_runs and data coverage.\n')
    elif args.command=='audit-identities':
        from cfb.identity import run
        run(args.limit)
    elif args.command=='analyze':
        import json
        from cfb.analyze import analyze
        print(json.dumps(analyze(json.loads(args.request)),indent=2,allow_nan=False))
    elif args.command=='serve':
        from cfb.analyze import serve
        serve(args.port)
    elif args.command=='outcome-audit':
        from cfb.outcomes import run
        run(args.run_ids)
    elif args.command=='interval-backtest':
        from cfb.intervals import run
        run(args.run_ids)
    elif args.command=='play-label-pilot':
        from cfb.play_labels import run
        run(args.limit)
    elif args.command=='player-history':
        from cfb.players import ingest
        ingest(args.years)
    elif args.command=='pou-backtest':
        from cfb.pou import run
        run(args.years,args.allow_missing_games)
    elif args.command=='lines': ingest_lines(args.years)
    elif args.command=='odds': snapshot_odds()
    elif args.command=='collect-props':
        from cfb.markets import collect
        collect(args.limit,args.analyze_limit,args.new_only,args.per_game_limit)
    elif args.command=='backtest':
        from cfb.backtest import run
        run(args.test_year)


if __name__=='__main__': main()
