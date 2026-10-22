"""Append observed provider game facts to Neon; never rewrite source models."""
from datetime import datetime,timedelta
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from uuid import uuid4
from zoneinfo import ZoneInfo

from .records import event_record


def main():
    from sqlalchemy import text
    from nba_game_update.db import get_engine
    host=os.environ['WNBA_API_HOST']
    if not re.fullmatch(r'tank01-[a-z0-9-]*wnba[a-z0-9-]*\.p\.rapidapi\.com',host):
        raise ValueError('Unexpected WNBA provider host; no requests sent')
    headers={'x-rapidapi-host':host,'x-rapidapi-key':os.environ['WNBA_API_KEY']}
    today=datetime.now(ZoneInfo('America/New_York')).date()
    dates=[(today+timedelta(days=d)).strftime('%Y%m%d') for d in (-1,0,1)]
    run_id=os.environ.get('GITHUB_RUN_ID',str(uuid4()))+'-'+os.environ.get('GITHUB_RUN_ATTEMPT','1')
    engine=get_engine();errors=[];count=0
    sql=text('''INSERT INTO propgpt_evidence.wnba_events
        (observation_id,run_id,endpoint,game_id,game_date,event_time,is_final,home_score,away_score,payload)
        VALUES (:observation_id,:run_id,:endpoint,:game_id,:game_date,:event_time,:is_final,:home_score,:away_score,CAST(:payload AS jsonb))
        ON CONFLICT (observation_id) DO NOTHING''')
    for date in dates:
        for endpoint in ('getWNBAGamesForDate','getWNBAScoresOnly'):
            url='https://'+host+'/'+endpoint+'?'+urllib.parse.urlencode({'gameDate':date})
            try:
                with urllib.request.urlopen(urllib.request.Request(url,headers=headers),timeout=45) as r:data=json.load(r)
                if data.get('statusCode') not in (200,'200') or not isinstance(data.get('body'),(dict,list)):
                    raise ValueError('Unexpected provider response schema')
                body=data['body'];rows=list(body.values()) if isinstance(body,dict) else body
                records=[event_record(row,endpoint,date,run_id) for row in rows]
                if records:
                    with engine.begin() as conn:conn.execute(sql,records)
                count+=len(records)
                print(json.dumps({'date':date,'endpoint':endpoint,'records':len(records),'final':sum(r['is_final'] for r in records)}),flush=True)
            except Exception as exc:
                # Never print request headers, connection strings or provider error bodies.
                errors.append({'date':date,'endpoint':endpoint,'error_type':type(exc).__name__})
    print(json.dumps({'observations':count,'errors':errors}),flush=True)
    if errors:raise SystemExit('Partial WNBA event collection; successful observations were retained')


if __name__=='__main__':main()
