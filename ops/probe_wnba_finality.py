"""Read-only schema probe; two provider calls, no database access."""
import argparse
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--date',required=True);args=parser.parse_args()
    if not re.fullmatch(r'\d{8}',args.date):raise ValueError('Expected YYYYMMDD')
    host=os.environ['WNBA_API_HOST']
    if not re.fullmatch(r'tank01-[a-z0-9-]*wnba[a-z0-9-]*\.p\.rapidapi\.com',host):
        raise ValueError('Expected the existing Tank01 WNBA RapidAPI host; no request sent')
    report=[]
    for endpoint in ('getWNBAGamesForDate','getWNBAScoresOnly'):
        url='https://'+host+'/'+endpoint+'?'+urllib.parse.urlencode({'gameDate':args.date})
        req=urllib.request.Request(url,headers={'x-rapidapi-host':host,'x-rapidapi-key':os.environ['WNBA_API_KEY']})
        try:
            with urllib.request.urlopen(req,timeout=45) as resp:data=json.load(resp)
        except urllib.error.HTTPError as e:
            report.append({'endpoint':endpoint,'http_status':e.code});continue
        body=data.get('body');rows=list(body.values()) if isinstance(body,dict) else body if isinstance(body,list) else []
        fields=sorted({k for row in rows if isinstance(row,dict) for k in row})
        allowed={'gameID','gameDate','home','away','homePts','awayPts','gameStatus','gameStatusCode','gameTime','gameTime_epoch','gameTimeUTC'}
        report.append({'endpoint':endpoint,'provider_status':data.get('statusCode'),'body_type':type(body).__name__,
                       'row_count':len(rows),'fields':fields,
                       'samples':[{k:v for k,v in row.items() if k in allowed} for row in rows[:3] if isinstance(row,dict)]})
    out=Path('reports/wnba-finality-probe.json');out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps({'date':args.date,'results':report},indent=2))
    for item in report:print(json.dumps({k:v for k,v in item.items() if k!='samples'}))


if __name__=='__main__':main()
