from datetime import datetime, timezone
import hashlib
import json
import math
import re
from zoneinfo import ZoneInfo


def event_record(raw, endpoint, date, run_id):
    gid=str(raw.get('gameID') or '')
    match=re.fullmatch(r'(\d{8})_([A-Z]+)@([A-Z]+)',gid)
    if not match or match[1]!=date or (match[2],match[3])!=(raw.get('away'),raw.get('home')):
        raise ValueError('Provider game identity does not match date/teams')
    event_time=None
    try:
        epoch=float(raw.get('gameTime_epoch'))
        if math.isfinite(epoch) and 946684800<=epoch<4102444800:
            value=datetime.fromtimestamp(epoch,timezone.utc)
            if value.astimezone(ZoneInfo('America/New_York')).strftime('%Y%m%d')==date:
                event_time=value
    except (ValueError,TypeError,OverflowError):pass
    def score(value):
        if isinstance(value,bool) or not re.fullmatch(r'\d+',str(value)):return None
        return int(value)
    home,away=score(raw.get('homePts')),score(raw.get('awayPts'))
    final=(endpoint=='getWNBAScoresOnly' and str(raw.get('gameStatusCode'))=='2'
           and str(raw.get('gameStatus','')).lower() in ('final','completed')
           and event_time is not None and home is not None and away is not None and home!=away)
    payload=json.dumps(raw,sort_keys=True,separators=(',',':'),allow_nan=False)
    ident=hashlib.sha256(json.dumps([run_id,endpoint,gid,payload],separators=(',',':')).encode()).hexdigest()
    return dict(observation_id=ident,run_id=run_id,endpoint=endpoint,game_id=gid,
                game_date=datetime.strptime(date,'%Y%m%d').date(),event_time=event_time,
                is_final=final,home_score=home,away_score=away,payload=payload)
