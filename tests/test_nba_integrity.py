import unittest
from unittest.mock import patch,Mock
from nba_game_update.boxscores import validate_box
from nba_game_update.tank01_client import Tank01Client
from nba_game_update.game_parser import parse_game
from nba_ratings_update.pipeline import current_season

class IngestionTests(unittest.TestCase):
    def test_boxscore_includes_players_not_known_in_database(self):
        game=dict(game_id='g',home_team='NYK',away_team='BOS',home_score=120,away_score=100,game_date='2026-10-23')
        players={str(i):dict(mins='48',pts='24' if i<5 else '20',fga='20',fta='4',OffReb='2',TOV='2',teamID=1 if i<5 else 2,teamAbv='NY' if i<5 else 'BOS',longName=f'Player {i}') for i in range(10)}
        box={'body':{'gameStatus':'Completed','playerStats':players}}
        players['10']={'mins':'--'}
        rows=validate_box(game,box)
        self.assertEqual(len(rows),10)
        self.assertEqual(rows[0]['home_away'],'Home')
        players['0']['pts']='0'
        with self.assertRaises(RuntimeError):validate_box(game,box)

    def test_api_errors_fail_instead_of_becoming_empty_schedule(self):
        response=Mock(status_code=403,headers={})
        with patch('nba_game_update.tank01_client.requests.get',return_value=response):
            with self.assertRaises(RuntimeError):Tank01Client('test',rate_limit=0).get_scores_for_date('20261023')

    def test_scores_dictionary_supplies_game_id(self):
        response=Mock(status_code=200)
        response.json.return_value={'body':{'20261023_BOS@NY':{'home':'NY','away':'BOS','homePts':'110','awayPts':'100','gameStatus':'Completed'}}}
        with patch('nba_game_update.tank01_client.requests.get',return_value=response):
            raw=Tank01Client('test',rate_limit=0).get_scores_for_date('20261023')
        row=parse_game(raw[0]);self.assertEqual(row['season'],'2026-27');self.assertEqual(row['home_team'],'NYK')

    def test_explicit_season_override(self):
        with patch.dict('os.environ',{'NBA_SEASON':'2026-27'}):self.assertEqual(current_season(),'2026-27')
