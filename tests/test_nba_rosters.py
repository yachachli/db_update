import copy
import unittest
from unittest.mock import AsyncMock, MagicMock
from db_update.runners.nba_rosters import TEAMS, roster_plan, sync_rosters


def fixture():
    teams = [dict(id=i,team_abv=t) for i,t in enumerate(sorted(TEAMS),1)]
    body=[]
    for t in teams:
        roster={}
        for j in range(12):
            pid=t['id']*100+j
            roster[str(pid)]=dict(playerID=str(pid),longName=f'Player {pid}',teamID=str(t['id']),team=t['team_abv'],pos='SF')
        body.append(dict(teamID=str(t['id']),teamAbv=t['team_abv'],Roster=roster))
    return dict(statusCode=200,body=body),teams


class RosterSafety(unittest.TestCase):
    def test_complete_roster_resolves_transferred_legacy_id_without_losing_history(self):
        payload,teams=fixture(); p=payload['body'][0]['Roster']['100'];p['longName']='Riley Minix'
        plan=roster_plan(payload,[dict(player_id=99,name='Riley Minix',team_id=30)],teams)
        self.assertEqual(plan[0]['player_id'],99)
        self.assertEqual(plan[0]['provider_player_id'],100)
        self.assertEqual(plan[0]['team_id'],1)
        self.assertTrue(plan[0]['existing'])

    def test_missing_team_rejected(self):
        payload,teams=fixture();payload['body'].pop()
        with self.assertRaises(ValueError):roster_plan(payload,[],teams)

    def test_duplicate_player_rejected(self):
        payload,teams=fixture();p=copy.deepcopy(payload['body'][0]['Roster']['100']);p.update(team='BKN',teamID='2')
        payload['body'][1]['Roster']['100']=p
        with self.assertRaises(ValueError):roster_plan(payload,[],teams)

    def test_ambiguous_alias_is_not_guessed(self):
        payload,teams=fixture()
        with self.assertRaisesRegex(ValueError,'Ambiguous'):
            roster_plan(payload,[dict(player_id=1,name='Player 100'),dict(player_id=2,name='Player 100')],teams)

    def test_reused_provider_id_rejected(self):
        payload,teams=fixture()
        with self.assertRaisesRegex(ValueError,'ID/name'):
            roster_plan(payload,[dict(player_id=100,name='Someone Else')],teams)

    def test_inner_roster_team_conflict_rejected(self):
        payload,teams=fixture();payload['body'][0]['Roster']['100']['teamID']='30'
        with self.assertRaisesRegex(ValueError,'team conflict'):roster_plan(payload,[],teams)

    def test_error_payload_rejected(self):
        _,teams=fixture()
        with self.assertRaises(ValueError):roster_plan(dict(statusCode=403,body=[]),[],teams)


class RosterTransaction(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_snapshot_does_not_write_players_or_clear_membership(self):
        payload,teams=fixture();payload['body'].pop()
        conn=AsyncMock();conn.fetch.side_effect=[[],teams]
        conn.transaction=MagicMock();conn.transaction.return_value.__aenter__=AsyncMock();conn.transaction.return_value.__aexit__=AsyncMock(return_value=False)
        pool=MagicMock();pool.acquire.return_value.__aenter__=AsyncMock(return_value=conn);pool.acquire.return_value.__aexit__=AsyncMock(return_value=False)
        with self.assertRaises(ValueError):await sync_rosters(pool,payload)
        self.assertEqual(conn.execute.await_count,1) # advisory lock only
        conn.executemany.assert_not_awaited()
