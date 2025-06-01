import datetime

import httpx
import msgspec

from db_update.env import Env


class MlbTeam(msgspec.Struct, frozen=True):
    """Represents an MLB team with statistics and metadata."""

    teamAbv: str
    """SF"""
    teamCity: str
    """San Francisco"""
    RS: str  # Runs Scored
    """242"""
    loss: str
    """25"""
    teamName: str
    """Giants"""
    mlbLogo1: str
    """https://www.mlbstatic.com/team-logos/team-cap-on-light/137.svg"""
    DIFF: str  # Run Differential
    """39"""
    teamID: str
    """24"""
    division: str
    """West"""
    RA: str  # Runs Against
    """203"""
    conferenceAbv: str
    """NL"""
    espnLogo1: str
    """https://a.espncdn.com/combiner/i?img=/i/teamlogos/mlb/500/sf.png"""
    wins: str
    """31"""
    conference: str
    """National League"""


class MlbTeamsResponse(msgspec.Struct, frozen=True):
    """Represents the API response containing MLB team data."""

    statusCode: int
    body: list[MlbTeam]


async def get_mlb_teams(client: httpx.AsyncClient) -> list[MlbTeam]:
    url = f"https://{Env.RAPIDAPI_HOST}/getMLBTeams"

    res = await client.get(
        url,
        headers={
            "x-rapidapi-host": Env.RAPIDAPI_HOST,
            "x-rapidapi-key": Env.RAPIDAPI_KEY,
        },
    )
    res.raise_for_status()
    data = msgspec.json.decode(res.content, type=MlbTeamsResponse)
    return data.body


class MlbPlayer(msgspec.Struct, frozen=True):
    """Represents an MLB player with basic information."""

    pos: str
    """'P'"""
    playerID: str
    """'571656'"""
    team: str
    """'HOU'"""
    longName: str
    """'Buck Farmer'"""
    teamID: str
    """'11'"""


class MlbPlayerResponses(msgspec.Struct, frozen=True):
    """Represents the API response containing MLB player data."""

    statusCode: int
    body: list[MlbPlayer]


async def get_mlb_players(client: httpx.AsyncClient) -> list[MlbPlayer]:
    url = f"https://{Env.RAPIDAPI_HOST}/getMLBPlayerList"

    res = await client.get(
        url,
        headers={
            "x-rapidapi-host": Env.RAPIDAPI_HOST,
            "x-rapidapi-key": Env.RAPIDAPI_KEY,
        },
    )
    res.raise_for_status()
    data = msgspec.json.decode(res.content, type=MlbPlayerResponses)
    return data.body


class MlbInjury(msgspec.Struct, frozen=True):
    description: str
    designation: str
    inj_return_date: str = msgspec.field(name="injReturnDate")
    inj_date: str | None = msgspec.field(name="injDate", default=None)


class MlbPlayerDetail(msgspec.Struct, frozen=True):
    bat: str
    height: str
    injury: MlbInjury
    pos: str
    team: str
    throw: str
    weight: str
    jersey_num: str = msgspec.field(name="jerseyNum")
    long_name: str = msgspec.field(name="longName")
    mlb_headshot: str = msgspec.field(name="mlbHeadshot")
    mlb_id: str = msgspec.field(name="mlbID")
    mlb_id_full: str = msgspec.field(name="mlbIDFull")
    mlb_link: str = msgspec.field(name="mlbLink")
    player_id: str = msgspec.field(name="playerID")
    team_abv: str = msgspec.field(name="teamAbv")
    team_id: str = msgspec.field(name="teamID")
    age: str | None = None
    college: str | None = None
    b_day: str | None = msgspec.field(name="bDay", default=None)
    birth_place: str | None = msgspec.field(name="birthPlace", default=None)
    cbs_long_name: str | None = msgspec.field(name="cbsLongName", default=None)
    cbs_player_id: str | None = msgspec.field(name="cbsPlayerID", default=None)
    cbs_player_id_full: str | None = msgspec.field(name="cbsPlayerIDFull", default=None)
    espn_headshot: str | None = msgspec.field(name="espnHeadshot", default=None)
    espn_id: str | None = msgspec.field(name="espnID", default=None)
    espn_link: str | None = msgspec.field(name="espnLink", default=None)
    espn_name: str | None = msgspec.field(name="espnName", default=None)
    espn_status: str | None = msgspec.field(name="espnStatus", default=None)
    fantasy_pros_link: str | None = msgspec.field(name="fantasyProsLink", default=None)
    fantasy_pros_player_id: str | None = msgspec.field(
        name="fantasyProsPlayerID", default=None
    )
    high_school: str | None = msgspec.field(name="highSchool", default=None)
    is_starting_pitcher: str | None = msgspec.field(
        name="isStartingPitcher", default=None
    )
    last_game_played: str | None = msgspec.field(name="lastGamePlayed", default=None)
    mlb_short_name: str | None = msgspec.field(name="mlbShortName", default=None)
    roto_wire_player_id: str | None = msgspec.field(
        name="rotoWirePlayerID", default=None
    )
    roto_wire_player_id_full: str | None = msgspec.field(
        name="rotoWirePlayerIDFull", default=None
    )
    sleeper_bot_id: str | None = msgspec.field(name="sleeperBotID", default=None)
    yahoo_link: str | None = msgspec.field(name="yahooLink", default=None)
    yahoo_player_id: str | None = msgspec.field(name="yahooPlayerID", default=None)


class MlbPlayerDetailResponse(msgspec.Struct, frozen=True):
    body: MlbPlayerDetail
    status_code: int = msgspec.field(name="statusCode")


async def get_mlb_player_info(
    client: httpx.AsyncClient, player_id: str
) -> MlbPlayerDetail:
    url = f"https://{Env.RAPIDAPI_HOST}/getMLBPlayerInfo"

    res = await client.get(
        url,
        headers={
            "x-rapidapi-host": Env.RAPIDAPI_HOST,
            "x-rapidapi-key": Env.RAPIDAPI_KEY,
        },
        params={
            "playerID": player_id,
            "getStats": "false",
            "statsSeason": datetime.datetime.now().year,
        },
    )
    res.raise_for_status()
    data = msgspec.json.decode(res.content, type=MlbPlayerDetailResponse)
    return data.body


class Hitting(msgspec.Struct, frozen=True):
    _2_b: str = msgspec.field(name="2B")
    _3_b: str = msgspec.field(name="3B")
    ab: str = msgspec.field(name="AB")
    avg: str = msgspec.field(name="AVG")
    bb: str = msgspec.field(name="BB")
    gidp: str = msgspec.field(name="GIDP")
    h: str = msgspec.field(name="H")
    hbp: str = msgspec.field(name="HBP")
    hr: str = msgspec.field(name="HR")
    ibb: str = msgspec.field(name="IBB")
    r: str = msgspec.field(name="R")
    rbi: str = msgspec.field(name="RBI")
    sac: str = msgspec.field(name="SAC")
    sf: str = msgspec.field(name="SF")
    so: str = msgspec.field(name="SO")
    tb: str = msgspec.field(name="TB")
    batting_order: str = msgspec.field(name="battingOrder")
    substitution_order: str = msgspec.field(name="substitutionOrder")


class Pitching(msgspec.Struct, frozen=True):
    decision: str
    bb: str = msgspec.field(name="BB")
    balk: str = msgspec.field(name="Balk")
    batters_faced: str = msgspec.field(name="Batters Faced")
    er: str = msgspec.field(name="ER")
    era: str = msgspec.field(name="ERA")
    flyouts: str = msgspec.field(name="Flyouts")
    groundouts: str = msgspec.field(name="Groundouts")
    h: str = msgspec.field(name="H")
    hbp: str = msgspec.field(name="HBP")
    hr: str = msgspec.field(name="HR")
    inherited_runners_scored: str = msgspec.field(name="Inherited Runners Scored")
    inherited_runners: str = msgspec.field(name="Inherited Runners")
    innings_pitched: str = msgspec.field(name="InningsPitched")
    pitches: str = msgspec.field(name="Pitches")
    r: str = msgspec.field(name="R")
    so: str = msgspec.field(name="SO")
    strikes: str = msgspec.field(name="Strikes")
    wild_pitch: str = msgspec.field(name="Wild Pitch")
    pitching_order: str = msgspec.field(name="pitchingOrder")


class Fielding(msgspec.Struct, frozen=True):
    e: str = msgspec.field(name="E")
    outfield_assists: str = msgspec.field(name="Outfield assists")
    passed_ball: str = msgspec.field(name="Passed Ball")
    pickoffs: str = msgspec.field(name="Pickoffs")


class BaseRunning(msgspec.Struct, frozen=True):
    cs: str = msgspec.field(name="CS")
    po: str = msgspec.field(name="PO")
    sb: str = msgspec.field(name="SB")


class MlbGameStats(msgspec.Struct, frozen=True):
    started: str
    team: str
    base_running: BaseRunning = msgspec.field(name="BaseRunning")
    fielding: Fielding = msgspec.field(name="Fielding")
    hitting: Hitting = msgspec.field(name="Hitting")
    pitching: Pitching = msgspec.field(name="Pitching")
    all_positions_played: str = msgspec.field(name="allPositionsPlayed")
    game_id: str = msgspec.field(name="gameID")
    mlb_id: str = msgspec.field(name="mlbID")
    player_id: str = msgspec.field(name="playerID")
    starting_position: str = msgspec.field(name="startingPosition")
    team_id: str = msgspec.field(name="teamID")
    note: str | None = None


class MlbPlayerGameForPlayerResponse(msgspec.Struct, frozen=True):
    body: dict[str, MlbGameStats]
    status_code: int = msgspec.field(name="statusCode")


async def get_mlb_games_for_player(
    client: httpx.AsyncClient, player_id: str
) -> dict[str, MlbGameStats]:
    url = f"https://{Env.RAPIDAPI_HOST}/getMLBGamesForPlayer"

    res = await client.get(
        url,
        headers={
            "x-rapidapi-host": Env.RAPIDAPI_HOST,
            "x-rapidapi-key": Env.RAPIDAPI_KEY,
        },
        params={
            "playerID": player_id,
            "season": datetime.datetime.now().year,
        },
    )
    data = msgspec.json.decode(res.content, type=MlbPlayerGameForPlayerResponse)
    return data.body
