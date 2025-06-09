import msgspec

from db_update.async_caching_client import AsyncCachingClient
from db_update.env import Env


class Injury(msgspec.Struct, frozen=True):
    description: str
    designation: str
    inj_return_date: str = msgspec.field(name="injReturnDate")


class Reb(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Tptfgm(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Stl(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Ftm(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Ast(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Tptfga(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Tov(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Pts(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Fta(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Fga(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Blk(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class Fgm(msgspec.Struct, frozen=True):
    c: str = msgspec.field(name="C")
    f: str = msgspec.field(name="F")
    g: str = msgspec.field(name="G")
    total: str = msgspec.field(name="Total")


class CurrentStreak(msgspec.Struct, frozen=True):
    length: int
    result: str


class OffensiveStats(msgspec.Struct, frozen=True):
    ast: Ast
    blk: Blk
    fga: Fga
    fgm: Fgm
    fta: Fta
    ftm: Ftm
    pts: Pts
    reb: Reb
    stl: Stl
    tptfga: Tptfga
    tptfgm: Tptfgm
    tov: Tov = msgspec.field(name="TOV")
    games_played: str = msgspec.field(name="gamesPlayed")
    pts_away: str = msgspec.field(name="ptsAway")
    pts_home: str = msgspec.field(name="ptsHome")
    team_abv: str = msgspec.field(name="teamAbv")
    team_id: str = msgspec.field(name="teamID")


class DefensiveStats(msgspec.Struct, frozen=True):
    ast: Ast
    blk: Blk
    fga: Fga
    fgm: Fgm
    fta: Fta
    ftm: Ftm
    pts: Pts
    reb: Reb
    stl: Stl
    tptfga: Tptfga
    tptfgm: Tptfgm
    tov: Tov = msgspec.field(name="TOV")
    games_played: str = msgspec.field(name="gamesPlayed")
    pts_away: str = msgspec.field(name="ptsAway")
    pts_home: str = msgspec.field(name="ptsHome")
    team_abv: str = msgspec.field(name="teamAbv")
    team_id: str = msgspec.field(name="teamID")


class WnbaTeam(msgspec.Struct, frozen=True):
    conference: str
    loss: str
    oppg: str
    ppg: str
    wins: str
    conference_abv: str = msgspec.field(name="conferenceAbv")
    current_streak: CurrentStreak = msgspec.field(name="currentStreak")
    defensive_stats: DefensiveStats = msgspec.field(name="defensiveStats")
    espn_logo_1: str = msgspec.field(name="espnLogo1")
    offensive_stats: OffensiveStats = msgspec.field(name="offensiveStats")
    team_abv: str = msgspec.field(name="teamAbv")
    team_city: str = msgspec.field(name="teamCity")
    team_id: str = msgspec.field(name="teamID")
    team_name: str = msgspec.field(name="teamName")


class WnbaTeamsResponse(msgspec.Struct, frozen=True):
    body: list[WnbaTeam]
    status_code: int = msgspec.field(name="statusCode")


async def get_wnba_teams(client: AsyncCachingClient) -> list[WnbaTeam]:
    url = f"https://{Env.WNBA_API_HOST}/getWNBATeams"

    data = await client.get(
        url,
        cache_key="index",
        ty=WnbaTeamsResponse,
        params={
            "rosters": "false",
            "teamStats": "true",
            "topPerformers": "false",
            "statsToGet": "averages",
            "schedules": "false",
        },
        headers={
            "x-rapidapi-host": Env.WNBA_API_HOST,
            "x-rapidapi-key": Env.WNBA_API_KEY,
        },
    )
    return data.body


class WnbaPlayer(msgspec.Struct, frozen=True):
    pos: str
    team: str
    long_name: str = msgspec.field(name="longName")
    player_id: str = msgspec.field(name="playerID")
    team_id: str = msgspec.field(name="teamID")


class WnbaPlayersResponse(msgspec.Struct, frozen=True):
    body: list[WnbaPlayer]
    status_code: int = msgspec.field(name="statusCode")


async def get_wnba_players(client: AsyncCachingClient) -> list[WnbaPlayer]:
    url = f"https://{Env.WNBA_API_HOST}/getWNBAPlayerList"

    data = await client.get(
        url,
        cache_key="index",
        ty=WnbaPlayersResponse,
        headers={
            "x-rapidapi-host": Env.WNBA_API_HOST,
            "x-rapidapi-key": Env.WNBA_API_KEY,
        },
    )
    return data.body


class Stats(msgspec.Struct, frozen=True):
    ast: str | None = None
    blk: str | None = None
    fga: str | None = None
    fgm: str | None = None
    fgp: str | None = None
    fta: str | None = None
    ftm: str | None = None
    ftp: str | None = None
    mins: str | None = None
    pts: str | None = None
    reb: str | None = None
    stl: str | None = None
    tptfga: str | None = None
    tptfgm: str | None = None
    tptfgp: str | None = None
    def_reb: str | None = msgspec.field(name="DefReb", default=None)
    off_reb: str | None = msgspec.field(name="OffReb", default=None)
    tov: str | None = msgspec.field(name="TOV", default=None)
    effective_shooting_percentage: str | None = msgspec.field(
        name="effectiveShootingPercentage", default=None
    )
    games_played: str | None = msgspec.field(name="gamesPlayed", default=None)
    true_shooting_percentage: str | None = msgspec.field(
        name="trueShootingPercentage", default=None
    )


class WnbaPlayerInfo(msgspec.Struct, frozen=True):
    exp: str
    injury: Injury
    pos: str
    school: str
    stats: Stats
    team: str
    weight: str
    espn_headshot: str = msgspec.field(name="espnHeadshot")
    espn_id: str = msgspec.field(name="espnID")
    espn_id_full: str = msgspec.field(name="espnIDFull")
    espn_link: str = msgspec.field(name="espnLink")
    espn_name: str = msgspec.field(name="espnName")
    jersey_num: str = msgspec.field(name="jerseyNum")
    long_name: str = msgspec.field(name="longName")
    player_id: str = msgspec.field(name="playerID")
    team_id: str = msgspec.field(name="teamID")
    age: str | None = None
    height: str | None = None
    b_day: str | None = msgspec.field(name="bDay", default=None)
    last_game_played: str | None = msgspec.field(name="lastGamePlayed", default=None)


class WnbaPlayerInfoResponse(msgspec.Struct, frozen=True):
    body: WnbaPlayerInfo
    status_code: int = msgspec.field(name="statusCode")


async def get_wnba_player_info(
    client: AsyncCachingClient, player_id: str
) -> WnbaPlayerInfo:
    url = f"https://{Env.WNBA_API_HOST}/getWNBAPlayerInfo"

    data = await client.get(
        url,
        cache_key=player_id,
        ty=WnbaPlayerInfoResponse,
        params={"playerID": player_id, "statsToGet": "averages"},
        headers={
            "x-rapidapi-host": Env.WNBA_API_HOST,
            "x-rapidapi-key": Env.WNBA_API_KEY,
        },
    )
    return data.body


class WnbaGame(msgspec.Struct, frozen=True):
    ast: str
    blk: str
    fga: str
    fgm: str
    fgp: str
    fta: str
    ftm: str
    ftp: str
    mins: str
    pts: str
    reb: str
    stl: str
    team: str
    tech: str
    tptfga: str
    tptfgm: str
    tptfgp: str
    def_reb: str = msgspec.field(name="DefReb")
    off_reb: str = msgspec.field(name="OffReb")
    pf: str = msgspec.field(name="PF")
    tov: str = msgspec.field(name="TOV")
    fantasy_points: str = msgspec.field(name="fantasyPoints")
    game_id: str = msgspec.field(name="gameID")
    long_name: str = msgspec.field(name="longName")
    player_id: str = msgspec.field(name="playerID")
    plus_minus: str = msgspec.field(name="plusMinus")
    team_abv: str = msgspec.field(name="teamAbv")
    team_id: str = msgspec.field(name="teamID")


class WnbaGamesForPlayerResponse(msgspec.Struct, frozen=True):
    body: dict[str, WnbaGame]
    status_code: int = msgspec.field(name="statusCode")


async def get_wnba_games_for_player(
    client: AsyncCachingClient, player_id: str
) -> dict[str, WnbaGame]:
    url = f"https://{Env.WNBA_API_HOST}/getWNBAGamesForPlayer"

    data = await client.get(
        url,
        cache_key=player_id,
        ty=WnbaGamesForPlayerResponse,
        params={
            "playerID": player_id,
            "fantasyPoints": "true",
        },
        headers={
            "x-rapidapi-host": Env.WNBA_API_HOST,
            "x-rapidapi-key": Env.WNBA_API_KEY,
        },
    )
    return data.body
