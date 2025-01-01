from collections import namedtuple
from typing import Generator, List, Tuple

from nba_api.stats.endpoints import playergamelogs, playerindex, teamgamelogs
from pandas import DataFrame

SEASON_TYPE_MAP = {
    'Regular Season': 'regular_season',
    'Playoffs': 'playoffs',
}

SeasonInfo = namedtuple('SeasonInfo', ['season', 'formatted_season'])


class NBADataExtractor:
    def __init__(self, seasons: List[str], endpoint: str):
        self.seasons = seasons
        self.endpoint = endpoint

    def get_data(self) -> Generator[Tuple[str, DataFrame], None, None]:
        raise NotImplementedError('Subclasses must implement get_data method')

    def _get_seasons(self) -> Generator[SeasonInfo, None, None]:
        for season in self.seasons:
            formatted_season = f'{season}-{str(int(season) + 1)[2:]}'
            yield SeasonInfo(season, formatted_season)


class TeamGameLogsExtractor(NBADataExtractor):
    def get_data(self) -> Generator[Tuple[str, DataFrame], None, None]:
        for season_info in self._get_seasons():
            for season_type in SEASON_TYPE_MAP.keys():
                s3_key = f'{self.endpoint}/season={season_info.season}/season_type={SEASON_TYPE_MAP[season_type]}'

                endpoint = teamgamelogs.TeamGameLogs(
                    season_nullable=season_info.formatted_season,
                    season_type_nullable=season_type,
                )

                df = endpoint.get_data_frames()[0]

                if not df.empty:
                    yield s3_key, df


class PlayerGameLogsExtractor(NBADataExtractor):
    def get_data(self) -> Generator[Tuple[str, DataFrame], None, None]:
        for season_info in self._get_seasons():
            for season_type in SEASON_TYPE_MAP.keys():
                s3_key = f'{self.endpoint}/season={season_info.season}/season_type={SEASON_TYPE_MAP[season_type]}'

                endpoint = playergamelogs.PlayerGameLogs(
                    season_nullable=season_info.formatted_season,
                    season_type_nullable=season_type,
                )

                df = endpoint.get_data_frames()[0]

                if not df.empty:
                    yield s3_key, df


class PlayerIndexExtractor(NBADataExtractor):
    def get_data(self) -> Generator[Tuple[str, DataFrame], None, None]:
        for season_info in self._get_seasons():
            table_name = f'{self.endpoint}/season={season_info.season}'

            endpoint = playerindex.PlayerIndex(season=season_info.formatted_season)

            df = endpoint.get_data_frames()[0]

            if not df.empty:
                yield table_name, df


ENDPOINT_EXTRACTOR_MAP = {
    'teamgamelogs': TeamGameLogsExtractor,
    'playergamelogs': PlayerGameLogsExtractor,
    'playerindex': PlayerIndexExtractor,
}
