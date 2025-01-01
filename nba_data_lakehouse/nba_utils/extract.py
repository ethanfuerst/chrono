import logging
import os
import sys
from typing import List

from .endpoint_config import ENDPOINT_EXTRACTOR_MAP, NBADataExtractor


def get_endpoint_class(
    endpoint: str, seasons: List[str] = ['2024']
) -> NBADataExtractor:
    try:
        extractor_class = ENDPOINT_EXTRACTOR_MAP[endpoint]

        return extractor_class(seasons, endpoint)
    except KeyError:
        raise ValueError(f'Unknown endpoint: {endpoint}')
