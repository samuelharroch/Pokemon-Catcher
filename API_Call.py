import json
from typing import List, Generator
import requests


class API:

    def __init__(self, api_base: str):
        self.api_base = api_base

    def get_api_call(self, endpoint: str, id_or_name: str) -> json:
        full_path = self.api_base + '/'.join([endpoint, id_or_name, ''])
        return requests.get(url=full_path).json()

    # consolidate with generator
    def multiple_get_api_call(self, endpoint: str, ids_or_names: List[str]) -> json:

        for id_or_name in ids_or_names:
            yield self.get_api_call(endpoint=endpoint, id_or_name=id_or_name)
