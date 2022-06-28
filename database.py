import json
import os

import pandas as pd
from api import API


def nested_field(nested_object, path: list):
    """
    >>> obj = {"name": "pickpocket","url": "https://pokeapi.co/api/v2/ability/124/"}
    >>> nested_field(obj,['name'])
    'pickpocket'
    >>> obj = {"ability": {"name": "overgrow","url": "https://pokeapi.co/api/v2/ability/65/"}}
    >>> nested_field(obj,['ability', 'name'])
    'overgrow'
    """
    if not len(path):
        return nested_object

    return nested_field(nested_object[path[0]], path[1:])


class Database:

    def __init__(self, database_path: str):
        self.tables = {}
        self.database_path = database_path
        self.raw_data_dir_path = './RawData'

        if not os.path.exists(self.raw_data_dir_path):
            # mkdir
            os.makedirs(self.raw_data_dir_path)

    def create_table(self, table_name: str, columns: list, index: str = 'id'):
        self.tables[table_name] = pd.DataFrame(columns=columns).set_index(index)

    def load_database(self, tables_name: list, files_type: str = '.csv'):
        for table_name in tables_name:
            table_path = self.database_path + '/' + table_name + files_type
            self.tables[table_name] = pd.read_csv(table_path, index_col=0)

    def upload_database(self, files_type: str = '.csv'):
        for table_name in self.tables.keys():
            self.tables[table_name].to_csv(self.database_path + '/' + table_name + files_type)

    def not_exist(self, table_name: str, check_list: dict):
        for col, value in check_list.items():
            if value in self.tables[table_name].values:
                return False
        return True

    def insert_row(self, table_name: str, row):

        next_index = len(self.tables[table_name])
        self.tables[table_name].loc[next_index] = row
        return next_index

    def fetch(self, api: API, endpoint: str, check_item: str, element_to_fetch, table_target: str):
        for item in element_to_fetch:
            # exist in DB ?
            if self.not_exist(table_name=table_target, check_list={check_item: item}):
                # fetch
                response = api.get_api_call(endpoint=endpoint, id_or_name=item)
                # insert in raw data
                with open(self.raw_data_dir_path + '/' + table_target+"_raw_data.txt", "a+") as pokemons_raw_data:
                    pokemons_raw_data.write(json.dumps(response) + '\n')

                yield response

    def save(self, table_target: str, attributes_to_insert: dict, attributes_to_normalize: dict = None):
        # add to table
        new_index = self.insert_row(table_name=table_target, row=attributes_to_insert)

        if attributes_to_normalize:
            next_to_fetch = set()
            for attribute_to_normalize, path, table in attributes_to_normalize.values():
                for attribute in attribute_to_normalize:
                    # add to normalizer table
                    field = nested_field(attribute, path)
                    self.insert_row(table_name=table, row=[new_index, field])

                    next_to_fetch.add(field)
            return next_to_fetch
