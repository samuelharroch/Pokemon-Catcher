import json
import os
import pandas as pd
from api import API


def nested_field(nested_object, path: list):
    """
    :return inner object from nested_object according to path

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
        """
        :param table_name:
        :param columns:
        :param index: index column name
        """
        self.tables[table_name] = pd.DataFrame(columns=columns).set_index(index)

    def load_database(self, tables_name: list, files_type: str = '.csv'):
        """
        :param tables_name: tables_name to load
        :param files_type: file type to load
        :return: None , load data in object
        """
        for table_name in tables_name:
            table_path = self.database_path + '/' + table_name + files_type
            self.tables[table_name] = pd.read_csv(table_path, index_col=0)

    def upload_database(self, files_type: str = '.csv'):
        """
        upload/update data to database directory
        :param files_type: file type to save
        :return:
        """
        for table_name in self.tables.keys():
            self.tables[table_name].to_csv(self.database_path + '/' + table_name + files_type)

    def not_exist(self, table_name: str, check_list: dict):
        """
        Check if all the check_list fields exist in table_name of database
        :param table_name:
        :param check_list: {column_name: value_to_check}
        :return: True if not exist, else False
        """
        for col, value in check_list.items():
            if value in self.tables[table_name].values:
                return False
        return True

    def insert_row(self, table_name: str, row):
        """
        insert row in table_name
        :param table_name:
        :param row: row to insert
        :return: the index of the new row
        """
        next_index = len(self.tables[table_name])
        self.tables[table_name].loc[next_index] = row
        return next_index

    def fetch(self, api: API, endpoint: str, check_item: str, items_to_fetch, table_target: str):
        """
        :param api: API to fetch from
        :param endpoint: endpoint of the api
        :param check_item: item to check if exist in table_target (base on PK or AK - keys)
        :param items_to_fetch: list-like
        :param table_target: table to check (check if exist before inserting)
        :return: generator of api responses
        """
        for item in items_to_fetch:
            # exist in DB ?
            if self.not_exist(table_name=table_target, check_list={check_item: item}):
                # fetch
                response = api.get_api_call(endpoint=endpoint, id_or_name=item)
                # insert in raw data
                with open(self.raw_data_dir_path + '/' + table_target+"_raw_data.txt", "a+") as pokemons_raw_data:
                    pokemons_raw_data.write(json.dumps(response) + '\n')

                yield response

    def save(self, table_target: str, row_to_insert: dict, attributes_to_normalize: dict = None):
        """
        save the row_to_insert in database ( and return next fields to fetch if needed)
        :param table_target: table to insert
        :param row_to_insert:
        :param attributes_to_normalize: dict of {attribute_name: (attribute_obj, path_to_final_attribute, normalizer_table_to_insert)}
        If None just insert row_to_insert in table_target ,
        otherwise also insert in normalizer_table_to_insert according to attributes_to_normalize
        :return: next field to fetch according to the normalized attributes
        """
        # add to table
        new_index = self.insert_row(table_name=table_target, row=row_to_insert)

        if attributes_to_normalize:
            next_to_fetch = set()
            for attribute_to_normalize, path, table in attributes_to_normalize.values():
                for attribute in attribute_to_normalize:
                    # add to normalizer table
                    field = nested_field(attribute, path)
                    self.insert_row(table_name=table, row=[new_index, field])

                    next_to_fetch.add(field)
            return next_to_fetch
