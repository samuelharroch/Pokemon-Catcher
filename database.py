import pandas as pd


class Database:

    def __init__(self):
        self.tables = {}

    def create_table(self, table_name: str, columns: list, index: str = 'id'):
        self.tables[table_name] = pd.DataFrame(columns=columns).set_index(index)

    def not_exist(self, table_name: str, columns: list, attributes: list):
        for col, attribute in zip(columns, attributes):
            if attribute in self.tables[table_name].values:
                return False
        return True

    def insert_row(self, table_name: str, row):

        next_index = len(self.tables[table_name])
        self.tables[table_name].loc[next_index] = row
        return next_index
