import os
import pandas as pd
from api import API
from database import Database


class PokemonDatabase(Database):

    def __init__(self):
        super().__init__('./PokemonDB')
        self.set_up()
        self.api = API("https://pokeapi.co/api/v2/")

    def set_up(self):
        """
        setup this pokemon database
        """
        if not os.path.exists(self.database_path):
            # mkdir
            os.makedirs(self.database_path)

            self.create_table(table_name='pokemon',
                              columns=['pokemon_id', 'name', 'base_experience', 'height'], index='pokemon_id')
            self.create_table(table_name='ability',
                              columns=['ability_id', 'name', 'is_main_series'], index='ability_id')
            self.create_table(table_name='pokemons_abilities', columns=['id', 'pokemon_id', 'ability_name'])
            self.create_table(table_name='generation', columns=['generation_id', 'name'], index='generation_id')
            self.create_table(table_name='generation_abilities', columns=['id', 'generation_id', 'ability_name'])

        else:
            self.load_database(tables_name=['pokemon', 'ability', 'pokemons_abilities', 'generation', 'generation_abilities'])

    def fetch_and_save_pokemon(self, pokemons_name: list) -> set:
        """
        :param pokemons_name: list-like
        :return: set of abilities to fetch as next step
        """
        abilities_set = set()
        # fetch pokemon
        for response in self.fetch(api=self.api, endpoint='pokemon', check_item='name', items_to_fetch=pokemons_name,
                                   table_target='pokemon'):
            # save pokemon
            columns = self.tables['pokemon'].columns
            attributes_to_insert = {col: response[col] for col in columns}
            abilities = self.save(table_target='pokemon', row_to_insert=attributes_to_insert,
                                  attributes_to_normalize=
                                  {'abilities': (response['abilities'], ['ability', 'name'], 'pokemons_abilities')})
            # keep ability to fetch
            abilities_set.update(abilities)
        return abilities_set

    def fetch_and_save_pokemon_abilities(self, abilities) -> set:
        """
        :param abilities: list-like
        :return: set of generations to fetch as next step
        """
        # fetch abilities
        generations_set = set()
        for response in self.fetch(api=self.api, endpoint='ability', check_item='name', items_to_fetch=abilities,
                                   table_target='ability'):
            # save ability
            columns = self.tables['ability'].columns
            attributes_to_insert = {col: response[col] for col in columns}
            self.save(table_target='ability', row_to_insert=attributes_to_insert)
            # keep generation to fetch
            ability_generation = response['generation']['name']
            generations_set.add(ability_generation)
        return generations_set

    def fetch_and_save_generations(self, generations):
        """
        :param generations: list-like
        """
        # fetch generation
        for response in self.fetch(api=self.api, endpoint='generation', check_item='name', items_to_fetch=generations,
                                   table_target='generation'):
            # save generation
            columns = self.tables['generation'].columns
            attributes_to_insert = {col: response[col] for col in columns}
            self.save(table_target='generation', row_to_insert=attributes_to_insert,
                      attributes_to_normalize={'abilities': (response['abilities'], ['name'], 'generation_abilities')})

    # task 2
    def get_pokemon_data(self, pokemons_name: list):
        """
        :param pokemons_name:
        :return: PokemonDatabase
        """
        # fetch_and_save_pokemon
        abilities_set = self.fetch_and_save_pokemon(pokemons_name=pokemons_name)

        # fetch_and_save_pokemon_abilities
        generations_set = self.fetch_and_save_pokemon_abilities(abilities=abilities_set)

        # fetch generation
        self.fetch_and_save_generations(generations=generations_set)

        # upload database
        self.upload_database()

        return self

    # task 3
    def denormalize_tables(self, new_columns_name: list, attributes_to_save: list) -> pd.DataFrame:
        """
        joining all database table (denormalize_tables)
        :param new_columns_name:
        :param attributes_to_save:
        :return: pd.DataFrame of joining result
        """

        generation_abilities = self.tables['generation_abilities'].merge(self.tables['generation'], how='left',
                                                                         on='generation_id')

        existing_abilities = generation_abilities.merge(self.tables['ability'], how='left',
                                                        left_on=['ability_name'],
                                                        right_on=['name'],
                                                        suffixes=('_generation', '_pokemon'))

        temp = existing_abilities.merge(self.tables['pokemons_abilities'], how='left', on='ability_name')

        final = temp.merge(self.tables['pokemon'].reset_index(), how='left',
                           on='pokemon_id')

        final = final[attributes_to_save]
        final.columns = new_columns_name

        return final



