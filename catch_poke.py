import json
from typing import List
from API_Call import API
import pandas as pd
from database import Database

def not_exist(table, keys, attributes):
    for key, attribute in zip(keys, attributes):
        if attribute in table[key].values:
            return False
    return True


def get_pokemon_data(pokemons_name: List):
    api = API("https://pokeapi.co/api/v2/")

    # create database
    database = Database()

    # create tables
    database.create_table(table_name='pokemon',
                          columns=['pokemon_id', 'name', 'base_experience', 'height'], index='pokemon_id')
    database.create_table(table_name='ability',
                          columns=['ability_id', 'name', 'generation_name', 'is_main_series'], index='ability_id')
    database.create_table(table_name='pokemons_abilities', columns=['id', 'pokemon_id', 'ability_id'])
    database.create_table(table_name='generation', columns=['generation_id', 'name'], index='generation_id')
    database.create_table(table_name='generation_abilities',columns=['id', 'generation_name', 'ability_name'])

    # fetch pokemon
    pokemon_generator = api.multiple_get_api_call(endpoint="pokemon", ids_or_names=pokemons_name)

    for pokemon in pokemon_generator:

        # with open("pokemons_raw_data.txt", "a+") as pokemons_raw_data:
        #     pokemons_raw_data.write(json.dumps(pokemon) + '\n')

        # pokemon exist in DB ?
        if database.not_exist(table_name='pokemon', columns=['name'], attributes=[pokemon['name']]):

            # add to pokemon table
            columns = database.tables['pokemon'].columns
            pokemon_index = database.insert_row(table_name='pokemon', row={col: pokemon[col] for col in columns})

            for pokemon_ability in pokemon['abilities']:
                # fetch ability by pokemon ability
                ability = api.get_api_call(endpoint='ability', id_or_name=pokemon_ability['ability']['name'])

                # with open("ability_raw_data.txt", "a+") as ability_raw_data:
                #     ability_raw_data.write(json.dumps(ability) + '\n')

                ability_index = None

                # ability exist in DB ?
                if database.not_exist(table_name='ability', columns=['name'], attributes=[ability['name']]):
                    # add to ability table
                    columns = database.tables['ability'].columns
                    row = {col: ability[col] if col != 'generation_name' else ability['generation']['name']
                           for col in columns}
                    ability_index = database.insert_row(table_name='ability', row=row)

                    # generation exist in DB ?
                    generation_name = ability['generation']['name']
                    if database.not_exist(table_name='generation', columns=['name'], attributes=[generation_name]):
                        # add to generation table
                        database.insert_row(table_name='generation', row=[generation_name])

                        # fetch generation by ability generation
                        generation = api.get_api_call('generation', generation_name)

                        # with open("generation_raw_data.txt", "a+") as generation_raw_data:
                        #     generation_raw_data.write(json.dumps(generation) + '\n')

                        for generation_ability in generation['abilities']:
                            # add to generation_to_ability_table
                            generation_ability_name = generation_ability['name']
                            database.insert_row(table_name='generation_abilities', row=[generation_name, generation_ability_name])

                # add to pokemons_abilities table , if ability already exist takes its id
                ability_index = ability_index if ability_index \
                    else database.tables['ability'][database.tables['ability'] == ability['name']].index.values[0]
                database.insert_row(table_name='pokemons_abilities', row=[pokemon_index, ability_index])

    database.tables['pokemon'].to_csv('pokemons.csv')
    database.tables['ability'].to_csv('ability.csv')
    database.tables['pokemons_abilities'].to_csv('pokemons_abilities.csv')
    database.tables['generation'].to_csv('generation.csv')
    database.tables['generation_abilities'].to_csv('generation_abilities.csv')


if __name__ == '__main__':
    import time

    start = time.time()

    get_pokemon_data(pokemons_name=['bulbasaur', 'caterpie', 'raticate'])

    end = time.time()

    print(end - start)
