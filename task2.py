
from typing import List
from api import API
from pokemon_database import PokemonDatabase


def get_pokemon_data(pokemons_name: List):
    api = API("https://pokeapi.co/api/v2/")

    # create database and set_up
    database = PokemonDatabase()

    abilities_set = set()
    # fetch pokemon
    for response in database.fetch(api=api, endpoint='pokemon', check_item='name',
                                   element_to_fetch=pokemons_name, table_target='pokemon'):
        # save pokemon
        columns = database.tables['pokemon'].columns
        attributes_to_insert = {col: response[col] for col in columns}
        abilities = database.save(table_target='pokemon', attributes_to_insert=attributes_to_insert,
                                  attributes_to_normalize=
                                  {'abilities': (response['abilities'], ['ability', 'name'], 'pokemons_abilities')})
        # keep ability to fetch
        abilities_set.update(abilities)

    # fetch abilities
    generations_set = set()
    for response in database.fetch(api=api, endpoint='ability', check_item='name',
                                   element_to_fetch=abilities_set, table_target='ability'):
        # save ability
        columns = database.tables['ability'].columns
        attributes_to_insert = {col: response[col] for col in columns}
        database.save(table_target='ability', attributes_to_insert=attributes_to_insert)
        # keep generation to fetch
        ability_generation = response['generation']['name']
        generations_set.add(ability_generation)

    # fetch generation
    for response in database.fetch(api=api, endpoint='generation', check_item='name',
                                   element_to_fetch=generations_set, table_target='generation'):
        # save generation
        columns = database.tables['generation'].columns
        attributes_to_insert = {col: response[col] for col in columns}
        database.save(table_target='generation', attributes_to_insert=attributes_to_insert,
                      attributes_to_normalize={'abilities': (response['abilities'], ['name'], 'generation_abilities')})

    # upload database
    database.upload_database()

    return database

if __name__ == '__main__':
    import time

    start = time.time()

    get_pokemon_data(pokemons_name=['bulbasaur', 'caterpie', 'raticate'])

    end = time.time()

    print(end - start)

