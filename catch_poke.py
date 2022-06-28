import json
from typing import List
from API_Call import API
import pandas as pd


def not_exist(table, keys, attributes):
    for key, attribute in zip(keys, attributes):
        if attribute in table[key].values:
            return False
    return True


def get_pokemon_data(pokemons_name: List):
    api = API("https://pokeapi.co/api/v2/")

    pokemon_table = pd.DataFrame(columns=['pokemon_id', 'name', 'base_experience', 'height']).set_index('pokemon_id')
    ability_table = pd.DataFrame(columns=['ability_id', 'name', 'generation_name', 'is_main_series']) \
        .set_index('ability_id')
    pokemons_abilities_table = pd.DataFrame(columns=['id', 'pokemon_id', 'ability_id']).set_index('id')
    generation_table = pd.DataFrame(columns=['generation_id', 'name']).set_index('generation_id')
    generation_to_ability_table = pd.DataFrame(columns=['id', 'generation_name', 'ability_name']).set_index('id')

    # fetch pokemon
    pokemon_generator = api.multiple_get_api_call("pokemon", pokemons_name)

    for pokemon in pokemon_generator:

        with open("pokemons_raw_data.txt", "a+") as pokemons_raw_data:
            pokemons_raw_data.write(json.dumps(pokemon) + '\n')

        # should check if exist
        if not_exist(pokemon_table, ['name'], [pokemon['name']]):
            # add to pokemon table
            columns = pokemon_table.columns
            row = {col: pokemon[col] for col in columns}
            pokemon_index = len(pokemon_table)
            pokemon_table.loc[pokemon_index] = row

            for pokemon_ability in pokemon['abilities']:
                ability = api.get_api_call('ability', pokemon_ability['ability']['name'])

                with open("ability_raw_data.txt", "a+") as ability_raw_data:
                    ability_raw_data.write(json.dumps(ability) + '\n')

                ability_index = None

                # should check if exist
                if not_exist(ability_table, ['name'], [ability['name']]):
                    # add to ability_table
                    columns = ability_table.columns
                    row = {col: ability[col] if col != 'generation_name' else ability['generation']['name']
                           for col in columns}
                    ability_index = len(ability_table)
                    ability_table.loc[ability_index] = row

                    # add to generation if not exist
                    if not_exist(generation_table, ['name'], [ability['generation']['name']]):
                        # add to generation_table
                        generation_name = ability['generation']['name']
                        generation_table.loc[len(generation_table)] = [generation_name]

                        generation = api.get_api_call('generation', generation_name)

                        with open("generation_raw_data.txt", "a+") as generation_raw_data:
                            generation_raw_data.write(json.dumps(generation) + '\n')

                        for generation_ability in generation['abilities']:
                            # add to generation_to_ability_table
                            generation_ability_name = generation_ability['name']
                            generation_to_ability_table.loc[len(generation_to_ability_table)] = \
                                [generation_name, generation_ability_name]

                # add to pokemons_abilities_table , if ability already exist takes its id
                ability_index = ability_index if ability_index \
                    else ability_table[ability_table['name'] == ability['name']].index.values[0]
                pokemons_abilities_table.loc[len(pokemons_abilities_table)] = [pokemon_index, ability_index]

    pokemon_table.to_csv('pokemons.csv')
    ability_table.to_csv('ability.csv')
    pokemons_abilities_table.to_csv('pokemons_abilities_table.csv')
    generation_table.to_csv('generation_table.csv')
    generation_to_ability_table.to_csv('generation_to_ability_table.csv')


if __name__ == '__main__':
    import time

    start = time.time()

    get_pokemon_data(pokemons_name=['bulbasaur', 'caterpie', 'raticate'])

    end = time.time()

    print(end - start)
