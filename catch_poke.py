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
    pokemons_abilities_table = pd.DataFrame(columns=['pokemon_id', 'ability_id'])
    generation_table = pd.DataFrame(columns=['generation_id', 'name']).set_index('generation_id')

    pokemon_generator = api.multiple_get_api_call("pokemon", pokemons_name)

    for pokemon in pokemon_generator:
        with open("pokemons_raw_data.txt", "a+") as pokemons_raw_data:
            pokemons_raw_data.write(json.dumps(pokemon) + '\n')

        # should check if exist
        if not_exist(pokemon_table, ['name'], [pokemon['name']]):

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
                    ability_index = len(ability_table)
                    columns = ability_table.columns
                    row = {col: ability[col] if col != 'generation_name' else ability['generation']['name'] for col in columns}
                    ability_table.loc[ability_index] = row

                    # add to generation if not exist
                    if not_exist(generation_table, ['name'], [ability['generation']['name']]):
                        generation_table.loc[len(generation_table)] = [ability['generation']['name']]

                # add to pokemons_abilities_table , if ability already exist takes its id
                ability_index = ability_index if ability_index \
                    else ability_table[ability_table['name'] == ability['name']].index.values[0]
                pokemons_abilities_table.loc[len(pokemons_abilities_table)] = [pokemon_index, ability_index]

    pokemon_table.to_csv('pokemons.csv')
    ability_table.to_csv('ability.csv')
    pokemons_abilities_table.to_csv('pokemons_abilities_table.csv', index=False)
    generation_table.to_csv('generation_table.csv')


if __name__ == '__main__':
    import time

    start = time.time()

    get_pokemon_data(pokemons_name=['bulbasaur', 'caterpie', 'raticate'])

    end = time.time()

    print(end - start)