import pandas as pd
from pokemon_database import PokemonDatabase


# task 4 function
def count_pokemons_abilities_by_generation(GenrationAbilites: pd.DataFrame):
    """
    SQL style:

    select generation_name, count(pokemon_name) as pokemon_name_count
            from GenrationAbilites
            where pokemon_name is not null
            group by 1

    """
    not_null_pokemon = GenrationAbilites[GenrationAbilites['pokemon_name'].notnull()]

    count_pokemons = not_null_pokemon.groupby(['generation_name'])['pokemon_name'].agg('count').reset_index()
    count_pokemons.columns = ['generation_name', 'pokemon_name_count']

    count_pokemons.to_csv('count.csv', index=False)


def run():
    # task 2 - see get_pokemon_data function in pokemon_database.py -> PokemonDatabase class
    pokemon_database = PokemonDatabase()

    # task 3 - see denormalize_tables function in pokemon_database.py -> PokemonDatabase class
    pokemon_database.get_pokemon_data(pokemons_name=['bulbasaur', 'caterpie', 'raticate'])

    GenrationAbilites = pokemon_database.denormalize_tables(
        new_columns_name=['generation_name', 'ability_name', 'ability_is_main_series', 'pokemon_name',
                          'pokemon_base_experience', 'pokemon_height'],
        attributes_to_save=['name_generation', 'ability_name', 'is_main_series', 'name', 'base_experience',
                            'height'])

    GenrationAbilites.to_csv('GenrationAbilites.csv', index=False)

    # task 4
    count_pokemons_abilities_by_generation(GenrationAbilites=GenrationAbilites)


def main():
    run()


main()



