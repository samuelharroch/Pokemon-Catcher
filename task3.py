from pokemon_database import PokemonDatabase
from task2 import get_pokemon_data

if __name__ == '__main__':

    pokemon_database = get_pokemon_data(pokemons_name=['bulbasaur', 'caterpie', 'raticate'])

    GenrationAbilites = pokemon_database.denormalize_tables(
        new_columns_name=['generation_name', 'ability_name', 'ability_is_main_series','pokemon_name',
                          'pokemon_base_experience', 'pokemon_height'],
        attributes_to_save=['name_generation', 'ability_name', 'is_main_series', 'name', 'base_experience',
                            'height'])

    GenrationAbilites.to_csv('GenrationAbilites.csv', index=False)
