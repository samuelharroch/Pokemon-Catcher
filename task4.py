import pandas as pd

"""
SQL style: 

select generation_name, count(pokemon_name) as pokemon_name_count
        from GenrationAbilites
        where pokemon_name is not null
        group by 1 
        
"""


def count_pokemons_abilities_by_generation():

    GenrationAbilites = pd.read_csv('GenrationAbilites.csv')

    not_null_pokemon = GenrationAbilites[GenrationAbilites['pokemon_name'].notnull()]

    count_pokemons = not_null_pokemon.groupby(['generation_name'])['pokemon_name'].agg('count').reset_index()
    count_pokemons.columns = ['generation_name', 'pokemon_name_count']

    count_pokemons.to_csv('count_pokemons_by_generation.csv', index=False)


if __name__ == '__main__':
    count_pokemons_abilities_by_generation()
