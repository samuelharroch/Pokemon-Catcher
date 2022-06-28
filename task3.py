import pandas as pd


def denormalize_tables(new_columns_name: list, attributes_to_save: list) -> pd.DataFrame:

    pokemon_table = pd.read_csv('pokemons.csv')
    ability_table = pd.read_csv('ability.csv')
    pokemons_abilities_table = pd.read_csv('pokemons_abilities_table.csv')
    generation_to_ability_table = pd.read_csv('generation_to_ability_table.csv')

    existing_abilities = generation_to_ability_table.merge(ability_table, how='left',
                                                           left_on=['ability_name', 'generation_name'],
                                                           right_on=['name', 'generation_name'])

    temp = existing_abilities.merge(pokemons_abilities_table, how='left',
                                    on='ability_id')

    final = temp.merge(pokemon_table, how='left',
                       on='pokemon_id',
                       suffixes=('_ability', '_pokemon'))

    final = final[attributes_to_save]
    final.columns = new_columns_name

    return final


if __name__ == '__main__':
    GenrationAbilites = denormalize_tables(['generation_name', 'ability_name', 'ability_is_main_series',
                                            'pokemon_name', 'pokemon_base_experience', 'pokemon_height'],
                                           ['generation_name', 'ability_name', 'is_main_series',
                                            'name_pokemon', 'base_experience', 'height'])

    GenrationAbilites.to_csv('GenrationAbilites.csv', index=False)
