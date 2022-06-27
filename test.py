import pandas as pd

if __name__ == '__main__':
    ability = pd.read_csv('ability.csv')
    print(ability)

    print('run-away' in ability['name'].values)
