'''
    Filename: player_scoring.py
    Purpose: To apply the 6 Nations 2024 points system to the 2023 RWC dataset.
'''

##################################################################################################################################
''' Initialising script '''
##################################################################################################################################
# Import packages
import os, sys
import pandas as pd
from math import floor
from pathlib import Path

# Fix pathway
module_path = os.path.abspath(os.path.join(''))
if module_path not in sys.path:
    sys.path.append(module_path)

# Output Directory
filepath = 'data/rwc_2023/'
Path(filepath).mkdir(parents=True, exist_ok=True)

# Import functinos
import scripts.useful_functions as uf



##################################################################################################################################
''' Useful variables '''
##################################################################################################################################
# Define rules
scoring = {
    'try' : 10,
    'try_assist' : 4,
    'conversion' : 2,
    'penalty' : 3,
    'drop_goal' : 5,
    'defenders_beaten' : 2,
    'meters_made' : 1,
    #'50-22' : 7,
    'tackles' : 1,
    'breakdown_steals' : 5,
    'lineout_steal' : 7,
    'penalties_conceded' : -1,
    'motm' : 15,
    'yellow_card' : -3,
    'red_card' : -6,
}

# Import lookup
lkup = pd.read_csv(f'{filepath}player_lookup.csv')
lkup = lkup[['name_rwc','name_espn','team']]



##################################################################################################################################
''' ESPN data '''
##################################################################################################################################
# Download data
df_espn = pd.read_csv(f'{filepath}player_data_espn.csv')

# Map player names
cols=['name','team']
df_espn = uf.join(lkup.rename(columns={'name_espn':'name'}),cols,df_espn,cols)

# Clean/filter columns
df_espn = df_espn[[
    'date',
    'team',
    'opposition',
    'name_rwc',
    'try',
    'try_assist',
    'conversion',
    'penalty',
    'drop_goal',
    'defenders_beaten',
    'meters_made',
    'tackles',
    'penalties_conceded',
    'yellow_card',
    'red_card'
]]
df_espn.rename(inplace=True,columns={'name_rwc':'name'})



##################################################################################################################################
''' RWC Data '''
##################################################################################################################################
# Download data
df_rwc = pd.read_csv(f'{filepath}player_data_rwc.csv')

# Clean/filter columns
df_rwc = df_rwc[[
    'Date',
    'Team',
    'Opposition',
    'Player',
    'No.',
    'turnovers',
    'lineout_steal',
]]
df_rwc.columns = [x.lower() for x in df_rwc.columns]
df_rwc.rename(inplace=True, columns={'player':'name'})



##################################################################################################################################
''' Wiki data '''
##################################################################################################################################
# Download
df_wiki = pd.read_csv(f'{filepath}match_data_wiki.csv')

# Clean/filter columns
df_wiki = df_wiki[['date','motm']].rename(columns={'motm':'name'})
df_wiki['motm'] = 1



##################################################################################################################################
''' Combine Data '''
##################################################################################################################################
# Merge data
cols = ['date','team','opposition','name']
dataset = uf.join(df_rwc,cols,df_espn,cols)
dataset = uf.join(dataset,['date','name'],df_wiki,['date','name'],out='left_join')
dataset['motm'] = dataset['motm'].fillna('0')

# Fix column names
col_fix = {
    'turnovers':'breakdown_steals',
    'defender_beaten':'defenders_beaten',
}
dataset.columns = [col_fix.get(x,x) for x in dataset.columns]

# Save dataset before applying the scores
dataset.to_csv(f'{filepath}player_data.csv',index=False)

# Manipulate the metres made & motm column
dataset['meters_made'] = [floor(int(x)/10) for x in dataset['meters_made']]

# Collapse
dataset = pd.melt(dataset, id_vars=dataset.columns.tolist()[0:5], value_vars=dataset.columns.tolist()[5:])
dataset.value = [int(str(x).replace('-','0').split('.')[0]) for x in dataset.value]

# Score
cols = cols + ['no.']
dataset['scoring'] = [score * scoring.get(x,0) for score,x in zip(dataset.value,dataset.variable)]
totals = dataset.groupby(cols).scoring.sum().reset_index()
totals['variable'] = 'total'
dataset = pd.concat([totals,dataset.drop(columns='value')]).reset_index(drop=True)

# Tidy
dataset = pd.pivot_table(dataset,values='scoring', index=dataset.columns.tolist()[0:5],columns='variable').reset_index()
dataset = dataset[cols + ['total'] + [x for x,y in scoring.items()]]

# Save
dataset.to_csv(f'{filepath}fantasy_scores.csv',index=False)