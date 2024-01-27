'''
    Filename: player_name_matchup.py
    Purpose: To generate a lookup file for the names used for players in the respective websites (RWC 2023 website, ESPN & Wikipedia).
'''

##################################################################################################################################
''' Initialising script '''
##################################################################################################################################
# Import packages
import os, sys
import pandas as pd
from pathlib import Path
from unidecode import unidecode

# Fixing pathway
module_path = os.path.abspath(os.path.join(''))
if module_path not in sys.path:
    sys.path.append(module_path)

# Output Directory
filepath = 'data/rwc_2023/'
Path(filepath).mkdir(parents=True, exist_ok=True)

# Import functinos
import scripts.useful_functions as uf



##################################################################################################################################
''' Import and clean data - RWC '''
##################################################################################################################################
# Download data
df_rwc = pd.read_csv(f'{filepath}player_data_rwc.csv')
df_rwc = df_rwc[['Player','Team']].drop_duplicates().rename(columns={'Player':'Name'}).reset_index(drop=True)

# Rename name column
df_rwc.rename(inplace=True,columns={'Name':'name_rwc','Team':'team'})

# Manual name fix
rwc_name_fix = {
    'Guido Petti Pagadizabal':'G. Petti',
    'David Wallis':'D. Carvalho',
    'Diogo Hasse Ferreira':'D. Ferreira',
    'James Lay':'J. Lay (James)',
    'Jordan Lay':'J. Lay (Jordan)',
    'Ulupano Junior Seuteni':'U. Seuteni',
    'Taleni Junior Agaese Seu':'T. Seu',
    'Sa Jordan Taufua':'J. Taufua',
    "Alai D'Angelo Leuila":'D. Leuila',
    'Salesi Piutau':'C. Piutau',
    'Halaleva Fifita':'L. Fifita',
    'Felipe Arcos Perez':'Felipe Perez',
    'Juan Manuel Rodríguez':'Juan Rodríguez',
    'Juan Manuel Alonso':'Juan Alonso',
    'Jeronimo de la Fuente':'Jeronimo Fuente'
}
df_rwc['name_rwc2'] = [rwc_name_fix.get(x,x) for x in df_rwc['name_rwc']]

# Create name_link column
df_rwc["name_link"] = [unidecode(x).split(" ")[0][0] + ". " + " ".join(unidecode(x).split(" ")[1:]) for x in df_rwc.name_rwc2]
df_rwc["name_link"] = df_rwc["name_link"].str.lower()
df_rwc.drop(inplace=True,columns='name_rwc2')



##################################################################################################################################
''' Import and clean data - ESPN'''
##################################################################################################################################
# Download data
df_espn = pd.read_csv(f'{filepath}player_data_espn.csv')
df_espn = df_espn[['name','team']].drop_duplicates().reset_index(drop=True)

# Rename name column
df_espn.rename(inplace=True,columns={'name':'name_espn'})

# Manual name fix
espn_name_fix = {
    'J Gonzalez':'J. Martin Gonzalez',
    'J Mallia':'J. Cruz Mallia',
    'F Kodela':'F. Gómez Kodela',
    'L Velez':'L. Bazan Velez',
    'L Luna':'L. Cinti',
    'J Larenas':'J. Ignacio Larenas',
    'J Albornoz':'J. Carrasco',
    'M Otero':'M. Torrealba',
    'T Cirikidaveta':'T. Ahiwaru Cirikidaveta',
    'J Flier':'J. Van Der Flier',
    'J Brex':'J. Ignacio Brex',
    'A Valu':'A. Ai Valu',
    'T Jaarsveld':'T. Van Jaarsveld',
    'J (Jnr)':'J. Deysel',
    'L Westhuizen':'L. Van Der Westhuizen',
    'T Klerk':'T. De Klerk',
    'A Berg':'A. Van Der Berg',
    'L Malan':'L. Roux Malan',
    'P Lill':'P. Van Lill',
    'E Groot':'E. De Groot',
    'N Guedes':'N. Sousa Guedes',
    'T Freitas':'T. De Freitas',
    'M Pinto':'M. Cardoso Pinto',
    'T Fonovai':'F. Tangimana',
    'N Wong':'N. Ah-Wong',
    'D Merwe':'D. Van Der Merwe',
    'P Toit':'P. Du Toit',
    'F Klerk':'F. De Klerk',
    'D Allende':'D. De Allende',
    'M Staden':'M. Van Staden',
    'W Roux':'W. Le Roux',
    'S Havili-Talitui':'S. Talitui',
    'S Havili':'S. Talitui',
    'F Pisano':'F. Berchesi',
    'G Lordon':'G. Kessler',
    'G Valente':'G. Mieres',
    'B Saavedra':'B. Amaya',
    'A Hontou':'A. Vilaseca',
    'T Rachetti':'T. Inciarte',
    'I Uria':'T. Inciarte',
    'M Olaso':'M. Diana',
    'L Bonfiglio':'L. Bianchi',
    'A Coetzee':'J Coetzee',
    'H Shifuka':'H Shikufa',
    'S Todua':'A Todua'
}
df_espn['name_espn2'] = [espn_name_fix.get(x,x) for x in df_espn['name_espn']]

# Create name_link column
df_espn["name_link"] = [unidecode(x).split(" ")[0][0] + ". " + " ".join(unidecode(x).split(" ")[1:]) for x in df_espn.name_espn2]
df_espn["name_link"] = df_espn["name_link"].str.lower()
df_espn.drop(inplace=True,columns='name_espn2')



##################################################################################################################################
''' Import and clean data - Wiki'''
##################################################################################################################################
# Download data
df_wiki = pd.read_csv(f'{filepath}match_data_wiki.csv')
df_wiki = pd.concat([
    df_wiki[['motm','team_home']].drop_duplicates().rename(columns={'team_home':'team'}),
    df_wiki[['motm','team_away']].drop_duplicates().rename(columns={'team_away':'team'}),
]).reset_index(drop=True)

# Rename name column
df_wiki.rename(inplace=True,columns={'motm':'name_wiki'})

# Create name_link column
df_wiki["name_link"] = [unidecode(x).split(" ")[0][0] + ". " + " ".join(unidecode(x).split(" ")[1:]) for x in df_wiki.name_wiki]
df_wiki["name_link"] = df_wiki["name_link"].str.lower()



##################################################################################################################################
''' Create lookup '''
##################################################################################################################################
# Start with Fantasy & Rugby Pass
lookup = uf.matching(df_rwc,df_espn)
# Match up to wiki
lookup = uf.matching(lookup,df_wiki)
# The ones that didn't match can just be removed (it's like 3 people so who cares)
lookup = lookup[(lookup.name_rwc.notnull()) & (lookup.name_espn.notnull())]
# Save to file
lookup = lookup[['name_rwc','name_espn','name_wiki','team']]
lookup.to_csv(f'{filepath}player_lookup.csv',index=False)
