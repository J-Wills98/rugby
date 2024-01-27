'''
    Filename: data_scraping.py
    Purpose: To scrape the RWC 2023 data from the following websites: RWC 2023 website, ESPN & Wikipedia.
'''

##################################################################################################################################
''' Initialise script '''
##################################################################################################################################
# Instiall packages
import os, sys
import pandas as pd
from pathlib import Path

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
''' Functions to scrape each website '''
##################################################################################################################################
# https://www.rugbyworldcup.com
def scrape_rwc():
    # Get fixtures
    url = "https://www.rugbyworldcup.com/2023/matches"
    print(f'\nStart scraping the RWC 2023 website')
    soup = uf.web_scrape(url)
    fixtures = soup.find_all('a',class_='button button--maintain-desktop button--match-centre')
    fixtures = ['https:' + x['href'] + '#stats' for x in fixtures]
    fixtures = [x for x in fixtures if '28791' not in x]
    fixtures = [x for x in fixtures if '28797' not in x]

    # Define objects and scrape website
    stat_options = ['General','Attack','Defence']
    xpaths = [f'/html/body/main/div/div[3]/div[2]/div[3]/div[4]/div[2]/div[2]/div/div[2]/div[1]/ul/li[{x+1}]' for x in range(len(stat_options))]
    scraped_urls = uf.web_scrape(fixtures[:20],xpaths)
    scraped_urls = scraped_urls + uf.web_scrape(fixtures[20:],xpaths)

    # Loop through each fixture
    dataset = pd.DataFrame()
    for scraped_url in scraped_urls:
        # Define objects
        soup, pages = scraped_url

        # Find game details
        date = soup.find('div',class_='date date--rwc2024 match-details__date')
        date = '-'.join([date.find('span',class_=f'date__unit date__unit--{x}').get_text().replace('\n','').replace(',','').strip() for x in ['day-number','month','year']])
        
        # Find team names
        teams = [x.get_text() for x in soup.find_all('div',class_='mc-lineups__team-name')][:2]
        print(f'    - {teams[0]} vs {teams[1]}')
        
        # Find lineup
        starters_and_subs = [
            soup.find('div',class_='mc-lineups__team-lineups js-starters'),
            soup.find('div',class_='mc-lineups__substitutes js-substitutes')
        ]
        cols = ['Date','Team','Opposition','No.','Player']
        df = pd.DataFrame(columns=cols)
        for lineup in starters_and_subs:
            for position in lineup.find_all('div',class_='mc-lineups__player-row'):
                try:
                    num = position.find('div',class_='mc-lineups__player-number').get_text()
                except AttributeError:
                    # Error on https://www.rugbyworldcup.com/2023/match/pool-b-ireland-tonga#stats
                    num = str(int(df['No.'].tolist()[-1])+1)
                players = [x.get_text().strip() for x in position.find_all('div',class_='mc-lineups__player-name')]
                for team, opposition, player in zip(teams,reversed(teams),players):
                    df = pd.concat([df,pd.DataFrame([[date,team,opposition,num,player]],columns=cols)]).reset_index(drop=True)

        # Loop through the different stat options
        stat_options = ['General','Attack','Defence']
        for stat_option, page in zip(stat_options,pages):        
            # Locate section within Beautiful Soup
            stat_box = page.find('div',{'data-ui-tab':stat_option})
            
            # Scrape the title of each column
            stat_titles = stat_box.find_all('span',class_='mc-player-stats__header-cell-content')
            stat_titles = [x.get_text().replace('\t','').replace('\n','').strip() for x in stat_titles]
            stat_titles[3:] = [f'{stat_option}_{x}' for x in stat_titles[3:]]
            
            # Get the data by looping through each
            stat_input = pd.DataFrame()
            
            # Loop through each team
            for team in range(1,3,1):
                players = stat_box.find_all('tr',class_=f'mc-player-stats__table-row mc-player-stats__table-row--team-{team}')
                
                # Loop through each player
                for player in players:
                    stats = [x.get_text().strip() for x in player.find_all('td',class_='mc-player-stats__cell')]
                    stats[1] = teams[team-1]
                    stat_input = pd.concat([
                        stat_input,
                        pd.DataFrame([stats],columns=stat_titles)
                    ]).reset_index(drop=True)
            
            # Add stats to main dataframe
            df = df.merge(stat_input,on=['Team','No.','Player'])

        # Add to main dataset
        dataset = pd.concat([dataset,df]).reset_index(drop=True)

    # Fix columns
    col_names = {
        'General_P' : 'points',
        'General_MP' : 'minutes',
        'General_O' : 'offloads',
        'General_CM' : 'carries_made',
        'General_HE' : 'handling_errors',
        'Attack_P' : 'passes',
        'Attack_M' : 'meters',
        'Attack_CB' : 'clean_breaks',
        'Attack_DB' : 'defenders_beaten',
        'Attack_KFH' : 'kicks_from_hand',
        'Attack_LW' : 'lineout_won',
        'Defence_TA' : 'tackles',
        'Defence_MT' : 'tackles_missed',
        'Defence_TS' : 'tackles_success',
        'Defence_TW' : 'turnovers',
        'Defence_LS' : 'lineout_steal',
    }
    dataset.columns = [col_names.get(x,x) for x in dataset.columns]

    # Fix date column
    dataset.Date = pd.to_datetime(dataset.Date)

    # Save to file
    print(f'Scraping Complete - {url}\n')
    dataset.to_csv(f'{filepath}player_data_rwc.csv',index=False)

# https://www.espn.co.uk/rugby '''
def scrape_espn():
    # Use https://www.rugbyworldcup.com/2023/matches to get dates
    url = 'https://www.rugbyworldcup.com/2023/matches'
    soup = uf.web_scrape(url)
    dates = soup.find_all('h2',class_='fixtures__date-title')
    dates = [x.find('span',class_='regular').get_text().replace('September','09').replace('October','10').split(' ') for x in dates]
    dates = [f'{x[2]}{x[1]}0{x[0]}' if int(x[0]) < 10 else f'{x[2]}{x[1]}{x[0]}' for x in dates]

    # Get fixtures
    url = 'https://www.espn.co.uk'
    print(f'Start scraping the ESPN website')
    dates = [f"{url}/rugby/scoreboard/_/league/164205?date={date}" for date in dates]
    soups = uf.web_scrape(dates)
    fixtures = uf.flatten_list([soup.find_all('a',class_='mobileScoreboardLink') for soup in soups])
    fixtures = [f"{url}{x['href']}" for x in fixtures]

    # For espn, we need to scrape 2 pages for each match. One for the match details and the other for player stats
    match_details = uf.web_scrape(fixtures)
    stat_options = ['Scoring','Attacking','Defending','Discipline']
    xpaths = [f'/html/body/div[4]/section/div/section/section/div/div[2]/div[1]/div[2]/div[1]/ul/li[{x+1}]' for x in range(len(stat_options))]
    player_stats = uf.web_scrape([x.replace('match','playerstats') for x in fixtures],xpaths)

    # Loop through each fixture
    dataset = pd.DataFrame()
    for md, ps in zip(match_details, player_stats):
        # Find match info
        date = ' '.join(md.find('div',class_='col-two').find('article',class_='sub-module game-information').find('div',class_='game-date-time').get_text().split(', ')[1:])
        teams = [x.get_text() for x in  md.find('div',class_='competitors').find_all('span',class_='long-name')]
        print(f'    - {teams[0]} vs {teams[1]}')

        # Loop through the different stat options
        soup, pages = ps
        for stat_option, page in zip(stat_options,pages):

            # Locate section within Beautiful Soup
            stat_boxes = page.find('div',class_='sub-module tabbedTable').find_all('table',class_='mod-data')

            # Scrape the title of each column
            cols = ['date','team','opposition','pos','name']
            stat_titles = stat_boxes[0].find('tr',class_='header')
            stat_titles = cols + [f'{stat_option}_{x.get_text().strip()}' for x in stat_titles.find_all('th')[1:]]

            # Get the data by looping through each
            stat_input = pd.DataFrame()
            for team, opposition, stat_box in zip(teams,reversed(teams),stat_boxes):
                players = stat_box.find('tbody').find_all('tr')
                
                # Loop through each player
                for player in players:
                    stats = player.find_all('td')
                    stats = [date,team,opposition,stats[0].find('span').get_text(),stats[0].find('a').get_text()] + [x.get_text() for x in stats[1:]]
                    stat_input = pd.concat([
                        stat_input,
                        pd.DataFrame([stats],columns=stat_titles)
                    ]).reset_index(drop=True)

            # Add stats to main dataframe
            df = stat_input if stat_option == stat_options[0] else df.merge(stat_input,on=cols)

        # Add to main dataset
        dataset = pd.concat([dataset,df]).reset_index(drop=True)

    # Fix columns
    col_names = {
        'Scoring_T' : 'try',
        'Scoring_TA' : 'try_assist',
        'Scoring_CG' : 'conversion',
        'Scoring_PG' : 'penalty',
        'Scoring_DGC' : 'drop_goal',
        'Scoring_PTS' : 'points',
        'Attacking_-' : 'remove',
        'Attacking_P' : 'passes',
        'Attacking_R' : 'runs',
        'Attacking_MR' : 'meters_made',
        'Attacking_CB' : 'clean_breaks',
        'Attacking_DB' : 'defenders_beaten',
        'Attacking_O' : 'offload',
        'Defending_TC' : 'turnovers_conceded',
        'Defending_T' : 'tackles',
        'Defending_MT' : 'tackles_missed',
        'Defending_LW' : 'lineouts_won',
        'Discipline_PC' : 'penalties_conceded',
        'Discipline_YC' : 'yellow_card',
        'Discipline_RC' : 'red_card',
    }
    dataset.columns = [col_names.get(x,x) for x in dataset.columns]
    dataset.date = [pd.to_datetime(f'{uf.trailing_zero(x[1])}-{x[0]}-{x[2]}') for x in [x.replace('Septiembre','September').split(' ') for x in dataset.date]]

    # Save to file
    print(f'Scraping Complete - {url}\n')
    dataset.to_csv(f'{filepath}player_data_espn.csv',index=False)

# https://en.wikipedia.org/wiki/2023_Rugby_World_Cup_final '''
def scrape_wiki():
    # Get links to the wiki page for the fixtures
    print(f'Start scraping https://en.wikipedia.org/wiki/2023_Rugby_World_Cup')
    urls = [
        'https://en.wikipedia.org/wiki/2023_Rugby_World_Cup_Pool_A',
        'https://en.wikipedia.org/wiki/2023_Rugby_World_Cup_Pool_B',
        'https://en.wikipedia.org/wiki/2023_Rugby_World_Cup_Pool_C',
        'https://en.wikipedia.org/wiki/2023_Rugby_World_Cup_Pool_D',
        'https://en.wikipedia.org/wiki/2023_Rugby_World_Cup_knockout_stage',
    ]

    # Scrape the websites
    dataset = pd.DataFrame()
    soups = uf.web_scrape(urls)
    for soup in soups:

        # Extract fixture information
        tables = [x for x in soup.find_all('table') if 'Player of the Match:' in x.get_text()]
        summaries = soup.find_all('div',class_='vevent summary')
        
        # Loop through the summary section and the area tgat says who the motm is
        for summary, table in zip(summaries,tables):
            # Get summary details
            match_details = summary.find_all('table')
            result = match_details[1].find('tr',{'style':'vertical-align:top;font-weight:bold'}).find_all('td')

            # Add to dataset  
            dataset = pd.concat([dataset,pd.DataFrame([{
                'date' : ' '.join(match_details[0].find('td').get_text().split(' ')[:2]+['2023']),
                'team_home' : result[0].find('a').get_text(),
                'score' : result[1].get_text(),
                'team_away' : result[2].find('a').get_text(),
                'location' : match_details[2].find('span',class_='location').get_text(),
                'referee' : match_details[2].find('span',class_='attendee').find('a').get_text(),
                'motm' : [x.find('a').get_text() for x in table.find_all('p') if x.find('b').get_text() == 'Player of the Match:'][0]
            }])]).reset_index(drop=True)

    # Fix date column
    dataset.date = [pd.to_datetime(f'{uf.trailing_zero(x[0])}-{x[1]}-{x[2]}') for x in [x.split(' ') for x in dataset.date]]        

    # Save to file
    print(f'Scraping Complete - https://en.wikipedia.org/wiki/2023_Rugby_World_Cup\n')
    dataset.to_csv(f'{filepath}match_data_wiki.csv',index=False)


##################################################################################################################################
''' Run each function '''
##################################################################################################################################
if __name__ == '__main__':
    #scrape_rwc()
    scrape_espn()
    #scrape_wiki()