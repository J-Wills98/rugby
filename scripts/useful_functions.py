'''
    Filename: useful_functions.py
    Purpose: Define some usefulf functions to be used in the other scripts.
'''

##################################################################################################################################
''' Importing packages and fixing pathway '''
##################################################################################################################################
# Import packages
import pandas as pd
import sys, os, itertools
import chromedriver_autoinstaller
from math import floor
from rapidfuzz import fuzz
from statistics import mean
from bs4 import BeautifulSoup
from selenium import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


# Set up selenium's chrome crawl
chromedriver_autoinstaller.install()
chrome_options = Options()
chrome_options.headless = True
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")

# Fix pathway
module_path = os.path.abspath(os.path.join(''))
if module_path not in sys.path:    
    sys.path.append(module_path)


##################################################################################################################################
''' Define class of functions '''
##################################################################################################################################
# Scrape website
def web_scrape(
        url_list,
        click = None,
        options = chrome_options
    ):
    '''
        Purpose: To scrape the webpages of the inputted urls
        Assumptions: We'll repeat the click for each url
    '''
    # Depends on number of calls required we might need to relaunch the webdriver a few times
    calls_per_url = len(click) if click != None else 1
    url_list = url_list if isinstance(url_list,list) else [url_list]
    url_list = break_list(url_list,floor(100/calls_per_url))

    # Loop through the list of urls
    soups = []
    for urls in url_list:
        # Launch chrome
        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(60)

        # Loop through the urls
        for url in urls:
            # Access url
            print(f'    - Scraping {url}')
            driver.get(url)

            # Scrape using beautiful soup
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            # If we need to click some buttons
            if click != None:
                click = click if isinstance(click,list) else [click]
                pages = []

                # Loop through each button that needs clicking and scrape the updated page
                for button in click:
                    # Click the button
                    l = driver.find_element(By.XPATH,button)
                    driver.execute_script("arguments[0].click();", l)

                    # Scrape the page
                    html = driver.page_source
                    pages = pages + [BeautifulSoup(html, 'html.parser')]
                
                # Add to output list
                soups = soups + [[soup,pages]]
            else:
                soups = soups + [soup]

        # Close driver
        driver.close()

    # Output
    if len(soups) == 1:
        return soups[0]
    else:
        return soups

# Generally useful function
def join(
        left : pd.DataFrame,
        lcols : list,
        right : pd.DataFrame,
        rcols = [],
        out = "join",
    ):
    '''
        Purpose: To join two dataframes together based on chosen columns
        Inputs:
            - left = First dataframe input (L)
            - lcols = Columns from first dataframe to match up
            - right = Second dataframe input (R)
            - rcols = Columns from second dataframe to match up. If not specified, use lcols
            - out = Type of join, options are:
                - 'join' (default) = Contains records that joined from the L input to the records in the R input.
                - 'left' = Contains records from the L input that didn't join to records from the R input.
                - 'right' = Contains records from the R input that didn't join to records from the L input.
                - 'left_join' = Combination of 'left' and 'join'.
                - 'right_join' = Combination of 'right' and 'join'.
                - 'all' = Combination of 'join', 'left' & 'right'
    '''
    # Add errors for incorrect inputs
    # if isinstance(left,pd.DataFrame) or isinstance(left):
    #     raise ValueError(f"'left' input must be a pandas dataframe. Type = {type(left)}")
    # if isinstance(right, pd.DataFrame):
    #     raise ValueError("'right' input must be a pandas dataframe")
    # if isinstance(lcols, list):
    #     raise ValueError("'lcols' input must be a list")
    # if isinstance(rcols, list):
    #     raise ValueError("'rcols' input must be a list")
    output_options = ['join','left','right','left_join','right_join','all']
    if output_options.count(out) != 1:
        raise ValueError(f"'out' input must be one of the following options: {', '.join(output_options)}")

    # Check if the columns selected are valid
    if mean([left.columns.tolist().count(x) for x in lcols]) != 1:
        raise ValueError(f"{', '.join([x for x in lcols if left.columns.tolist().count(x) != 1])} are not columns in 'lcols'")
    rcols = lcols if rcols == [] else rcols # If only one list of column names has been specified, update rcols to equal lcols
    if mean([right.columns.tolist().count(x) for x in rcols]) != 1:
        raise ValueError(f"{', '.join([x for x in rcols if right.columns.tolist().count(x) != 1])} are not columns in 'rcols'")

    # Create copies of the inputted datasets
    l = left.copy(deep=True).reset_index(drop=True)
    r = right.copy(deep=True).reset_index(drop=True)

    # If selected output is 'join', enact merge
    if out == "join":
        df = l.merge(r,left_on=lcols,right_on=rcols)

    # If selected output is 'left', enact merge
    elif out == "left":
        # Add a unique ID column called "dummy_col"
        l.index = l.index.set_names(["dummy_col"])
        l = l.reset_index()
        # Do a full merge and a left merge then use the unique ID to spot the missing columns
        df_merged = l.merge(r,left_on=lcols,right_on=rcols)
        rows = list(set(l["dummy_col"]) - set(df_merged["dummy_col"]))
        df = l[l["dummy_col"].isin(rows)].drop(columns="dummy_col")

    # If selected output is 'right', enact merge
    elif out == "right":
        # Add a unique ID column called "dummy_col"
        r.index = r.index.set_names(["dummy_col"])
        r = r.reset_index()
        # Do a full merge and a left merge then use the unique ID to spot the missing columns
        df_merged = l.merge(r,left_on=lcols,right_on=rcols)
        rows = list(set(r["dummy_col"]) - set(df_merged["dummy_col"]))
        df = r[r["dummy_col"].isin(rows)].drop(columns="dummy_col")

    # If selected output is 'left_join', enact merge
    elif out == "left_join":
        df = l.merge(r,left_on=lcols,right_on=rcols,how="left")

    # If selected output is 'right_join', enact merge
    elif out == "right_join":
        df = l.merge(r,left_on=lcols,right_on=rcols,how="right")

    # If selected output is 'all', enact merge
    elif out == "all":
        df = l.merge(r,left_on=lcols,right_on=rcols,how="outer")
    
    # Output
    return df.reset_index(drop=True)

# Name fix
def name_fix(df,col,mapping,col_new=False):
    # Map column while keeping a copy of the data before the mapping
    col_list = df[col]
    if col_new==False:
        df[col] = [mapping.get(x,x) for x in df[col]]
    else:
        df[col_new] = [mapping.get(x,x) for x in df[col]]

    # Count the changes made
    number_changes_expected = len(mapping)
    number_changes_actual = sum([1 if x != 0 else 0 for x in [mapping.get(x,0) for x in col_list]])

    # If miss-match in expected vs actual changes, then flag
    if number_changes_expected != number_changes_actual:
        print('Missmatch in number of changes made against expected number of changes')
        print([x for x in [mapping.get(x,0) for x in col_list] if x != 0])

# Define function to matching up
def matching(df1, df2, threshold = 90):
    # Definve vars
    on = ["name_link","team"]

    # Matches
    df = join(df1,on,df2,on)

    # Misses
    df_left = join(df1,on,df2,on,out='left')
    df_left['link'] = 1
    df_right = join(df1,on,df2,on,out='right')
    df_right['link'] = 1

    # Fuzzy match
    df_fuzzy = pd.DataFrame()
    for x in df_left.name_link:
        team = df_left[df_left.name_link == x].team.values[0]
        for y in df_right[df_right.team == team].name_link:
            if fuzz.ratio(x,y) > threshold:
                # If there's a match, join up
                fuzz1 = df_left[df_left.name_link == x].reset_index(drop=True)
                fuzz2 = df_right[df_right.name_link == y].reset_index(drop=True)
                on2 = ['link','team']
                fuzz_match = join(fuzz1,on2,fuzz2,on2)
                # Add to fuzzy list
                df_fuzzy = pd.concat([df_fuzzy, fuzz_match])
                # Remove from missing list
                df_left = df_left[df_left.name_link != x].reset_index(drop=True)
                df_right = df_right[df_right.name_link != y].reset_index(drop=True)
    # Keep left name_link
    if df_fuzzy.shape != (0,0):
        #print(df_fuzzy)
        df_fuzzy = df_fuzzy.drop(columns='name_link_y').rename(columns={'name_link_x':'name_link'}).drop(columns='link').reset_index(drop=True)

    # Merge and output
    df_missing = pd.concat([df_left,df_right]).drop(columns='link').reset_index(drop=True)
    return pd.concat([df, df_fuzzy,df_missing]).reset_index(drop=True)

# Trailing zero
def trailing_zero(num):
    out = str(num) if len(num) == 2 else '0' + str(num)
    return out

# Flatten a list of lists to just one list
def flatten_list(nested_list):
    return list(itertools.chain(*nested_list))

# Break a list up into lists of list (each one of a particular maximum length)
def break_list(l,cap):
    # Define objects
    out = []

    # First iteration
    out = out + [l[:cap]]
    l = [] if len(l) < cap else l[cap:]

    # Breaks the list up
    while len(l) > 0:
        out = out + [l[:cap]]
        l = l[cap:]

    # Output
    return out
