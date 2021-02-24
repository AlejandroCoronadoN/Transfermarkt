import os
import requests
from time import sleep
import pdb
from bs4 import BeautifulSoup
import pandas as pd
import logging 

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
#logging.disable(logging.DEBUG)
logging.debug('start program')

def transfers(league_name, league_id, start, stop, type ='transfer'):
    """Scrape a league's transfers over a range of seasons.

    Args:
        league_name (str): Name of the league.
        league_id (str): League's unique Transfermarkt ID.
        start (int): First calendar year of the first season to scrape, e.g. 1992 for the 1992/93 season.
        stop (int): Second calendar year of the last season, e.g. 2019 for the 2019/20 season.
        tyep (str): 'transfer', 'valuation'
    """
    for i in range(start, stop + 1):
        league_transfers = []
        season_id = str(i)
        for window in ['s', 'w']:
            league_transfers.append(scrape_season(league_name, league_id, season_id, window, type))
            sleep(3)
        df = pd.concat(league_transfers)
        df = df[~df['Name'].isna()]
        df.reset_index(drop=True, inplace=True)
        export_csv(df, season_id, league_name)


def scrape_season(league_name, league_id, season_id, window, type):
    """Web scrapes Transfermarkt for all transfer activity in a league's given window.

    Args:
        league_name (str): Name of the league.
        league_id (str): League's unique Transfermarkt ID.
        season_id (str): First calendar year of the season, e.g. '2018' for 2018-19.
        window (str): 's' for summer or 'w' for winter transfer windows.
    Returns:
        A DataFrame of all season transfer activity in the input league.
    """
    if type== 'transfer': 
      clubs, transfer_in_list, transfer_out_list = get_clubs_and_transfers(league_name, league_id, season_id, window)
      print("Got data for {} {} {} transfer window".format(season_id, league_name.upper(), window.upper()))
      transfers_in, transfers_out = formatted_transfers(clubs, transfer_in_list, transfer_out_list)
      print("Formatted transfers")
      df_in = transfers_dataframe(transfers_in)
      df_out = transfers_dataframe(transfers_out)
      df_return= pd.concat([df_in, df_out])
      print("Created dataframes")
      print("\n********************************\n")
    
    elif type=='valuation': 
      valuation_rows = get_clubs_and_valuation(league_name, league_id)
      df_return = pd.DataFrame(valuation_rows[1:], columns = valuation_rows[0])
      raise ValueError('test to save')
    return df_return



def get_clubs_and_valuation(league_name, league_id):
    """Requests the Transfermarkt page for the input league season and scrapes the page HTML for valuation data.

    Args:
        league_name (str): Name of the league.
        league_id (str): League's unique Transfermarkt ID.
    Returns:
        A list of the clubs in the league, and two lists of tables (list of lists) for each club's transfer activity. 
    """
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    base = "https://www.transfermarkt.co.uk"
    url = base + "/{league_name}/marktwerte/wettbewerb/{league_id}/pos//detailpos/0/altersklasse/alle/plus/1 ".format(league_name=league_name, league_id=league_id)
    
    #TODO include Selenium extension to iterate across tabs and download html files
    try:
        print("Connecting...")
        response = requests.get(url, headers=headers)
        print("Connection successful, status code {}".format(response.status_code))
    except requests.exceptions.RequestException as e:
        print(e)
        exit()
    #! -------------------- DELETE --------------------------------
    soup = BeautifulSoup(response.content, 'lxml')
    test1  = open('../evaluations/e4.html', 'r')
    content = test1.read()
    soup = BeautifulSoup(content, 'lxml')
    #! -------------------- DELETE --------------------------------


    tables = [tag.findChild() for tag in soup.find_all('div', {'class': 'responsive-table'})]
    logging.info('tables: ...' + str(tables))
    #pdb.set_trace()
    for table_in in tables:
        transfer_in_list = get_valuation_info(base, table_in, league_name)
    return transfer_in_list




def get_clubs_and_transfers(league_name, league_id, season_id, window):
    """Requests the Transfermarkt page for the input league season and scrapes the page HTML for transfer data.

    Args:
        league_name (str): Name of the league.
        league_id (str): League's unique Transfermarkt ID.
        season_id (str): First calendar year of the season, e.g. '2018' for 2018-19.
        window (str): 's' for summer or 'w' for winter transfer windows.
    Returns:
        A list of the clubs in the league, and two lists of tables (list of lists) for each club's transfer activity. 
    """
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    base = "https://www.transfermarkt.co.uk"
    url = base + "/{league_name}/transfers/wettbewerb/{league_id}/plus/?saison_id={season_id}&s_w={window}".format(league_name=league_name, league_id=league_id, season_id=season_id, window=window)
    #https://www.transfermarkt.us/liga-mx-apertura/marktwerte/wettbewerb/MEXA/pos//detailpos/0/altersklasse/alle/plus/1    
    
    try:
        print("Connecting...")
        response = requests.get(url, headers=headers)
        print("Connection successful, status code {}".format(response.status_code))
    except requests.exceptions.RequestException as e:
        print(e)
        exit()
    soup = BeautifulSoup(response.content, 'lxml')
    
    clubs = [tag.text for tag in soup.find_all('div', {'class': 'table-header'})][1:]
    logging.info('Clubs: ...' + str(clubs))
    #pdb.set_trace()
    
    tables = [tag.findChild() for tag in soup.find_all('div', {'class': 'responsive-table'})]
    logging.info('tables: ...' + str(tables))
    #pdb.set_trace()
    
    table_in_list = tables[::2]
    table_out_list = tables[1::2]
    
    pdb.set_trace()
  
    transfer_in_list = []
    transfer_out_list = []
    column_headers = {'season': season_id, 'window': window, 'league': league_name}
    for table_in, table_out in zip(table_in_list, table_out_list):
        transfer_in_list.append(get_transfer_info(base, table_in, movement='In', **column_headers))
        transfer_out_list.append(get_transfer_info(base, table_out, movement='Out', **column_headers))

    logging.info('transfer_in_list: ...' + str(transfer_in_list))
    logging.info('transfer_out_list: ...' + str(transfer_out_list))

    pdb.set_trace()    
    return clubs, transfer_in_list, transfer_out_list

def get_valuation_info(url_base, table, league):
    """Helper function to parse an HTML table and extract all desired player information.

    Args:
        url_base (str): Transfermark URL for profile link prepending.
        table (bs4.element.Tag): BeautifulSoup HTML table.
        movement (str): 'In' for arrival or 'Out' departure. 
        league (str): League name.
    Returns:
        The input table's information reformatted as a list of lists. 
    """

    transfer_info = []
    trs = table.find_all('tr')
    header_row = [header.get_text(strip=True) for header in trs[0].find_all('th')]
    if header_row:
        #['#', 'player', 'Nat.', 'Age', 'club', 'Market value']
        logging.info('header _row: ...' + str(header_row))
        header_row = ['index_valuation_liga', 'player', 'position', 'nationality', 'age', 'club', 'godknowswhat', 'highestValue_Date', 'highestValue', 'valuationDate', 'valuation','liga', 'player_url']
        transfer_info.append(header_row)
    for tr in trs[1:]:
      if tr.get('class'):
        if tr.get('class')[0] == 'even' or tr.get('class')[0] == 'odd':
          logging.info('table == table_in/tablue_out => tr : ...' + str(header_row))
          row = []
          tds = tr.find_all('td')
          for td in tds:
              child = td.findChild()
              if child and child.get('class'):
                  # Player name and profile link
                  if child.get('class')[0] == 'inline-table':
                      player = child.find('a', href=True)
                      row.append([player.get_text(strip=True), url_base + player.get('href')])# Player nationality
                  elif child.get('class')[0] == 'flaggenrahmen':
                      row.append(child.get('alt'))
                  #Highest Value
                  elif child.get('class')[0]=='cp':
                      row.append(child.get('alt'))  
                      row.append([child.get_text(strip=True), child.get('title')])# Player nationality
    
                  # Club dealt to/from
                  elif child.get('class')[0] == 'vereinprofil_tooltip':
                      row.append(child.findChild().get('alt'))
              else:
                  row.append(td.get_text(strip=True))
          # Mark tables of no transfer activity with None for later cleaning
          if "No new arrivals" in row or "No departures" in row:
              transfer_info.append([None] * (len(header_row) - 1))
          else:
              #TODO the row system to collect data is ineficcient, It could be improved with dictionary
              row += [league]
              row.append(row[1][1])
              row[1] = row[1][0]
              row.append(row[7][1])
              row[7] = row[7][0]
              transfer_info.append(row)
              
    return transfer_info
  
  
def get_transfer_info(url_base, table, movement, season, window, league):
    """Helper function to parse an HTML table and extract all desired player information.

    Args:
        url_base (str): Transfermark URL for profile link prepending.
        table (bs4.element.Tag): BeautifulSoup HTML table.
        movement (str): 'In' for arrival or 'Out' departure. 
        season (str): Season.
        window (str): 's' for summer or 'w' for winter.
        league (str): League name.
    Returns:
        The input table's information reformatted as a list of lists. 
    """
    transfer_info = []
    trs = table.find_all('tr')
    header_row = [header.get_text(strip=True) for header in trs[0].find_all('th')]
    if header_row:
        logging.info('header _row: ...' + str(header_row))
        #['In', 'Age', 'Nat.', 'Position', 'Pos', 'Market value', 'Left', 'Fee']
        header_row[0] = 'Name'
        header_row.insert(0, 'Club')
        header_row[3] = 'Nationality'
        header_row[-3] = 'MarketValue'
        header_row[-2] = 'ClubInvolved'
        header_row.insert(-1, 'CountryInvolved')
        header_row += ['Movement', 'Season', 'Window', 'League', 'Profile']
        transfer_info.append(header_row)
    for tr in trs[1:]:
        logging.info('table == table_in/tablue_out => tr : ...' + str(header_row))

        row = []
        tds = tr.find_all('td')
        for td in tds:
            child = td.findChild()
            if child and child.get('class'):
                # Player name and profile link
                if child.get('class')[0] == 'di':
                    player = child.find('a', href=True)
                    row.append([player.get_text(strip=True), url_base + player.get('href')])
                # Player nationality
                elif child.get('class')[0] == 'flaggenrahmen':
                    row.append(child.get('alt'))
                # Club dealt to/from
                elif child.get('class')[0] == 'vereinprofil_tooltip':
                    row.append(child.findChild().get('alt'))
            else:
                row.append(td.get_text(strip=True))
        # Mark tables of no transfer activity with None for later cleaning
        if "No new arrivals" in row or "No departures" in row:
            transfer_info.append([None] * (len(header_row) - 1))
        else:
            row += [movement, season, window, league]
            row.append(row[0][1])
            row[0] = row[0][0]
            transfer_info.append(row)
    #['Federico Viñas', '22', 'Uruguay', 'Centre-Forward', 'CF', '£2.70m', 'Juventud de Las Piedras', 'Uruguay', '£1.56m', 'In', '2020', 's', 'liga-mx-apertura', 'https://www.transfermarkt.co.uk/federico-vinas/profil/spieler/578419']
    #['Club', 'Name', 'Age', 'Nationality', 'Position', 'Pos', 'MarketValue', 'ClubInvolved', 'CountryInvolved', 'Fee', 'Movement', 'Season', 'Window', 'League', 'Profile']
    return transfer_info


def formatted_transfers(clubs, transfers_in, transfers_out):
    """Prepends club names to their transfers.

    Args:
        clubs (list): List of clubs.
        transfers_in (list): List of lists.
        transfers_out (list): List of lists.
    Return:
        Updated transfer tables.
    """
    for i in range(len(clubs)):
        club_name = clubs[i]
        for row in transfers_in[i][1:]:
            row.insert(0, club_name)
        for row in transfers_out[i][1:]:
            row.insert(0, club_name)
    
    return transfers_in, transfers_out


def transfers_dataframe(tables_list):
    """Converts all transfer tables to dataframes then concatenates them into a single dataframe.

    Args:
        tables_list (list): List of transfer DataFrames.
    Returns:
        A DataFrame of all transfers.
    """
    return pd.concat([pd.DataFrame(table[1:], columns=table[0]) for table in tables_list])


def export_csv(df, season_id, league_name):
    """Writes an input DataFrame to a csv in its corresponding season's folder.

    Args:
        df (DataFrame): Transfer data to be exported.
        season_id (str): Folder in which to write the csv.
        league_name (str): File name for the csv.
    """
    file_name = '{}.csv'.format(league_name)
    current_dir = os.path.dirname(__file__)
    path_name = os.path.join(current_dir, '../data/{}'.format(season_id))
    if not os.path.exists(path_name):
        os.mkdir(path_name)
    
    export_name = os.path.join(path_name, file_name)
    df.to_csv(export_name, index=False, encoding='utf-8')



def main():
    # England, Premier League
    print("Getting Premier League data...\n")
    #transfers('liga-mx-apertura', 'MEXA', 2020, 2021, 'transfer')
    transfers('liga-mx-apertura', 'MEXA', 2020, 2021, 'valuation')
    print("Done with the Premier League!")
    print("********************************\n")


if __name__ == "__main__":
    main()
