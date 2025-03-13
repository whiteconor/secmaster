import requests
import pandas as pd
from bs4 import BeautifulSoup

class LSEException( Exception ):
    pass

def get_lse_daily_list_url():
    """
    url = 'https://www.londonstockexchange.com/reports?tab=instruments'
    res = requests.get(url)
    
    if res.status_code == 200:
        print(res.text)
        soup = BeautifulSoup(res.text, 'html.parser')
        download_div = soup.find('div', class_='download-single-content')
        download_link = download_div.find('a')
        return download_link['href']
    else:
        msg = f'LSE daily list download failed: {res.status_code} : {res.text}'
        raise LSEException( msg )
    """
    return 'https://docs.londonstockexchange.com/sites/default/files/reports/Instrument%20list_62.xlsx'

def get_lse_isin_to_currency():
    url = get_lse_daily_list_url()
    lse_daily_list = pd.read_excel(url, sheet_name='1.0 All Equity', skiprows=8)
    isin_to_currency = lse_daily_list[['ISIN','Trading Currency']].drop_duplicates().set_index('ISIN')
    isin_to_currency['Trading Currency'] = isin_to_currency['Trading Currency'].apply( lambda x: x.replace('GBX','GBp') )
    return isin_to_currency.to_dict()['Trading Currency']
