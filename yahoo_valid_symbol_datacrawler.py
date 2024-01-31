import requests as  r
from yfinance.data import YfData
import json
import pandas as pd 
import os 
# os.chdir('..')

_data  : YfData = YfData(session=None)
os.chdir(r'c:\\Users\\ankit.ka\\Desktop\\work\\ankit_practice')




def test(ticker):
    _ticker = ticker + ".NS"
    _base_url = 'https://query2.finance.yahoo.com'
    url = f"{_base_url}/v7/finance/options/{_ticker}"
    try:
        return_val = json.loads(_data.get(url = url, proxy = None).content)['optionChain']['result'][0]['quote']['shortName']
    except:
        return_val = 'error'
    return return_val

if __name__ == '__main__':
    df = pd.read_csv('./market_symbols.csv')

    df['short_name'] = df['symbol'].apply(test)
    df.to_csv('market_value.csv',index=False)