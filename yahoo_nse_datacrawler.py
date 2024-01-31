import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
# Set the ticker symbol for the desired Indian stock or index
# ticker = "HDFCBANK.NS"  # Example: Use "RELIANCE.NS" for Reliance Industries


cols = ['Datetime',
'Open',
'High',
'Low',
'Close',
'Adj Close',
'Volume','symbol']

    
os.chdir(r'c:\\Users\\ankit.ka\\Desktop\\work\\ankit_practice')

symbols = pd.read_csv('./valid_symbols.csv', sep='\t')['symbol'].values.tolist()

end_date = datetime.today()
start_date = end_date - timedelta(days=365)


os.chdir(r'c:\\Users\\ankit.ka\\Desktop\\work\\ankit_practice\\datasets')


cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume',
       'symbol']

new_cols = ['Date', 'Close', 'Volume',
       'symbol']

def DataExtract(ticker,time_frame = '1h'):
    temp = yf.download(ticker, start=start_date, end=end_date, interval=time_frame)
    temp['symbol'] = ticker
    temp = temp.reset_index()
    temp.columns = cols
    return temp

temp_df = pd.DataFrame([],columns=cols)

i = 0
# valid symbols looping
for symbol in symbols:

    _symbol = symbol+'.NS'

    try:
        temp_df = pd.concat([temp_df, DataExtract(_symbol)], ignore_index=1)

        
        
        print(f'{i}. {symbol} iteration done')
        i+=1
    except Exception as e:
        print(e)
        print(f'{i}. {symbol} iteration Failed')
        i+=1

temp_df = temp_df[new_cols]
temp_df['Close'] = temp_df['Close'].astype(int)

temp_df.to_feather(f'./stockdata_all.feather')