import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
from datetime import date


class Nse_data_dumper:

    cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume','symbol']
    new_cols = ['Date', 'Close', 'Volume','symbol']
    
    def __init__(self) -> None:
        pass 

    def get_valid_symbol(self,read_file_loc=None):    
        '''
        read all the valid symbols from csv and returns
        '''
        symbols = pd.read_csv(read_file_loc, sep='\t')['symbol'].values.tolist()
        
        return symbols
    
    def get_symbol_data(self,read_file_loc=None):
        try:
            symbols_data = pd.read_csv(read_file_loc)
        except:
            symbols_data = pd.DataFrame([],columns = Nse_data_dumper.new_cols)

        finally:
            return symbols_data


    def DataExtract(ticker,time_frame = '1h'):
        temp = yf.download(ticker, start=start_date, end=end_date, interval=time_frame)
        temp['symbol'] = ticker
        temp = temp.reset_index()
        temp.columns = cols
        return temp