import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
from datetime import date, datetime
from pytz import timezone
import json

class Nse_data_dumper:
    '''
    To get the data for each valid NSE symbols from given START_TS and END_TS
    and update the last run value to the config file.
    '''
    
    def __init__(self) -> None:
        self.cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume','symbol']
        self.new_cols = ['Date', 'Close', 'Volume','symbol']
        self.config_loc = '/opt/airflow/scripts/config.json'

    def get_valid_symbol(self,read_file_loc='/opt/airflow/datasets/valid_symbols.csv'):    
        '''
        read all the valid symbols from csv and returns
        '''
        symbols = pd.read_csv(read_file_loc, sep='\t')['symbol'].values.tolist()
        
        return symbols
    
    def get_symbol_data(self,read_file_loc=None):
        '''
        get the existing symbol data (CORPUS last 1 year data)
        '''
        try:
            symbols_data = pd.read_csv(read_file_loc)
        except:
            symbols_data = pd.DataFrame([],columns = Nse_data_dumper.new_cols)

        finally:
            return symbols_data


    def DataExtract(self,ticker, start_date, end_date,time_frame = '1h'):
        '''
        function to call Yahoo API and get the data for {TICKER}
        '''
        temp = yf.download(ticker, start=start_date, end=end_date, interval=time_frame)
        temp['symbol'] = ticker
        temp = temp.reset_index()
        temp.columns = self.cols
        return temp


    def update_lastupdate(self,ts,file_loc=None, log_obj=None):
        if not file_loc:
            file_loc = self.config_loc
        with open(file_loc, 'r+') as file_obj:
            json_file = json.load(file_obj)
            json_file['last_update'] = ts.isoformat()
            file_obj.seek(0)
            json.dump(json_file, file_obj)
            file_obj.truncate()
            
            if log_obj:
                log_obj.log(f'file saved: {ts}')


    def get_lastupdate(self,file_loc=None, log_obj=None):
        if not file_loc:
            file_loc = self.config_loc
        with open(file_loc, 'r') as file_obj:
            json_file = json.load(file_obj)

            return datetime.fromisoformat(json_file['last_update'])

    def merge_and_export(self,df_input, file_path):
        try:
            if os.path.exists(file_path):
                main_df = pd.read_parquet(file_path)
                df_merged = pd.concat([main_df, df_input], ignore_index=True)
            else:
                df_merged = df_input

            df_merged = df_merged.drop_duplicates()
            
            # Filter out data older than 6 months -------------------------------------------------------
            six_months_ago = datetime.now(timezone("Asia/Kolkata")) - timedelta(days=6*30)
            df_merged = df_merged[df_merged['Date'] >= pd.to_datetime(six_months_ago).strftime('%Y-%m-%d %H:%M:%S.%f')]
            print('doneeeeeeeeeeeeeee')
            df_merged.to_parquet(file_path, compression='snappy')
            
            print(f"Data merged, cleaned, and exported successfully to {file_path}")
            
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == '__main__':
    nse = Nse_data_dumper()

    start_date = nse.get_lastupdate()
    end_date = datetime.today()


    if start_date.date() == end_date.date():
        exit(69)
    print(f'starting session for {start_date} to {end_date}')

    symbols = nse.get_valid_symbol()
    
    temp_df = pd.DataFrame([],columns=nse.cols)

    for i, symbol in enumerate(symbols):
    # exit(1)
        try:
            temp_df = pd.concat([temp_df, nse.DataExtract(symbol+'.NS', start_date, end_date)], ignore_index=1)
            print(f'after running {symbol} here is the updated shape {temp_df.shape}')
            print(f'{i}. {symbol} iteration done\n')
            i+=1
        except Exception as e:
            print(e)
            print(f'{i}. {symbol} iteration Failed\n')
            i+=1    

    temp_df = temp_df[nse.new_cols]
    temp_df['Close'] = temp_df['Close'].astype(int)
    temp_df['Date'] = pd.to_datetime(temp_df['Date'], errors='coerce')


    print('after running all symbols here is the dim of df', temp_df.shape)
    try:
        nse.merge_and_export(temp_df, '../all_symbols_data.parquet')
        nse.update_lastupdate(end_date)
    except:
        print('new feather file creation failed')
        

