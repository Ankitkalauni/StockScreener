# main.py

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
from datetime import date, datetime
from pytz import timezone
import json
import boto3, io
from botocore.exceptions import ClientError
from logger import Logger

class Nse_data_dumper:
    '''
    To get the data for each valid NSE symbol from given START_TS and END_TS
    and update the last run value to the config file.
    '''
    
    def __init__(self, logger, log_file_obj) -> None:
        self.logger = logger
        self.log_file_obj = log_file_obj
        self.cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume','symbol']
        self.new_cols = ['Date', 'Close', 'Volume','symbol']
        self.config_loc = '/opt/airflow/scripts/config.json'

        self.s3_client = boto3.client('s3')

        with open(self.config_loc, 'r') as file_obj:
            _json_file = json.load(file_obj)

        self.s3bucket_name = _json_file['bucket_name']
        self.s3object_key = _json_file['object_key']
        self.last_update = datetime.fromisoformat(_json_file['last_update'])

        del file_obj

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
            symbols_data = pd.DataFrame([],columns = self.new_cols)

        finally:
            return symbols_data

    def DataExtract(self, ticker, start_date, end_date, time_frame = '1h'):
        '''
        function to call Yahoo API and get the data for {TICKER}
        '''
        temp = yf.download(ticker, start=start_date, end=end_date, interval=time_frame)
        temp['symbol'] = ticker
        temp = temp.reset_index()
        temp.columns = self.cols
        return temp

    def update_lastupdate(self, ts, file_loc=None):
        if not file_loc:
            file_loc = self.config_loc
        with open(file_loc, 'r+') as file_obj:
            json_file = json.load(file_obj)
            json_file['last_update'] = ts.isoformat()
            file_obj.seek(0)
            json.dump(json_file, file_obj)
            file_obj.truncate()
            self.logger.log(self.log_file_obj, f"Last update time saved: {ts}")

    def get_lastupdate(self, file_loc=None):
        """
        Instead, use class variable to get the last updated date
        """
        if not file_loc:
            file_loc = self.config_loc
        with open(file_loc, 'r') as file_obj:
            json_file = json.load(file_obj)
            return datetime.fromisoformat(json_file['last_update'])

    def get_S3data(self, file_loc=None):
        '''
        Get the parquet data from the S3 location.
        Setup the config for AWS access key if not getting for boto3.
        '''
        try:
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=self.s3bucket_name, Key=self.s3object_key)
            parquet_file = io.BytesIO(response['Body'].read())
            del response, s3

            self.logger.log(self.log_file_obj, f'Loaded data from s3 to the machine')


            return pd.read_parquet(parquet_file)
        except ClientError as e:
            self.logger.log(self.log_file_obj, f'An error occurred while accessing S3: {e}')
            raise
    
    def _put_S3data(self, df: pd.DataFrame, file_loc=None):
        '''
        Input ->
        file: pandas dataframe

        Stores the pandas dataframe to the S3 bucket named in the config file.
        '''
        try:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        
            self.s3_client.put_object(Bucket=self.s3bucket_name, Key=self.s3object_key, Body=parquet_buffer.getvalue())
            self.logger.log(self.log_file_obj, "Data successfully uploaded to S3.")
            return True
        except Exception as e:
            self.logger.log(self.log_file_obj, f'Error occurred while uploading data to S3: {e}')
            raise e

    def _check_file_exists(self, bucket_name, object_key):
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.log(self.log_file_obj, f'Error while checking for object in S3: -- Code 404 -- {e}')
                return False
            else:
                # For any other error code, re-raise the exception
                self.logger.log(self.log_file_obj, f'Error while checking for object in S3: {e}')
                raise

    def merge_and_export(self, df_input):
        try:
            if self._check_file_exists(bucket_name=self.s3bucket_name, object_key=self.s3object_key):
                main_df = self.get_S3data()
                self.logger.log(self.log_file_obj, f"loaded dataframe shape {main_df.shape}")
                df_merged = pd.concat([main_df, df_input], ignore_index=True)
            else:

                self.logger.log(self.log_file_obj, f"No file found on s3 pushing new file instead")
                df_merged = df_input

            df_merged = df_merged.drop_duplicates()
            
            six_months_ago = datetime.now(timezone("Asia/Kolkata")) - timedelta(days=6*30)
            df_merged = df_merged[df_merged['ts'] >= pd.to_datetime(six_months_ago).strftime('%Y-%m-%d %H:%M:%S.%f')]

            if self._put_S3data(df_merged):
                self.logger.log(self.log_file_obj, f"Data merged, cleaned, and exported successfully to {self.s3bucket_name}")
            else:
                raise Exception('An error occurred while uploading the new object to S3')
        except Exception as e:
            self.logger.log(self.log_file_obj, f"An error occurred: {e}")
            raise

if __name__ == '__main__':
    # Initialize the logger
    logger = Logger()

    # Specify the log file path
    log_file_path = '/opt/airflow/logs/activity_log.txt'

    # Open the log file in append mode
    with open(log_file_path, 'a') as log_file_obj:
        nse = Nse_data_dumper(logger, log_file_obj)

        start_date = nse.last_update
        end_date = datetime.today()

        if start_date.date() == end_date.date():
            logger.log(log_file_obj, f"No data to update. Start and end dates are the same: {start_date.date()}")
            exit(69)
        logger.log(log_file_obj, f"Starting session for {start_date} to {end_date}")

        symbols = nse.get_valid_symbol()
        
        temp_df = pd.DataFrame([], columns=nse.cols)

        for i, symbol in enumerate(symbols[:5]):
            try:
                temp_df = pd.concat([temp_df, nse.DataExtract(symbol+'.NS', start_date, end_date)], ignore_index=1)

                if i%100 == 0:
                    logger.log(log_file_obj, f"After running {symbol}, updated shape: {temp_df.shape}")
                    logger.log(log_file_obj, f"{i}. {symbol} iteration done")
            except Exception as e:
                logger.log(log_file_obj, f"{i}. {symbol} iteration failed: {e}")
        
        temp_df = temp_df[nse.new_cols]
        temp_df['Close'] = temp_df['Close'].astype(int)
        temp_df['Date'] = pd.to_datetime(temp_df['Date'], errors='coerce')
        temp_df.rename(columns={'Date':'ts'}, inplace = True)
        logger.log(log_file_obj, f"After running all symbols, dataframe shape: {temp_df.shape}")

        try:
            nse.merge_and_export(temp_df)
            nse.update_lastupdate(end_date)
        except Exception as e:
            logger.log(log_file_obj, f"New Parquet file creation failed: {e}")