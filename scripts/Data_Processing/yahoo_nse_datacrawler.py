import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
from datetime import date



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
start_date = end_date - timedelta(days=2)

                                                              
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

        
        
        print(f'{i}. {symbol} iteration done\n')
        i+=1
    except Exception as e:
        print(e)
        print(f'{i}. {symbol} iteration Failed\n')
        i+=1

temp_df = temp_df[new_cols]
temp_df['Close'] = temp_df['Close'].astype(int)


try:
    temp_df.to_feather(f'./stockdata_all{str(date.today())}.feather')
except:
    print('new feather file creation failed')

temp_df.to_csv(f'./stockdata_all{str(date.today())}.csv', index=False)

df1 = pd.read_csv('./stockdata_all.csv')

final_df = pd.concat([df1,temp_df], axis= 0)

final_df = final_df.drop_duplicates(ignore_index=True).reset_index(drop=True)

print(os.getcwd())

final_df.to_csv('./stockdata_all.csv', index=False)
final_df.to_feather('./stockdata_all.feather')








































# import concurrent.futures
# import pandas as pd
# import requests
# import xml.etree.ElementTree as ET

# import requests
# import xml.etree.ElementTree as ET
# import pandas as pd 
# import os


# os.chdir("c:\\Users\\ankit.ka\\Desktop\\work\\July'23")


# base_url = 'http://url-crawl-status-ao1-g-usw1b.internal.media.net/CrawlUrlStatus/find?url='

# def response_data(url):
#     url1 = base_url + url
#     print(url1)
#     xml_data = ET.fromstring(requests.get(url1).content)
#     status = xml_data.find('status')
#     print(status.text)
#     return status.text if status is not None else None

# def process_urls(url):
#     return url, response_data(url)

# if __name__ == '__main__':
#     df = pd.read_csv("./urls_nocov_20230705.csv")
#     urls = df['url'].tolist()

#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         results = list(executor.map(process_urls, urls))

#     for url, status in results:
#         print(url)
#         df.loc[df['url'] == url, 'crawled'] = status

#     df.to_csv('./url_is_crawled_output.csv', index=False)


# def get_data_for_recall():
#     df = pd.read_csv('/data/brand_safety_serving/military_conflicts.tsv', sep="\t", error_bad_lines=False)
#     #df = pd.read_csv('/Users/vishakhasharma/final_data.csv', sep="\t", error_bad_lines=False)
#     executor = ThreadPoolExecutor(max_workers=10)
#     futures = [executor.submit(get_sentence_based_brand_safety_info_v2, row) for index, row in
#                df.iterrows()]
#     data = []
#     for future in as_completed(futures):
#         result = future.result()
#         #print(result)
#         data.append(result)
#     #df = pd.DataFrame(data, columns=['url','domain','title','category','whitelisted','impression','score','informative','model','sentence','isBrandSafe'])
#     df = pd.DataFrame(data, columns=['url','domain','title','category','regex_vertical','whitelisted','impression','score_old','model','sentences/topics','Review','Model Accuracy','Error Code','URL Level Feedback','Concept Reviewer','isBrandSafe_cri