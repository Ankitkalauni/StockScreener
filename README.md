# Aone-Auto-D



**yahoo_valid_symbol_datacrawler.py**

Get the valid listed stocks name with symbols from Yahoo and NSE (Intersect).
Takes `market_symbols.csv` which is downloaded from NSE (list of stock/company name)
Hits the Yahoo finance API and get the valid symbol name.
export valid symbol to `market_value.csv`


After manual clean `market_value.csv` --> `valid_symbols.csv`


**yahoo_nse_datacrawler.py**
hit Yahoo API to get the data for each stock (hourly)
and store the data to `stockdata_all.csv`

```
change this line 26 and 27 in the file for data range

end_date = datetime.today()
start_date = end_date - timedelta(days=2)
```
