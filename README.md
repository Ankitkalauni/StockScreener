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




#Steps to follow (raw)

1.setup the nse_data_dumper.py file and check the config.json and output_data/symbol_data.parquet (if there)
2.change/fix the volumne of the docker-compose file to the proper location of the scripts and data
3.change/fix the dag script to point to the proper location of python scirpt (location will be the docker location not the local location)
4.add REQUIREMENT.TXT to the docker-compose file (not recommended but works for small projects)
5. make .env file and add

```
AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
AIRFLOW_UID=50000
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=ap-south-1

```
and add this variable to the docker compose file
6.install boto3 or add to the req file
7. add spark image to the docker-compose file and add the aws access key to the env variable and create a network to interact with containers.
8. if getting error while reading the data from s3 to the pyspark. try to see the aws configs, if need add a jar or edit the `spark-defaults.conf` and use `s3a://` as hadoop aws uses this.

```
# Define the S3 path
s3_path = "s3a://symboldatabucket86/symboldata/all_symbols_data.parquet"

# Read data from S3
df = spark.read.parquet(s3_path)
```

9. if using ec2 instance we can use IAM roles.
