create database stockdb;

create external table if not exists stockdb.symbol_data(
    ts timestamp,
    close decimal,
    Volume bigint,
    symbol string )
stored as PARQUET
LOCATION 's3://symboldatabucket86/symboldata/';

SELECT * from stockdb.symbol_data;