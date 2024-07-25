create database stockdb;

create external table if not exists stockdb.symbol_data(
    ts timestamp,
    close decimal,
    Volume bigint,
    symbol string )
stored as PARQUET
LOCATION 's3://symboldatabucket86/symboldata/';

SELECT * from stockdb.symbol_data;




--------------- testing (SparkSQL) --------------------------

-- set end = current_date() - 3 days;
-- set start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')-24*61*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


-- set _start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+18*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 
-- set _end = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+24*24*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


-- with temp_max_vol as (  
--     select symbol _symbol, percentile(cast(close as bigint), 0.5) median_price,max(volume) _volume, min(close) min_price, max(close) max_price
--     from stockdb.symbol_data
--     where ts between ${start} and ${end}
--     group by 1

-- ), 
-- _temp_max_vol as (
--     select *, ((max_price-min_price)/min_price) diff_price
--     from temp_max_vol
-- )
-- select *
-- from stockdb.symbol_data a 
-- inner join _temp_max_vol b 
-- where a.ts  between ${_start} and ${_end} and a.symbol = b._symbol  and a.volume > b._volume and a.close > median_price



-- drop table if exists misc.performance_stats_20240131_ak_chn;
-- create table misc.performance_stats_20240131_ak_chn
-- select CAST(from_utc_timestamp(from_unixtime(unix_timestamp(date, 'yyyy-MM-dd HH:mm:ssXXX')), 'Asia/Kolkata') AS STRING) AS ts, close, volume, symbol
-- from misc.performance_stats_20240131_ak;

-- checking if last 60 days median volumne is less than last 15 days median volumn and the price has not increased yet.


/*
check if 
volume after the "END" is greater than
the max volume of last 2 months from "END".

check if
close price after "END" is greater than
the median price of last 2 months from "END"


*/


-- set tbl_name = performance_stats_20240131_ak_chn;
-- set end = '2024-03-04 15:15:00';
-- set start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')-24*61*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


-- set _start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+18*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 
-- set _end = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+24*24*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


-- with temp_max_vol as (  
--     select symbol _symbol, percentile(cast(close as bigint), 0.5) median_price,
--     percentile(cast(volume as bigint), 0.5) median_volumn,
--     max(volume) _volume, min(close) min_price, max(close) max_price
--     from misc.${tbl_name}
--     where ts between ${start} and ${end}
--     group by 1

-- )
-- select *
-- from misc.${tbl_name} a 
-- inner join temp_max_vol b 
-- where a.ts  between ${_start} and ${_end} and a.symbol = b._symbol  and a.volume > b._volume and a.close > median_price
