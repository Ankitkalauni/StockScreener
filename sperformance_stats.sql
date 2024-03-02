-- From 30th Jan'23 to 26th Jan'24
-- 2023-01-30 to 2024-01-26

-- select * from misc.performance_stats_20240131_ak 


drop table if exists misc.performance_stats_20240131_ak_chn;
drop table if exists misc.performance_stats_20240131_ak;
drop table if exists misc.performance_stats_20240131_akre_v1;


create table misc.performance_stats_20240131_ak_chn
select CAST(from_utc_timestamp(from_unixtime(unix_timestamp(date, 'yyyy-MM-dd HH:mm:ssXXX')), 'Asia/Kolkata') AS STRING) AS formatted_timestamp, close, volume, symbol
from misc.performance_stats_20240131_ak;



-- per day (7 ROWS INTOTAL) 6 hrs data (EXCLUDING 9:15 DATA)


-- https://vogon.reports.mn/query/results?id=vogon-e7d60e30-0513-4a0b-820e-f03f19491b2c

with
temp_symbol_max_vol as (
    select symbol _symbol, max(volume) _volume, max(close) _close
    from misc.performance_stats_20240131_ak_chn
    where formatted_timestamp between '2023-09-26 09:15:00' and '2024-01-06 15:15:00'
    group by 1
)

select *
from misc.performance_stats_20240131_ak_chn a 
inner join temp_symbol_max_vol b 
on a.symbol = b._symbol
where a.formatted_timestamp between '2024-01-07 09:15:00' and '2024-01-30 15:15:00' and a.volume > b._volume and a.close > b._close
order by a.volume/b._volume desc





with
temp_symbol_max_vol as (
    select symbol _symbol, max(volume) _volume, max(close) _close
    from misc.performance_stats_20240131_ak_chn
    where formatted_timestamp between '2023-09-26 09:15:00' and '2024-01-06 15:15:00'
    group by 1
)

select a.symbol, max(formatted_timestamp) max_formatted_timestamp
from misc.performance_stats_20240131_ak_chn a 
inner join temp_symbol_max_vol b 
on a.symbol = b._symbol
where a.formatted_timestamp between '2024-01-07 09:15:00' and '2024-01-30 15:15:00' and a.volume > b._volume and a.close > b._close
-- order by a.volume/b._volume desc
group by 1




with
temp_symbol_max_vol as (
    select symbol _symbol, max(volume) _volume, max(close) _close
    from misc.performance_stats_20240131_ak_chn
    where formatted_timestamp between '2023-09-26 09:15:00' and '2024-01-06 15:15:00' and symbol = 'ACC.NS'
    group by 1
)

select *
from misc.performance_stats_20240131_ak_chn a 
inner join temp_symbol_max_vol b 
on a.symbol = b._symbol
where a.formatted_timestamp between '2024-01-07 09:15:00' and '2024-01-30 15:15:00' and a.volume > b._volume and a.close > b._close and symbol = 'ACC.NS'
order by a.volume/b._volume desc









-----------------------------------------------------------
-- updated data till budget +1 day  

drop table if exists misc.performance_stats_20240131_ak_chn;
create table misc.performance_stats_20240131_ak_chn
select CAST(from_utc_timestamp(from_unixtime(unix_timestamp(date, 'yyyy-MM-dd HH:mm:ssXXX')), 'Asia/Kolkata') AS STRING) AS formatted_timestamp, close, volume, symbol
from misc.performance_stats_20240131_ak;

-- create table misc.performance_stats_20240131_akre_v1
-- select CAST(from_utc_timestamp(from_unixtime(unix_timestamp(date, 'yyyy-MM-dd HH:mm:ssXXX')), 'Asia/Kolkata') AS STRING) AS formatted_timestamp, close, volume, symbol
-- from misc.performance_stats_20240131_ak_v1;



-- 1. check for the max volumn for last 3-6 months 
-- 2. get the min & max of price action, then keep (max-min/min) as difference 











set end = '2024-01-29 15:15:00';
set start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')-24*61*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


set _start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+18*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 
set _end = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+24*24*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


with temp_max_vol as (  
    select symbol _symbol, percentile(cast(close as bigint), 0.5) median_price,max(volume) _volume, min(close) min_price, max(close) max_price
    from misc.performance_stats_20240131_akre_v1
    where formatted_timestamp between ${start} and ${end}
    group by 1

), 
_temp_max_vol as (
    select *, ((max_price-min_price)/min_price) diff_price
    from temp_max_vol
)
select *
from misc.performance_stats_20240131_akre_v1 a 
inner join _temp_max_vol b 
where a.formatted_timestamp  between ${_start} and ${_end} and a.symbol = b._symbol  and a.volume > b._volume and a.close > median_price












drop table if exists misc.performance_stats_20240131_ak_chn;
create table misc.performance_stats_20240131_ak_chn
select CAST(from_utc_timestamp(from_unixtime(unix_timestamp(date, 'yyyy-MM-dd HH:mm:ssXXX')), 'Asia/Kolkata') AS STRING) AS formatted_timestamp, close, volume, symbol
from misc.performance_stats_20240131_ak;

-- checking if last 60 days median volumne is less than last 15 days median volumn and the price has not increased yet.


/*
check if 
volume after the "END" is greater than
the max volume of last 2 months from "END".

check if
close price after "END" is greater than
the median price of last 2 months from "END"


*/


set tbl_name = performance_stats_20240131_ak_chn;
set end = '2024-02-08 15:15:00';
set start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')-24*61*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


set _start = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+18*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 
set _end = cast(from_unixtime(unix_timestamp( ${end} , 'yyyy-MM-dd HH:mm:ss')+24*24*3600, 'yyyy-MM-dd HH:mm:ssXXX') as string); 


with temp_max_vol as (  
    select symbol _symbol, percentile(cast(close as bigint), 0.5) median_price,
    percentile(cast(volume as bigint), 0.5) median_volumn,
    max(volume) _volume, min(close) min_price, max(close) max_price
    from misc.${tbl_name}
    where formatted_timestamp between ${start} and ${end}
    group by 1

)
select *
from misc.${tbl_name} a 
inner join temp_max_vol b 
where a.formatted_timestamp  between ${_start} and ${_end} and a.symbol = b._symbol  and a.volume > b._volume and a.close > median_price
