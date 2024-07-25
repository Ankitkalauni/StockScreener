WITH temp_max_vol AS (
    SELECT 
        symbol AS _symbol, 
        approx_percentile(close, 0.5) AS median_price, 
        max(volume) AS _volume, 
        min(close) AS min_price, 
        max(close) AS max_price
    FROM 
        stockdb.symbol_data
    WHERE 
        ts BETWEEN date_add('day', -64, current_date) 
            AND date_add('day', -3, current_date)
    GROUP BY 
        symbol
), 
_temp_max_vol AS (
    SELECT 
        *, 
        ((max_price - min_price) / min_price) AS diff_price
    FROM 
        temp_max_vol
), 
_temp_vbreakout AS (
    SELECT 
        a.*,
        b.median_price,
        b._volume,
        b.min_price,
        b.max_price,
        ROW_NUMBER() OVER (PARTITION BY a.symbol ORDER BY a.ts ASC) AS row_num
    FROM 
        stockdb.symbol_data a 
    INNER JOIN 
        _temp_max_vol b 
    ON 
        a.symbol = b._symbol
    WHERE 
        a.ts BETWEEN date_add('hour', 6, date_add('day', -3, now())) 
            AND current_date
        AND a.volume > b._volume 
        AND a.close > b.median_price
)
SELECT 
    symbol, 
    COUNT(1) AS breakout_cnt,
    MAX((close - median_price) / median_price) AS max_price_diff,
    MAX(volume) AS max_breakout_volume,
    MAX((volume - _volume) / volume) AS max_volume_diff,
    MAX(close) AS max_breakout_price,
    MIN_BY(min_price, row_num) AS min_price,
    MIN_BY(max_price, row_num) AS max_price,
    MIN_BY(median_price, row_num) AS median_price,
    MAX(ts) AS latest_breakout_ts
FROM 
    _temp_vbreakout
GROUP BY 
    symbol
HAVING 
    date(max(ts)) = current_date;