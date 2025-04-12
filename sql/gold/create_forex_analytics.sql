-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Create forex analytics table
CREATE OR REPLACE TABLE GOLD.FOREX_ANALYTICS (
    currency_pair STRING,
    date DATE,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    daily_return_pct FLOAT,
    daily_volatility FLOAT,
    ma_20 FLOAT,
    rsi_14 FLOAT,
    processed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create view for technical analysis
CREATE OR REPLACE VIEW GOLD.VW_FOREX_TECHNICAL_ANALYSIS AS
WITH price_changes AS (
    SELECT 
        currency_pair,
        date,
        open,
        close,
        high,
        low,
        close - LAG(close) OVER (PARTITION BY currency_pair ORDER BY date) as price_change
    FROM SILVER.FOREX_RATES_CLEAN
    WHERE is_valid = TRUE
),
gains_losses AS (
    SELECT 
        *,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gains,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as losses
    FROM price_changes
)
SELECT 
    currency_pair,
    date,
    open,
    close,
    high,
    low,
    -- Daily return percentage
    ((close - open) / open) * 100 as daily_return_pct,
    
    -- Daily volatility (high-low range as percentage)
    ((high - low) / open) * 100 as daily_volatility,
    
    -- 20-day moving average
    AVG(close) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma_20,
    
    -- RSI 14-day
    100 - (100 / (1 + (
        AVG(gains) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) /
        NULLIF(AVG(losses) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 0)
    ))) as rsi_14
FROM gains_losses;

-- Create task to update analytics
CREATE OR REPLACE TASK GOLD.TASK_UPDATE_FOREX_ANALYTICS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '30 MINUTE'
AS
MERGE INTO GOLD.FOREX_ANALYTICS target
USING GOLD.VW_FOREX_TECHNICAL_ANALYSIS source
    ON target.currency_pair = source.currency_pair
    AND target.date = source.date
WHEN MATCHED THEN
    UPDATE SET
        open = source.open,
        close = source.close,
        high = source.high,
        low = source.low,
        daily_return_pct = source.daily_return_pct,
        daily_volatility = source.daily_volatility,
        ma_20 = source.ma_20,
        rsi_14 = source.rsi_14,
        processed_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        currency_pair, date, open, close, high, low,
        daily_return_pct, daily_volatility, ma_20, rsi_14
    )
    VALUES (
        source.currency_pair, source.date, source.open, source.close, source.high, source.low,
        source.daily_return_pct, source.daily_volatility, source.ma_20, source.rsi_14
    ); 