-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS SILVER;

-- Create cleaned forex rates table
CREATE OR REPLACE TABLE SILVER.FOREX_RATES_CLEAN (
    currency_pair STRING,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    mid_price FLOAT,
    daily_range FLOAT,
    is_valid BOOLEAN,
    processed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create view with data quality checks
CREATE OR REPLACE VIEW SILVER.VW_FOREX_RATES_VALIDATED AS
SELECT 
    currency_pair,
    date,
    open,
    high,
    low,
    close,
    (high + low) / 2 as mid_price,
    high - low as daily_range,
    CASE 
        WHEN high >= low 
        AND open BETWEEN low AND high
        AND close BETWEEN low AND high
        AND high > 0 AND low > 0
        THEN TRUE 
        ELSE FALSE 
    END as is_valid
FROM BRONZE.FOREX_RATES;

-- Create task to merge validated data
CREATE OR REPLACE TASK SILVER.TASK_MERGE_FOREX_RATES
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.FOREX_RATES_STREAM')
AS
MERGE INTO SILVER.FOREX_RATES_CLEAN target
USING SILVER.VW_FOREX_RATES_VALIDATED source
    ON target.currency_pair = source.currency_pair
    AND target.date = source.date
WHEN MATCHED THEN
    UPDATE SET
        open = source.open,
        high = source.high,
        low = source.low,
        close = source.close,
        mid_price = source.mid_price,
        daily_range = source.daily_range,
        is_valid = source.is_valid,
        processed_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        currency_pair, date, open, high, low, close,
        mid_price, daily_range, is_valid, processed_timestamp
    )
    VALUES (
        source.currency_pair, source.date, source.open, source.high, source.low, source.close,
        source.mid_price, source.daily_range, source.is_valid, CURRENT_TIMESTAMP()
    ); 