-- Silver Layer: Cleaned, validated, and standardized data
-- Implements data quality rules and basic transformations

-- Create schema
CREATE SCHEMA IF NOT EXISTS FOREX_DATA;
CREATE SCHEMA IF NOT EXISTS FOREX_DATA.SILVER;

-- Validated and cleaned forex rates
CREATE OR REPLACE TABLE FOREX_DATA.SILVER.forex_rates (
    date DATE NOT NULL,
    currency_pair VARCHAR(7) NOT NULL,
    open_rate FLOAT NOT NULL,
    high_rate FLOAT NOT NULL,
    low_rate FLOAT NOT NULL,
    close_rate FLOAT NOT NULL,
    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (date, currency_pair)
);

-- Create a view for analytics with computed columns
CREATE OR REPLACE VIEW FOREX_DATA.SILVER.forex_rates_analytics AS
SELECT 
    date,
    currency_pair,
    open_rate,
    high_rate,
    low_rate,
    close_rate,
    -- Time components
    YEAR(date) as year,
    MONTH(date) as month,
    QUARTER(date) as quarter,
    DAYOFWEEK(date) as day_of_week,
    -- Daily price movement
    (close_rate - open_rate) as daily_change,
    ((close_rate - open_rate) / open_rate * 100) as daily_change_percent,
    -- Daily volatility
    (high_rate - low_rate) as daily_range,
    ((high_rate - low_rate) / open_rate * 100) as daily_range_percent,
    inserted_at
FROM FOREX_DATA.SILVER.forex_rates;

-- Load data from PUBLIC schema with validation
INSERT INTO FOREX_DATA.SILVER.forex_rates (
    date,
    currency_pair,
    open_rate,
    high_rate,
    low_rate,
    close_rate
)
SELECT DISTINCT
    CAST(TRIM(date) AS DATE),
    'EUR/USD' as currency_pair,
    NULLIF(TRIM(open_rate), '')::FLOAT,
    NULLIF(TRIM(high_rate), '')::FLOAT,
    NULLIF(TRIM(low_rate), '')::FLOAT,
    NULLIF(TRIM(close_rate), '')::FLOAT
FROM PUBLIC.FOREX_RATES_EUR_USD
WHERE 
    date IS NOT NULL
    AND open_rate > 0
    AND high_rate > 0
    AND low_rate > 0
    AND close_rate > 0
    AND high_rate >= low_rate
    AND open_rate >= low_rate 
    AND open_rate <= high_rate
    AND close_rate >= low_rate 
    AND close_rate <= high_rate
    AND date >= '2000-01-01' 
    AND date <= CURRENT_DATE();

INSERT INTO FOREX_DATA.SILVER.forex_rates (
    date,
    currency_pair,
    open_rate,
    high_rate,
    low_rate,
    close_rate
)
SELECT DISTINCT
    CAST(TRIM(date) AS DATE),
    'EUR/EGP' as currency_pair,
    NULLIF(TRIM(open_rate), '')::FLOAT,
    NULLIF(TRIM(high_rate), '')::FLOAT,
    NULLIF(TRIM(low_rate), '')::FLOAT,
    NULLIF(TRIM(close_rate), '')::FLOAT
FROM PUBLIC.FOREX_RATES_EUR_EGP
WHERE 
    date IS NOT NULL
    AND open_rate > 0
    AND high_rate > 0
    AND low_rate > 0
    AND close_rate > 0
    AND high_rate >= low_rate
    AND open_rate >= low_rate 
    AND open_rate <= high_rate
    AND close_rate >= low_rate 
    AND close_rate <= high_rate
    AND date >= '2000-01-01' 
    AND date <= CURRENT_DATE();

INSERT INTO FOREX_DATA.SILVER.forex_rates (
    date,
    currency_pair,
    open_rate,
    high_rate,
    low_rate,
    close_rate
)
SELECT DISTINCT
    CAST(TRIM(date) AS DATE),
    'USD/EGP' as currency_pair,
    NULLIF(TRIM(open_rate), '')::FLOAT,
    NULLIF(TRIM(high_rate), '')::FLOAT,
    NULLIF(TRIM(low_rate), '')::FLOAT,
    NULLIF(TRIM(close_rate), '')::FLOAT
FROM PUBLIC.FOREX_RATES_USD_EGP
WHERE 
    date IS NOT NULL
    AND open_rate > 0
    AND high_rate > 0
    AND low_rate > 0
    AND close_rate > 0
    AND high_rate >= low_rate
    AND open_rate >= low_rate 
    AND open_rate <= high_rate
    AND close_rate >= low_rate 
    AND close_rate <= high_rate
    AND date >= '2000-01-01' 
    AND date <= CURRENT_DATE(); 



    -- Check the data
SELECT currency_pair, COUNT(*) 
FROM FOREX_DATA.SILVER.forex_rates 
GROUP BY currency_pair;

-- Check the analytics
SELECT * 
FROM FOREX_DATA.SILVER.forex_rates_analytics 
ORDER BY date DESC 
LIMIT 5;