-- Gold Layer: Business-ready analytics views
-- Provides cleaned, transformed data ready for analysis

-- Create schema
CREATE SCHEMA IF NOT EXISTS FOREX_DATA;
CREATE SCHEMA IF NOT EXISTS FOREX_DATA.GOLD;

-- 1. Daily rates with cross-rate validation
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.daily_rates AS
SELECT 
    e_u.date,
    e_u.close_rate as eur_usd_rate,
    e_e.close_rate as eur_egp_rate,
    u_e.close_rate as usd_egp_rate,
    -- Calculate cross-rates validation
    e_e.close_rate / e_u.close_rate as calculated_usd_egp_rate,
    -- Calculate difference percentage
    ABS((e_e.close_rate / e_u.close_rate - u_e.close_rate) / u_e.close_rate) * 100 as rate_difference_percent
FROM FOREX_DATA.SILVER.forex_rates e_u
JOIN FOREX_DATA.SILVER.forex_rates e_e 
    ON e_u.date = e_e.date 
    AND e_u.currency_pair = 'EUR/USD' 
    AND e_e.currency_pair = 'EUR/EGP'
JOIN FOREX_DATA.SILVER.forex_rates u_e 
    ON e_u.date = u_e.date 
    AND u_e.currency_pair = 'USD/EGP';

-- 2. Currency converter with all possible combinations
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.currency_converter AS
WITH base_rates AS (
    -- Direct rates
    SELECT 
        date,
        SPLIT_PART(currency_pair, '/', 1) as from_currency,
        SPLIT_PART(currency_pair, '/', 2) as to_currency,
        close_rate as rate
    FROM FOREX_DATA.SILVER.forex_rates
    
    UNION ALL
    
    -- Inverse rates
    SELECT 
        date,
        SPLIT_PART(currency_pair, '/', 2) as from_currency,
        SPLIT_PART(currency_pair, '/', 1) as to_currency,
        1/close_rate as rate
    FROM FOREX_DATA.SILVER.forex_rates
)
SELECT 
    date,
    from_currency,
    to_currency,
    rate,
    1/rate as inverse_rate
FROM base_rates;

-- 3. Daily changes and volatility metrics
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.daily_changes AS
WITH daily_stats AS (
    SELECT 
        currency_pair,
        date,
        close_rate,
        LAG(close_rate) OVER (PARTITION BY currency_pair ORDER BY date) as prev_close,
        high_rate/low_rate - 1 as daily_volatility
    FROM FOREX_DATA.SILVER.forex_rates
)
SELECT 
    currency_pair,
    date,
    close_rate as current_rate,
    prev_close as previous_rate,
    ((close_rate - prev_close) / prev_close) * 100 as daily_change_percent,
    daily_volatility * 100 as intraday_volatility_percent
FROM daily_stats
WHERE prev_close IS NOT NULL;

-- 4. Monthly average rates
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.monthly_averages AS
SELECT 
    currency_pair,
    DATE_TRUNC('month', date) as month,
    AVG(close_rate) as avg_rate,
    MIN(low_rate) as lowest_rate,
    MAX(high_rate) as highest_rate,
    AVG(high_rate/low_rate - 1) * 100 as avg_daily_volatility_percent
FROM FOREX_DATA.SILVER.forex_rates
GROUP BY currency_pair, DATE_TRUNC('month', date);

-- 5. Rate correlations
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.currency_correlations AS
WITH daily_changes AS (
    SELECT 
        date,
        currency_pair,
        (close_rate - LAG(close_rate) OVER (PARTITION BY currency_pair ORDER BY date)) 
        / LAG(close_rate) OVER (PARTITION BY currency_pair ORDER BY date) as daily_return
    FROM FOREX_DATA.SILVER.forex_rates
)
SELECT 
    p1.currency_pair as pair1,
    p2.currency_pair as pair2,
    CORR(p1.daily_return, p2.daily_return) as correlation
FROM daily_changes p1
JOIN daily_changes p2 
    ON p1.date = p2.date 
    AND p1.currency_pair < p2.currency_pair
GROUP BY p1.currency_pair, p2.currency_pair
HAVING COUNT(*) >= 20;

-- Check daily rates with validation
SELECT * FROM FOREX_DATA.GOLD.daily_rates 
WHERE date = CURRENT_DATE;

-- Convert currencies
SELECT * FROM FOREX_DATA.GOLD.currency_converter 
WHERE date = CURRENT_DATE 
AND from_currency = 'EUR' 
AND to_currency = 'EGP';

-- Check volatility
SELECT * FROM FOREX_DATA.GOLD.daily_changes 
ORDER BY intraday_volatility_percent DESC 
LIMIT 10;

-- Monthly trends
SELECT * FROM FOREX_DATA.GOLD.monthly_averages 
ORDER BY month DESC;

-- Currency correlations
SELECT * FROM FOREX_DATA.GOLD.currency_correlations 
ORDER BY ABS(correlation) DESC;