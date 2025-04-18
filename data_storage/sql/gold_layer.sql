-- Gold Layer: Business-ready analytics views and tables
-- Provides analytics-ready datasets for dashboards, reports, and KPIs

-- Create schema
CREATE SCHEMA IF NOT EXISTS gold;

-- 1. Fact table: Daily Exchange Rates
CREATE TABLE IF NOT EXISTS gold.fact_daily_rates (
    date_key DATE NOT NULL,
    currency_pair_key VARCHAR(7) NOT NULL,
    open_rate FLOAT NOT NULL,
    high_rate FLOAT NOT NULL,
    low_rate FLOAT NOT NULL,
    close_rate FLOAT NOT NULL,
    daily_change_amount FLOAT,
    daily_change_percent FLOAT,
    volatility_percent FLOAT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, currency_pair_key)
);

-- Create index for better performance
CREATE INDEX IF NOT EXISTS idx_fact_daily_rates_date ON gold.fact_daily_rates(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_daily_rates_currency ON gold.fact_daily_rates(currency_pair_key);

-- 2. Dimension table: Currency Pairs
CREATE TABLE IF NOT EXISTS gold.dim_currency_pairs (
    currency_pair_key VARCHAR(7) PRIMARY KEY,
    base_currency CHAR(3),
    quote_currency CHAR(3),
    pair_description VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- 3. Dimension table: Calendar
CREATE TABLE IF NOT EXISTS gold.dim_calendar (
    date_key DATE PRIMARY KEY,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    week_number INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Load dimension data
INSERT INTO gold.dim_currency_pairs (currency_pair_key, base_currency, quote_currency, pair_description)
VALUES 
    ('EUR/USD', 'EUR', 'USD', 'Euro to US Dollar'),
    ('EUR/EGP', 'EUR', 'EGP', 'Euro to Egyptian Pound'),
    ('USD/EGP', 'USD', 'EGP', 'US Dollar to Egyptian Pound')
ON CONFLICT (currency_pair_key) DO NOTHING;

-- Load calendar dimension (for next 5 years)
INSERT INTO gold.dim_calendar (
    date_key, year, quarter, month, month_name, 
    week_number, day_of_week, day_name, is_weekend
)
SELECT 
    datum AS date_key,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(WEEK FROM datum) AS week_number,
    EXTRACT(DOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM datum) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT generate_series(
        CURRENT_DATE,
        CURRENT_DATE + INTERVAL '5 years',
        '1 day'::interval
    )::date AS datum
) dates
ON CONFLICT (date_key) DO NOTHING;

-- Load fact data
INSERT INTO gold.fact_daily_rates (
    date_key,
    currency_pair_key,
    open_rate,
    high_rate,
    low_rate,
    close_rate,
    daily_change_amount,
    daily_change_percent,
    volatility_percent
)
SELECT 
    date,
    currency_pair,
    open_rate,
    high_rate,
    low_rate,
    close_rate,
    close_rate - open_rate,
    ((close_rate - open_rate) / open_rate) * 100,
    ((high_rate - low_rate) / open_rate) * 100
FROM silver.forex_rates
ON CONFLICT (date_key, currency_pair_key) DO UPDATE SET
    open_rate = EXCLUDED.open_rate,
    high_rate = EXCLUDED.high_rate,
    low_rate = EXCLUDED.low_rate,
    close_rate = EXCLUDED.close_rate,
    daily_change_amount = EXCLUDED.daily_change_amount,
    daily_change_percent = EXCLUDED.daily_change_percent,
    volatility_percent = EXCLUDED.volatility_percent,
    updated_at = CURRENT_TIMESTAMP;

-- 4. Analytics Views

-- 4.1 Exchange Rate Analysis
CREATE OR REPLACE VIEW gold.exchange_rate_analysis AS
SELECT 
    f.date_key,
    c.year,
    c.month,
    c.quarter,
    c.month_name,
    p.currency_pair_key,
    p.base_currency,
    p.quote_currency,
    f.close_rate as exchange_rate,
    f.daily_change_percent,
    f.volatility_percent,
    LAG(f.close_rate) OVER (PARTITION BY f.currency_pair_key ORDER BY f.date_key) as prev_day_rate,
    LAG(f.close_rate, 7) OVER (PARTITION BY f.currency_pair_key ORDER BY f.date_key) as prev_week_rate,
    LAG(f.close_rate, 30) OVER (PARTITION BY f.currency_pair_key ORDER BY f.date_key) as prev_month_rate
FROM gold.fact_daily_rates f
JOIN gold.dim_calendar c ON f.date_key = c.date_key
JOIN gold.dim_currency_pairs p ON f.currency_pair_key = p.currency_pair_key;

-- 4.2 Rate Changes
CREATE OR REPLACE VIEW gold.rate_changes AS
SELECT 
    date_key,
    currency_pair_key,
    close_rate,
    LAG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as prev_day_rate,
    LAG(close_rate, 7) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as prev_week_rate,
    LAG(close_rate, 30) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as prev_month_rate,
    daily_change_percent,
    ((close_rate - LAG(close_rate, 7) OVER (PARTITION BY currency_pair_key ORDER BY date_key)) / 
     NULLIF(LAG(close_rate, 7) OVER (PARTITION BY currency_pair_key ORDER BY date_key), 0)) * 100 as weekly_change_pct,
    ((close_rate - LAG(close_rate, 30) OVER (PARTITION BY currency_pair_key ORDER BY date_key)) / 
     NULLIF(LAG(close_rate, 30) OVER (PARTITION BY currency_pair_key ORDER BY date_key), 0)) * 100 as monthly_change_pct
FROM gold.fact_daily_rates;

-- 4.3 Volatility Analysis
CREATE OR REPLACE VIEW gold.volatility_metrics AS
SELECT 
    DATE_TRUNC('month', date_key)::DATE as month_start,
    currency_pair_key,
    AVG(volatility_percent) as avg_daily_volatility,
    MAX(volatility_percent) as max_daily_volatility,
    MIN(volatility_percent) as min_daily_volatility,
    STDDEV(daily_change_percent) as std_dev_daily_change
FROM gold.fact_daily_rates
GROUP BY DATE_TRUNC('month', date_key)::DATE, currency_pair_key;

-- 4.4 Cross-Rate Analysis
CREATE OR REPLACE VIEW gold.cross_rates AS
SELECT 
    e_u.date_key,
    e_u.close_rate as eur_usd_rate,
    e_e.close_rate as eur_egp_rate,
    u_e.close_rate as usd_egp_rate,
    e_e.close_rate / e_u.close_rate as calculated_usd_egp_rate,
    ABS((e_e.close_rate / e_u.close_rate - u_e.close_rate) / NULLIF(u_e.close_rate, 0)) * 100 as cross_rate_diff_pct
FROM gold.fact_daily_rates e_u
JOIN gold.fact_daily_rates e_e 
    ON e_u.date_key = e_e.date_key 
    AND e_u.currency_pair_key = 'EUR/USD' 
    AND e_e.currency_pair_key = 'EUR/EGP'
JOIN gold.fact_daily_rates u_e 
    ON e_u.date_key = u_e.date_key 
    AND u_e.currency_pair_key = 'USD/EGP';

-- 4.5 Moving Averages
CREATE OR REPLACE VIEW gold.moving_averages AS
SELECT 
    date_key,
    currency_pair_key,
    close_rate,
    AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7_day,
    AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as ma_30_day,
    AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as ma_90_day
FROM gold.fact_daily_rates;

-- 4.6 Technical Analysis - RSI
CREATE OR REPLACE VIEW gold.technical_rsi AS
WITH price_changes AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        close_rate - LAG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as price_change
    FROM gold.fact_daily_rates
),
gains_losses AS (
    SELECT 
        date_key,
        currency_pair_key,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
    FROM price_changes
),
avg_gains_losses AS (
    SELECT 
        date_key,
        currency_pair_key,
        AVG(gain) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 13 PRECEDING) as avg_gain,
        AVG(loss) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 13 PRECEDING) as avg_loss
    FROM gains_losses
)
SELECT 
    date_key,
    currency_pair_key,
    CASE 
        WHEN avg_loss = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0))))
    END as rsi_14
FROM avg_gains_losses;

-- 4.7 Technical Analysis - MACD
CREATE OR REPLACE VIEW gold.technical_macd AS
WITH price_data AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 11 PRECEDING) as sma_12,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 25 PRECEDING) as sma_26
    FROM gold.fact_daily_rates
),
ema_calc AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        (close_rate * (2.0/13) + LAG(sma_12) OVER (PARTITION BY currency_pair_key ORDER BY date_key) * (1 - (2.0/13))) as ema_12,
        (close_rate * (2.0/27) + LAG(sma_26) OVER (PARTITION BY currency_pair_key ORDER BY date_key) * (1 - (2.0/27))) as ema_26
    FROM price_data
),
macd_base AS (
    SELECT 
        date_key,
        currency_pair_key,
        ema_12,
        ema_26,
        (ema_12 - ema_26) as macd_line
    FROM ema_calc
)
SELECT 
    date_key,
    currency_pair_key,
    ema_12,
    ema_26,
    macd_line,
    AVG(macd_line) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 8 PRECEDING) as signal_line,
    macd_line - AVG(macd_line) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 8 PRECEDING) as macd_histogram
FROM macd_base;

-- 4.6 Technical Analysis - EMA
DROP VIEW IF EXISTS gold.technical_ema CASCADE;
CREATE OR REPLACE VIEW gold.technical_ema(
    date_key,
    currency_pair_key,
    close_rate,
    ema_9,
    ema_22,
    ema_50
) AS
WITH ema_calc AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        -- Calculate SMAs first as base for EMAs
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 8 PRECEDING) as sma_9,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 21 PRECEDING) as sma_22,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 49 PRECEDING) as sma_50
    FROM gold.fact_daily_rates
)
SELECT 
    date_key,
    currency_pair_key,
    close_rate,
    -- EMA 9 (short-term)
    (close_rate * (2.0/10) + LAG(sma_9) OVER (PARTITION BY currency_pair_key ORDER BY date_key) * (1 - (2.0/10))) as ema_9,
    -- EMA 22 (medium-term)
    (close_rate * (2.0/23) + LAG(sma_22) OVER (PARTITION BY currency_pair_key ORDER BY date_key) * (1 - (2.0/23))) as ema_22,
    -- EMA 50 (long-term)
    (close_rate * (2.0/51) + LAG(sma_50) OVER (PARTITION BY currency_pair_key ORDER BY date_key) * (1 - (2.0/51))) as ema_50
FROM ema_calc;

-- 4.8 Technical Analysis - Combined Analysis
DROP VIEW IF EXISTS gold.technical_analysis CASCADE;
CREATE OR REPLACE VIEW gold.technical_analysis(
    date_key,
    currency_pair_key,
    close_rate,
    daily_change_percent,
    rsi_14,
    ema_9,
    ema_22,
    ema_50,
    macd_line,
    signal_line,
    macd_histogram,
    rsi_signal,
    trend_signal,
    macd_signal
) AS
SELECT 
    f.date_key,
    f.currency_pair_key,
    f.close_rate,
    f.daily_change_percent,
    r.rsi_14,
    e.ema_9,
    e.ema_22,
    e.ema_50,
    m.macd_line,
    m.signal_line,
    m.macd_histogram,
    -- Trading signals
    CASE 
        WHEN r.rsi_14 < 30 THEN 'Oversold'
        WHEN r.rsi_14 > 70 THEN 'Overbought'
        ELSE 'Neutral'
    END as rsi_signal,
    CASE 
        WHEN e.ema_9 > e.ema_22 AND e.ema_22 > e.ema_50 THEN 'Strong Uptrend'
        WHEN e.ema_9 < e.ema_22 AND e.ema_22 < e.ema_50 THEN 'Strong Downtrend'
        WHEN e.ema_9 > e.ema_22 THEN 'Potential Uptrend'
        WHEN e.ema_9 < e.ema_22 THEN 'Potential Downtrend'
        ELSE 'Neutral'
    END as trend_signal,
    CASE 
        WHEN m.macd_line > m.signal_line AND m.macd_histogram > 0 THEN 'Buy'
        WHEN m.macd_line < m.signal_line AND m.macd_histogram < 0 THEN 'Sell'
        ELSE 'Hold'
    END as macd_signal
FROM gold.fact_daily_rates f
LEFT JOIN gold.technical_rsi r ON f.date_key = r.date_key AND f.currency_pair_key = r.currency_pair_key
LEFT JOIN gold.technical_ema e ON f.date_key = e.date_key AND f.currency_pair_key = e.currency_pair_key
LEFT JOIN gold.technical_macd m ON f.date_key = m.date_key AND f.currency_pair_key = m.currency_pair_key
WHERE f.date_key >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY f.date_key DESC, f.currency_pair_key;

