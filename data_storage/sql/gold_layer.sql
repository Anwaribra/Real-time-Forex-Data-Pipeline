-- Gold Layer: Business-ready analytics views and tables
-- Provides analytics-ready datasets for dashboards, reports, and KPIs

-- Create schema
CREATE SCHEMA IF NOT EXISTS FOREX_DATA;
CREATE SCHEMA IF NOT EXISTS FOREX_DATA.GOLD;

-- 1. Fact table: Daily Exchange Rates
CREATE OR REPLACE TABLE FOREX_DATA.GOLD.fact_daily_rates (
    date_key DATE NOT NULL,
    currency_pair_key VARCHAR(7) NOT NULL,
    open_rate FLOAT NOT NULL,
    high_rate FLOAT NOT NULL,
    low_rate FLOAT NOT NULL,
    close_rate FLOAT NOT NULL,
    daily_change_amount FLOAT,
    daily_change_percent FLOAT,
    volatility_percent FLOAT,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (date_key, currency_pair_key)
) CLUSTER BY (date_key);

-- 2. Dimension table: Currency Pairs
CREATE OR REPLACE TABLE FOREX_DATA.GOLD.dim_currency_pairs (
    currency_pair_key VARCHAR(7) PRIMARY KEY,
    base_currency CHAR(3),
    quote_currency CHAR(3),
    pair_description VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- 3. Dimension table: Calendar
CREATE OR REPLACE TABLE FOREX_DATA.GOLD.dim_calendar (
    date_key DATE PRIMARY KEY,
    year NUMBER,
    quarter NUMBER,
    month NUMBER,
    month_name VARCHAR(10),
    week_number NUMBER,
    day_of_week NUMBER,
    day_name VARCHAR(10),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year NUMBER,
    fiscal_quarter NUMBER
);

-- Load dimension data
INSERT INTO FOREX_DATA.GOLD.dim_currency_pairs (currency_pair_key, base_currency, quote_currency, pair_description)
VALUES 
    ('EUR/USD', 'EUR', 'USD', 'Euro to US Dollar'),
    ('EUR/EGP', 'EUR', 'EGP', 'Euro to Egyptian Pound'),
    ('USD/EGP', 'USD', 'EGP', 'US Dollar to Egyptian Pound');

-- Load calendar dimension (for next 5 years)
INSERT INTO FOREX_DATA.GOLD.dim_calendar (
    date_key, year, quarter, month, month_name, 
    week_number, day_of_week, day_name, is_weekend
)
SELECT 
    d::DATE as date_key,
    YEAR(d) as year,
    QUARTER(d) as quarter,
    MONTH(d) as month,
    MONTHNAME(d) as month_name,
    WEEKOFYEAR(d) as week_number,
    DAYOFWEEK(d) as day_of_week,
    DAYNAME(d) as day_name,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
FROM (
    SELECT DATEADD(day, SEQ4(), CURRENT_DATE) as d
    FROM TABLE(GENERATOR(ROWCOUNT => 1825)) -- 5 years
);

-- Load fact data
INSERT INTO FOREX_DATA.GOLD.fact_daily_rates (
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
FROM FOREX_DATA.SILVER.forex_rates;

-- 4. Analytics Views

-- 4.1 Exchange Rate Analysis
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.exchange_rate_analysis AS
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
FROM FOREX_DATA.GOLD.fact_daily_rates f
JOIN FOREX_DATA.GOLD.dim_calendar c ON f.date_key = c.date_key
JOIN FOREX_DATA.GOLD.dim_currency_pairs p ON f.currency_pair_key = p.currency_pair_key;

-- 4.2 Time-Based KPIs
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.rate_changes AS
SELECT 
    date_key,
    currency_pair_key,
    close_rate,
    LAG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as prev_day_rate,
    LAG(close_rate, 7) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as prev_week_rate,
    LAG(close_rate, 30) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as prev_month_rate,
    daily_change_percent,
    ((close_rate - LAG(close_rate, 7) OVER (PARTITION BY currency_pair_key ORDER BY date_key)) / 
     LAG(close_rate, 7) OVER (PARTITION BY currency_pair_key ORDER BY date_key)) * 100 as weekly_change_pct,
    ((close_rate - LAG(close_rate, 30) OVER (PARTITION BY currency_pair_key ORDER BY date_key)) / 
     LAG(close_rate, 30) OVER (PARTITION BY currency_pair_key ORDER BY date_key)) * 100 as monthly_change_pct
FROM FOREX_DATA.GOLD.fact_daily_rates;

-- 4.3 Volatility Analysis
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.volatility_metrics AS
SELECT 
    DATE_TRUNC('MONTH', date_key) as month_start,
    currency_pair_key,
    AVG(volatility_percent) as avg_daily_volatility,
    MAX(volatility_percent) as max_daily_volatility,
    MIN(volatility_percent) as min_daily_volatility,
    STDDEV(daily_change_percent) as std_dev_daily_change
FROM FOREX_DATA.GOLD.fact_daily_rates
GROUP BY DATE_TRUNC('MONTH', date_key), currency_pair_key;

-- 4.4 Cross-Rate Analysis
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.cross_rates AS
SELECT 
    e_u.date_key,
    e_u.close_rate as eur_usd_rate,
    e_e.close_rate as eur_egp_rate,
    u_e.close_rate as usd_egp_rate,
    e_e.close_rate / e_u.close_rate as calculated_usd_egp_rate,
    ABS((e_e.close_rate / e_u.close_rate - u_e.close_rate) / u_e.close_rate) * 100 as cross_rate_diff_pct
FROM FOREX_DATA.GOLD.fact_daily_rates e_u
JOIN FOREX_DATA.GOLD.fact_daily_rates e_e 
    ON e_u.date_key = e_e.date_key 
    AND e_u.currency_pair_key = 'EUR/USD' 
    AND e_e.currency_pair_key = 'EUR/EGP'
JOIN FOREX_DATA.GOLD.fact_daily_rates u_e 
    ON e_u.date_key = u_e.date_key 
    AND u_e.currency_pair_key = 'USD/EGP';

-- 4.5 Moving Averages
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.moving_averages AS
SELECT 
    date_key,
    currency_pair_key,
    close_rate,
    AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 7 PRECEDING) as ma_7_day,
    AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 30 PRECEDING) as ma_30_day,
    AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 90 PRECEDING) as ma_90_day
FROM FOREX_DATA.GOLD.fact_daily_rates;

