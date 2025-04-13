-- Bronze Layer: Raw data storage
-- Stores data as-is from source with minimal transformations

-- Create schema
CREATE SCHEMA IF NOT EXISTS FOREX_DATA;
CREATE SCHEMA IF NOT EXISTS FOREX_DATA.BRONZE;

-- EUR/USD raw data
CREATE OR REPLACE TABLE FOREX_DATA.BRONZE.forex_eur_usd (
    date DATE,
    open_rate FLOAT,
    high_rate FLOAT,
    low_rate FLOAT,
    close_rate FLOAT
);

-- EUR/EGP raw data
CREATE OR REPLACE TABLE FOREX_DATA.BRONZE.forex_eur_egp (
    date DATE,
    open_rate FLOAT,
    high_rate FLOAT,
    low_rate FLOAT,
    close_rate FLOAT
);

-- USD/EGP raw data
CREATE OR REPLACE TABLE FOREX_DATA.BRONZE.forex_usd_egp (
    date DATE,
    open_rate FLOAT,
    high_rate FLOAT,
    low_rate FLOAT,
    close_rate FLOAT
);

-- Copy data from PUBLIC schema
INSERT INTO FOREX_DATA.BRONZE.forex_eur_usd 
SELECT date, open_rate, high_rate, low_rate, close_rate
FROM PUBLIC.FOREX_RATES_EUR_USD;

INSERT INTO FOREX_DATA.BRONZE.forex_eur_egp 
SELECT date, open_rate, high_rate, low_rate, close_rate
FROM PUBLIC.FOREX_RATES_EUR_EGP;

INSERT INTO FOREX_DATA.BRONZE.forex_usd_egp 
SELECT date, open_rate, high_rate, low_rate, close_rate
FROM PUBLIC.FOREX_RATES_USD_EGP; 