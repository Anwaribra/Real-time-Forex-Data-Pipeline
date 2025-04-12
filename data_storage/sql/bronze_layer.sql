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