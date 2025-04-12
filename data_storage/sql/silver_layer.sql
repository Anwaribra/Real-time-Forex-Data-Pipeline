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
    PRIMARY KEY (date, currency_pair),
    CONSTRAINT valid_high_low CHECK (high_rate >= low_rate),
    CONSTRAINT valid_open CHECK (open_rate >= low_rate AND open_rate <= high_rate),
    CONSTRAINT valid_close CHECK (close_rate >= low_rate AND close_rate <= high_rate),
    CONSTRAINT valid_pair CHECK (currency_pair IN ('EUR/USD', 'EUR/EGP', 'USD/EGP'))
);

-- Load data from bronze to silver with validation
CREATE OR REPLACE PROCEDURE FOREX_DATA.SILVER.load_forex_rates()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Clear existing data
    TRUNCATE TABLE FOREX_DATA.SILVER.forex_rates;
    
    -- Load EUR/USD
    INSERT INTO FOREX_DATA.SILVER.forex_rates
    SELECT
        date,
        'EUR/USD' as currency_pair,
        open_rate,
        high_rate,
        low_rate,
        close_rate
    FROM FOREX_DATA.BRONZE.forex_eur_usd
    WHERE high_rate >= low_rate
    AND open_rate >= low_rate AND open_rate <= high_rate
    AND close_rate >= low_rate AND close_rate <= high_rate;
    
    -- Load EUR/EGP
    INSERT INTO FOREX_DATA.SILVER.forex_rates
    SELECT
        date,
        'EUR/EGP' as currency_pair,
        open_rate,
        high_rate,
        low_rate,
        close_rate
    FROM FOREX_DATA.BRONZE.forex_eur_egp
    WHERE high_rate >= low_rate
    AND open_rate >= low_rate AND open_rate <= high_rate
    AND close_rate >= low_rate AND close_rate <= high_rate;
    
    -- Load USD/EGP
    INSERT INTO FOREX_DATA.SILVER.forex_rates
    SELECT
        date,
        'USD/EGP' as currency_pair,
        open_rate,
        high_rate,
        low_rate,
        close_rate
    FROM FOREX_DATA.BRONZE.forex_usd_egp
    WHERE high_rate >= low_rate
    AND open_rate >= low_rate AND open_rate <= high_rate
    AND close_rate >= low_rate AND close_rate <= high_rate;
    
    RETURN 'Success';
END;
$$; 