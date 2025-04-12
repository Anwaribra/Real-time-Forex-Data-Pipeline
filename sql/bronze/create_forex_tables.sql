-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS BRONZE;

-- Create forex rates table
CREATE OR REPLACE TABLE BRONZE.FOREX_RATES (
    currency_pair STRING,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source STRING,
    raw_data VARIANT
);

-- Create file format for CSV loading
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create stream for change tracking
CREATE OR REPLACE STREAM BRONZE.FOREX_RATES_STREAM 
    ON TABLE BRONZE.FOREX_RATES; 