-- Create table for historical rates
CREATE TABLE IF NOT EXISTS historical_rates_template (
    timestamp TIMESTAMP,
    currency_pair VARCHAR(10),
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT
);

-- Create table for realtime rates
CREATE TABLE IF NOT EXISTS realtime_rates (
    timestamp TIMESTAMP,
    currency_pair VARCHAR(10),
    rate FLOAT,
    bid FLOAT,
    ask FLOAT
); 