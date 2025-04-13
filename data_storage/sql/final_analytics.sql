-- Technical Analysis Indicators
-- Building on top of FOREX_DATA.GOLD layer

-- 1. RSI (Relative Strength Index)
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.technical_rsi AS
WITH price_changes AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        close_rate - LAG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key) as price_change
    FROM FOREX_DATA.GOLD.fact_daily_rates
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
        ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
    END as rsi_14
FROM avg_gains_losses;

-- 2. EMA (Exponential Moving Average)
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.technical_ema AS
WITH ema_calc AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        -- EMA = Price(t) × k + EMA(y) × (1 − k)
        -- where k = 2/(N+1), N = number of periods
        -- Calculate different EMAs for different periods
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 8 PRECEDING) as sma_9,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 21 PRECEDING) as sma_22,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 49 PRECEDING) as sma_50
    FROM FOREX_DATA.GOLD.fact_daily_rates
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

-- 3. MACD (Moving Average Convergence Divergence)
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.technical_macd AS
WITH price_data AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 11 PRECEDING) as sma_12,
        AVG(close_rate) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 25 PRECEDING) as sma_26
    FROM FOREX_DATA.GOLD.fact_daily_rates
),
ema_calc AS (
    SELECT 
        date_key,
        currency_pair_key,
        close_rate,
        -- EMA 12 (faster)
        (close_rate * (2.0/13) + LAG(sma_12) OVER (PARTITION BY currency_pair_key ORDER BY date_key) * (1 - (2.0/13))) as ema_12,
        -- EMA 26 (slower)
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
    -- Signal line (9-day EMA of MACD)
    AVG(macd_line) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 8 PRECEDING) as signal_line,
    -- MACD Histogram
    macd_line - AVG(macd_line) OVER (PARTITION BY currency_pair_key ORDER BY date_key ROWS 8 PRECEDING) as macd_histogram
FROM macd_base;

-- 4. Combined Technical Analysis View
CREATE OR REPLACE VIEW FOREX_DATA.GOLD.technical_analysis AS
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
FROM FOREX_DATA.GOLD.fact_daily_rates f
LEFT JOIN FOREX_DATA.GOLD.technical_rsi r ON f.date_key = r.date_key AND f.currency_pair_key = r.currency_pair_key
LEFT JOIN FOREX_DATA.GOLD.technical_ema e ON f.date_key = e.date_key AND f.currency_pair_key = e.currency_pair_key
LEFT JOIN FOREX_DATA.GOLD.technical_macd m ON f.date_key = m.date_key AND f.currency_pair_key = m.currency_pair_key
WHERE f.date_key >= DATEADD(day, -90, CURRENT_DATE())
ORDER BY f.date_key DESC, f.currency_pair_key;

-- Example queries for analysis

-- 1. Latest Technical Signals with All Indicators
SELECT 
    currency_pair_key,
    date_key,
    close_rate,
    daily_change_percent,
    rsi_14,
    rsi_signal,
    trend_signal,
    macd_signal,
    ema_9,
    ema_22,
    ema_50,
    macd_line,
    signal_line,
    macd_histogram
FROM FOREX_DATA.GOLD.technical_analysis
WHERE date_key = (SELECT MAX(date_key) FROM FOREX_DATA.GOLD.technical_analysis)
ORDER BY currency_pair_key;

-- 2. Strong Trading Signals (Multiple Indicators Alignment)
SELECT 
    currency_pair_key,
    date_key,
    close_rate,
    rsi_14,
    rsi_signal,
    trend_signal,
    macd_signal,
    daily_change_percent
FROM FOREX_DATA.GOLD.technical_analysis
WHERE date_key >= DATEADD(day, -7, CURRENT_DATE())
    AND ((rsi_signal = 'Oversold' AND macd_signal = 'Buy')
    OR (rsi_signal = 'Overbought' AND macd_signal = 'Sell')
    OR (trend_signal LIKE 'Strong%' AND macd_signal != 'Hold'))
ORDER BY date_key DESC, currency_pair_key;

-- 3. Volatility Analysis
WITH daily_stats AS (
    SELECT 
        currency_pair_key,
        date_key,
        close_rate,
        daily_change_percent,
        ABS(daily_change_percent) as abs_change,
        AVG(ABS(daily_change_percent)) OVER (
            PARTITION BY currency_pair_key 
            ORDER BY date_key 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as avg_volatility_21d
    FROM FOREX_DATA.GOLD.technical_analysis
)
SELECT 
    currency_pair_key,
    date_key,
    close_rate,
    daily_change_percent,
    avg_volatility_21d,
    CASE 
        WHEN abs_change > 2 * avg_volatility_21d THEN 'High Volatility'
        WHEN abs_change > avg_volatility_21d THEN 'Above Average'
        ELSE 'Normal'
    END as volatility_signal
FROM daily_stats
WHERE date_key >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY date_key DESC, currency_pair_key;

-- 4. Trend Strength and Duration
WITH trend_periods AS (
    SELECT 
        currency_pair_key,
        date_key,
        close_rate,
        trend_signal,
        COUNT(*) OVER (
            PARTITION BY currency_pair_key, trend_signal
            ORDER BY date_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as trend_duration
    FROM FOREX_DATA.GOLD.technical_analysis
    WHERE trend_signal != 'Neutral'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY currency_pair_key 
        ORDER BY date_key DESC
    ) = 1
)
SELECT 
    currency_pair_key,
    date_key as latest_date,
    close_rate,
    trend_signal,
    trend_duration as days_in_current_trend
FROM trend_periods
ORDER BY trend_duration DESC;

-- 5. RSI Divergence Detection
WITH price_rsi_trends AS (
    SELECT 
        t1.currency_pair_key,
        t1.date_key,
        t1.close_rate,
        t1.rsi_14,
        CASE 
            WHEN t1.close_rate > LAG(t1.close_rate, 1) OVER (PARTITION BY t1.currency_pair_key ORDER BY t1.date_key)
            AND t1.rsi_14 < LAG(t1.rsi_14, 1) OVER (PARTITION BY t1.currency_pair_key ORDER BY t1.date_key)
            THEN 'Bearish Divergence'
            WHEN t1.close_rate < LAG(t1.close_rate, 1) OVER (PARTITION BY t1.currency_pair_key ORDER BY t1.date_key)
            AND t1.rsi_14 > LAG(t1.rsi_14, 1) OVER (PARTITION BY t1.currency_pair_key ORDER BY t1.date_key)
            THEN 'Bullish Divergence'
            ELSE 'No Divergence'
        END as divergence_signal
    FROM FOREX_DATA.GOLD.technical_analysis t1
    WHERE date_key >= DATEADD(day, -14, CURRENT_DATE())
)
SELECT 
    currency_pair_key,
    date_key,
    close_rate,
    rsi_14,
    divergence_signal
FROM price_rsi_trends
WHERE divergence_signal != 'No Divergence'
ORDER BY date_key DESC;

-- 6. Support and Resistance Levels
WITH price_levels AS (
    SELECT 
        currency_pair_key,
        date_key,
        close_rate,
        MIN(close_rate) OVER (
            PARTITION BY currency_pair_key 
            ORDER BY date_key 
            ROWS BETWEEN 20 PRECEDING AND 20 FOLLOWING
        ) as support_level,
        MAX(close_rate) OVER (
            PARTITION BY currency_pair_key 
            ORDER BY date_key 
            ROWS BETWEEN 20 PRECEDING AND 20 FOLLOWING
        ) as resistance_level
    FROM FOREX_DATA.GOLD.technical_analysis
)
SELECT 
    currency_pair_key,
    date_key,
    close_rate,
    support_level,
    resistance_level,
    ((resistance_level - support_level) / support_level) * 100 as price_channel_width_pct
FROM price_levels
WHERE date_key >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY date_key DESC, currency_pair_key;

-- 7. Multiple Timeframe Analysis
WITH daily_trends AS (
    SELECT 
        currency_pair_key,
        date_key,
        close_rate,
        trend_signal as daily_trend,
        AVG(close_rate) OVER (
            PARTITION BY currency_pair_key 
            ORDER BY date_key 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as weekly_ma,
        AVG(close_rate) OVER (
            PARTITION BY currency_pair_key 
            ORDER BY date_key 
            ROWS BETWEEN 21 PRECEDING AND CURRENT ROW
        ) as monthly_ma
    FROM FOREX_DATA.GOLD.technical_analysis
)
SELECT 
    currency_pair_key,
    date_key,
    close_rate,
    daily_trend,
    CASE 
        WHEN close_rate > weekly_ma AND weekly_ma > monthly_ma THEN 'Strong Uptrend'
        WHEN close_rate < weekly_ma AND weekly_ma < monthly_ma THEN 'Strong Downtrend'
        WHEN close_rate > monthly_ma THEN 'Bullish'
        ELSE 'Bearish'
    END as multi_timeframe_trend
FROM daily_trends
WHERE date_key >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY date_key DESC, currency_pair_key;

-- 8. Performance Summary
SELECT 
    currency_pair_key,
    COUNT(*) as total_signals,
    SUM(CASE WHEN macd_signal = 'Buy' THEN 1 ELSE 0 END) as buy_signals,
    SUM(CASE WHEN macd_signal = 'Sell' THEN 1 ELSE 0 END) as sell_signals,
    AVG(CASE WHEN macd_signal = 'Buy' THEN daily_change_percent ELSE NULL END) as avg_buy_return,
    AVG(CASE WHEN macd_signal = 'Sell' THEN daily_change_percent ELSE NULL END) as avg_sell_return,
    MIN(rsi_14) as min_rsi,
    MAX(rsi_14) as max_rsi,
    AVG(rsi_14) as avg_rsi
FROM FOREX_DATA.GOLD.technical_analysis
WHERE date_key >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY currency_pair_key
ORDER BY currency_pair_key; 