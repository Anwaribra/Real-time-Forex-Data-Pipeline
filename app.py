import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
from pathlib import Path
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Set page config
st.set_page_config(
    page_title="Advanced Forex Data Dashboard",
    page_icon="💱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to improve UI and lighten DataFrame backgrounds
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E88E5;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 500;
    }
    .stat-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
        color: #222;
    }
    .positive-change {
        color: green;
    }
    .negative-change {
        color: red;
    }
    .stDataFrame, .stTable {background-color: #f8f9fa !important; color: #222 !important;}
</style>
""", unsafe_allow_html=True)

# Title and description
st.markdown("<div class='main-header'>Advanced Forex Data Dashboard</div>", unsafe_allow_html=True)
st.markdown("""
This dashboard provides comprehensive visualization and analysis tools for historical forex data across multiple currency pairs.
Use the sidebar to customize your view and analysis parameters.
""")

# Function to load and preprocess data
@st.cache_data
def load_data(file_path):
    """
    Load and preprocess forex data from CSV file
    
    Parameters:
    file_path (Path): Path to the CSV file
    
    Returns:
    DataFrame: Processed pandas DataFrame or None if loading fails
    """
    try:
        if not os.path.exists(file_path):
            st.error(f"File not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path)
        
        # Clean column names
        df.columns = [col.replace('1. ', '').replace('2. ', '').replace('3. ', '').replace('4. ', '') 
                      for col in df.columns]
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date')
        
        # Check for missing values
        if df.isnull().sum().sum() > 0:
            st.warning(f"Data contains {df.isnull().sum().sum()} missing values. Some calculations may be affected.")
            # Fill missing values with appropriate methods
            df['open'] = df['open'].fillna(method='ffill')
            df['high'] = df['high'].fillna(method='ffill')
            df['low'] = df['low'].fillna(method='ffill')
            df['close'] = df['close'].fillna(method='ffill')
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

# Function to create candlestick chart
def create_candlestick_chart(df, title, show_volume=False):
    """
    Create a candlestick chart for forex data
    
    Parameters:
    df (DataFrame): DataFrame containing OHLC data
    title (str): Chart title
    show_volume (bool): Whether to show volume subplot
    
    Returns:
    Figure: Plotly figure object
    """
    fig = go.Figure()
    
    # Add candlestick trace
    fig.add_trace(go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        increasing_line_color='#26A69A',
        decreasing_line_color='#EF5350',
        name='Price'
    ))
    
    # Add moving averages if available
    if 'SMA_20' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['date'], 
            y=df['SMA_20'], 
            line=dict(color='#1E88E5', width=1.5),
            name='SMA 20'
        ))
    
    if 'SMA_50' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['date'], 
            y=df['SMA_50'], 
            line=dict(color='#FFC107', width=1.5),
            name='SMA 50'
        ))
    
    if 'SMA_200' in df.columns:
        fig.add_trace(go.Scatter(
            x=df['date'], 
            y=df['SMA_200'], 
            line=dict(color='#7B1FA2', width=1.5),
            name='SMA 200'
        ))
    
    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title='Rate',
        height=600,
        xaxis_rangeslider_visible=False,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template='ggplot2'
    )
    
    # Add volume subplot if requested
    if show_volume and 'volume' in df.columns:
        fig.add_trace(go.Bar(
            x=df['date'],
            y=df['volume'],
            name='Volume',
            marker_color='rgba(128, 128, 128, 0.5)',
            yaxis="y2"
        ))
        
        fig.update_layout(
            yaxis2=dict(
                title="Volume",
                overlaying="y",
                side="right",
                showgrid=False
            )
        )
    
    return fig

# Function to add technical indicators
def add_indicators(df):
    """
    Calculate and add technical indicators to the DataFrame
    
    Parameters:
    df (DataFrame): DataFrame containing OHLC data
    
    Returns:
    DataFrame: DataFrame with added technical indicators
    """
    # Simple moving averages
    df['SMA_20'] = df['close'].rolling(window=20).mean()
    df['SMA_50'] = df['close'].rolling(window=50).mean()
    df['SMA_200'] = df['close'].rolling(window=200).mean()
    
    # Exponential moving averages
    df['EMA_12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['EMA_26'] = df['close'].ewm(span=26, adjust=False).mean()
    
    # MACD (Moving Average Convergence Divergence)
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_histogram'] = df['MACD'] - df['MACD_signal']
    
    # RSI (Relative Strength Index)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # Bollinger Bands
    df['BB_middle'] = df['SMA_20']
    df['BB_std'] = df['close'].rolling(window=20).std()
    df['BB_upper'] = df['BB_middle'] + 2 * df['BB_std']
    df['BB_lower'] = df['BB_middle'] - 2 * df['BB_std']
    
    # Daily metrics
    df['daily_change'] = df['close'] - df['open']
    df['daily_change_pct'] = (df['close'] - df['open']) / df['open'] * 100
    df['daily_range'] = df['high'] - df['low']
    df['daily_range_pct'] = df['daily_range'] / df['open'] * 100
    
    # Calculate weekly returns
    df['weekly_return'] = df['close'].pct_change(periods=5) * 100
    
    # Calculate monthly returns
    df['monthly_return'] = df['close'].pct_change(periods=20) * 100
    
    return df

# Function to create a correlation heatmap
def create_correlation_heatmap(data_dict):
    """
    Create a correlation heatmap for multiple currency pairs
    
    Parameters:
    data_dict (dict): Dictionary with currency pairs as keys and DataFrames as values
    
    Returns:
    Figure: Plotly figure object
    """
    # Create DataFrame with closing prices
    corr_data = pd.DataFrame()
    
    for pair, df in data_dict.items():
        # Ensure all DataFrames have the same date range
        corr_data[pair] = df.set_index('date')['close']
    
    # Fill missing values to align dates
    corr_data = corr_data.fillna(method='ffill')
    
    # Calculate correlation matrix
    corr_matrix = corr_data.pct_change().corr()
    
    # Create heatmap
    fig = px.imshow(
        corr_matrix,
        text_auto=True,
        color_continuous_scale='RdBu_r',
        title='Correlation Between Currency Pairs',
        labels=dict(x="Currency Pair", y="Currency Pair", color="Correlation")
    )
    
    fig.update_layout(height=500)
    
    return fig

# Function to detect significant support and resistance levels
def find_support_resistance(df, window=10, threshold=0.01):
    """
    Identify potential support and resistance levels
    
    Parameters:
    df (DataFrame): DataFrame containing OHLC data
    window (int): Window size for peak detection
    threshold (float): Minimum percentage difference for level significance
    
    Returns:
    tuple: Lists of support and resistance levels
    """
    support_levels = []
    resistance_levels = []
    
    # Find local minima (support)
    for i in range(window, len(df) - window):
        if all(df['low'].iloc[i] <= df['low'].iloc[i-window:i]) and \
           all(df['low'].iloc[i] <= df['low'].iloc[i+1:i+window+1]):
            support_levels.append((df['date'].iloc[i], df['low'].iloc[i]))
    
    # Find local maxima (resistance)
    for i in range(window, len(df) - window):
        if all(df['high'].iloc[i] >= df['high'].iloc[i-window:i]) and \
           all(df['high'].iloc[i] >= df['high'].iloc[i+1:i+window+1]):
            resistance_levels.append((df['date'].iloc[i], df['high'].iloc[i]))
    
    # Filter out levels that are too close to each other
    filtered_support = []
    for date, level in support_levels:
        if not filtered_support or abs((level - filtered_support[-1][1]) / filtered_support[-1][1]) > threshold:
            filtered_support.append((date, level))
    
    filtered_resistance = []
    for date, level in resistance_levels:
        if not filtered_resistance or abs((level - filtered_resistance[-1][1]) / filtered_resistance[-1][1]) > threshold:
            filtered_resistance.append((date, level))
    
    return filtered_support, filtered_resistance

# Function to plot MACD indicator
def plot_macd(df):
    """
    Create a MACD indicator chart
    
    Parameters:
    df (DataFrame): DataFrame containing MACD indicators
    
    Returns:
    Figure: Plotly figure object
    """
    fig = go.Figure()
    
    # Add MACD line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['MACD'],
        mode='lines',
        name='MACD',
        line=dict(color='#1E88E5')
    ))
    
    # Add signal line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['MACD_signal'],
        mode='lines',
        name='Signal',
        line=dict(color='#FF9800')
    ))
    
    # Add histogram
    colors = ['#EF5350' if val < 0 else '#26A69A' for val in df['MACD_histogram']]
    fig.add_trace(go.Bar(
        x=df['date'],
        y=df['MACD_histogram'],
        name='Histogram',
        marker_color=colors
    ))
    
    fig.update_layout(
        title='MACD Indicator',
        xaxis_title='Date',
        yaxis_title='Value',
        height=300,
        template='ggplot2',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

# Function to plot RSI indicator
def plot_rsi(df):
    """
    Create an RSI indicator chart
    
    Parameters:
    df (DataFrame): DataFrame containing RSI indicator
    
    Returns:
    Figure: Plotly figure object
    """
    fig = go.Figure()
    
    # Add RSI line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['RSI'],
        mode='lines',
        name='RSI',
        line=dict(color='#7B1FA2')
    ))
    
    # Add overbought and oversold lines
    fig.add_trace(go.Scatter(
        x=[df['date'].iloc[0], df['date'].iloc[-1]],
        y=[70, 70],
        mode='lines',
        name='Overbought (70)',
        line=dict(color='#EF5350', dash='dash')
    ))
    
    fig.add_trace(go.Scatter(
        x=[df['date'].iloc[0], df['date'].iloc[-1]],
        y=[30, 30],
        mode='lines',
        name='Oversold (30)',
        line=dict(color='#26A69A', dash='dash')
    ))
    
    fig.add_trace(go.Scatter(
        x=[df['date'].iloc[0], df['date'].iloc[-1]],
        y=[50, 50],
        mode='lines',
        name='Midline (50)',
        line=dict(color='gray', dash='dash')
    ))
    
    fig.update_layout(
        title='RSI Indicator',
        xaxis_title='Date',
        yaxis_title='RSI Value',
        height=300,
        template='ggplot2',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(range=[0, 100])
    )
    
    return fig

# Function to plot Bollinger Bands
def plot_bollinger_bands(df):
    """
    Create a Bollinger Bands chart
    
    Parameters:
    df (DataFrame): DataFrame containing Bollinger Bands indicators
    
    Returns:
    Figure: Plotly figure object
    """
    fig = go.Figure()
    
    # Add price line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['close'],
        mode='lines',
        name='Close',
        line=dict(color='#1E88E5')
    ))
    
    # Add Bollinger Bands
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['BB_upper'],
        mode='lines',
        name='Upper Band',
        line=dict(color='#EF5350', dash='dot')
    ))
    
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['BB_middle'],
        mode='lines',
        name='Middle Band (SMA 20)',
        line=dict(color='#FFC107')
    ))
    
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['BB_lower'],
        mode='lines',
        name='Lower Band',
        line=dict(color='#26A69A', dash='dot'),
        fill='tonexty',
        fillcolor='rgba(0, 176, 146, 0.1)'
    ))
    
    fig.update_layout(
        title='Bollinger Bands',
        xaxis_title='Date',
        yaxis_title='Price',
        height=400,
        template='ggplot2',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

# Function to calculate and display statistics
def calculate_statistics(df):
    """
    Calculate trading statistics from DataFrame
    
    Parameters:
    df (DataFrame): DataFrame containing OHLC data
    
    Returns:
    dict: Dictionary of statistics
    """
    stats = {}
    
    # Current values
    stats['current_price'] = df['close'].iloc[-1]
    stats['previous_price'] = df['close'].iloc[-2]
    stats['daily_change'] = stats['current_price'] - stats['previous_price']
    stats['daily_change_pct'] = (stats['daily_change'] / stats['previous_price']) * 100
    
    # High/Low values
    stats['period_high'] = df['high'].max()
    stats['period_low'] = df['low'].min()
    stats['high_date'] = df.loc[df['high'].idxmax(), 'date'].strftime('%Y-%m-%d')
    stats['low_date'] = df.loc[df['low'].idxmin(), 'date'].strftime('%Y-%m-%d')
    
    # Returns
    stats['period_return'] = ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]) * 100
    
    # Volatility
    stats['volatility'] = df['daily_change_pct'].std()
    stats['avg_daily_range'] = df['daily_range_pct'].mean()
    stats['max_daily_range'] = df['daily_range_pct'].max()
    
    # Trend indicators
    if 'SMA_50' in df.columns and 'SMA_200' in df.columns:
        current_sma_50 = df['SMA_50'].iloc[-1]
        current_sma_200 = df['SMA_200'].iloc[-1]
        stats['trend_signal'] = 'Bullish' if current_sma_50 > current_sma_200 else 'Bearish'
    else:
        stats['trend_signal'] = 'Unknown'
    
    return stats

# Function to get trading signals
def get_trading_signals(df):
    """
    Generate trading signals based on technical indicators
    
    Parameters:
    df (DataFrame): DataFrame containing technical indicators
    
    Returns:
    dict: Dictionary of trading signals and their strengths
    """
    signals = {}
    
    # Check for enough data
    if len(df) < 200:
        return {'overall': 'Insufficient data for reliable signals'}
    
    # Moving Average signals
    if 'SMA_50' in df.columns and 'SMA_200' in df.columns:
        # Golden Cross / Death Cross
        prev_sma_50 = df['SMA_50'].iloc[-2]
        prev_sma_200 = df['SMA_200'].iloc[-2]
        curr_sma_50 = df['SMA_50'].iloc[-1]
        curr_sma_200 = df['SMA_200'].iloc[-1]
        
        if prev_sma_50 < prev_sma_200 and curr_sma_50 > curr_sma_200:
            signals['moving_average'] = 'Strong Buy (Golden Cross)'
        elif prev_sma_50 > prev_sma_200 and curr_sma_50 < curr_sma_200:
            signals['moving_average'] = 'Strong Sell (Death Cross)'
        elif curr_sma_50 > curr_sma_200:
            signals['moving_average'] = 'Buy (Uptrend)'
        else:
            signals['moving_average'] = 'Sell (Downtrend)'
    
    # RSI signals
    if 'RSI' in df.columns:
        current_rsi = df['RSI'].iloc[-1]
        if current_rsi > 70:
            signals['rsi'] = 'Sell (Overbought)'
        elif current_rsi < 30:
            signals['rsi'] = 'Buy (Oversold)'
        elif current_rsi > 50:
            signals['rsi'] = 'Neutral to Bullish'
        else:
            signals['rsi'] = 'Neutral to Bearish'
    
    # MACD signals
    if 'MACD' in df.columns and 'MACD_signal' in df.columns:
        current_macd = df['MACD'].iloc[-1]
        current_signal = df['MACD_signal'].iloc[-1]
        prev_macd = df['MACD'].iloc[-2]
        prev_signal = df['MACD_signal'].iloc[-2]
        
        if prev_macd < prev_signal and current_macd > current_signal:
            signals['macd'] = 'Buy (Bullish Crossover)'
        elif prev_macd > prev_signal and current_macd < current_signal:
            signals['macd'] = 'Sell (Bearish Crossover)'
        elif current_macd > current_signal:
            signals['macd'] = 'Hold Buy (Bullish)'
        else:
            signals['macd'] = 'Hold Sell (Bearish)'
    
    # Bollinger Bands signals
    if 'BB_upper' in df.columns and 'BB_lower' in df.columns:
        current_close = df['close'].iloc[-1]
        upper_band = df['BB_upper'].iloc[-1]
        lower_band = df['BB_lower'].iloc[-1]
        
        if current_close > upper_band:
            signals['bollinger'] = 'Sell (Above Upper Band)'
        elif current_close < lower_band:
            signals['bollinger'] = 'Buy (Below Lower Band)'
        else:
            signals['bollinger'] = 'Neutral (Within Bands)'
    
    # Determine overall signal
    buy_signals = sum(1 for val in signals.values() if 'Buy' in val)
    sell_signals = sum(1 for val in signals.values() if 'Sell' in val)
    
    if buy_signals > sell_signals:
        signals['overall'] = f'Buy ({buy_signals}/{len(signals)})'
    elif sell_signals > buy_signals:
        signals['overall'] = f'Sell ({sell_signals}/{len(signals)})'
    else:
        signals['overall'] = 'Neutral'
    
    return signals

# Set up sidebar
st.sidebar.header("Dashboard Settings")

# Directory for data files
data_dir = Path("data")

# Verify data directory exists
if not os.path.exists(data_dir):
    st.error(f"Data directory not found: {data_dir}")
    st.stop()

# Get list of available forex files
forex_files = list(data_dir.glob("historical_*.csv"))

if not forex_files:
    st.error(f"No historical forex data files found in {data_dir}")
    st.stop()

# Extract currency pair options
pair_options = [file.stem.replace('historical_', '').replace('_', '/') for file in forex_files]

# Select currency pair
selected_pair = st.sidebar.selectbox(
    "Select Currency Pair",
    options=pair_options,
    index=0
)

# Convert selected pair to file path
file_name = f"historical_{selected_pair.replace('/', '_')}.csv"
file_path = data_dir / file_name

# Load selected pair data
data = load_data(file_path)

if data is not None:
    # Add technical indicators
    data = add_indicators(data)
    
    # Date range selection with default values
    min_date = data['date'].min().date()
    max_date = data['date'].max().date()
    
    # Calculate default date range (last 6 months if available)
    default_start = max(min_date, (max_date - timedelta(days=180)))
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Advanced settings in an expander
    with st.sidebar.expander("Advanced Settings"):
        show_sma = st.checkbox("Show Moving Averages", value=True)
        show_volume = st.checkbox("Show Volume (if available)", value=False)
        show_signals = st.checkbox("Show Trading Signals", value=True)
        show_support_resistance = st.checkbox("Show Support/Resistance", value=False)
        window_size = st.slider("Support/Resistance Window Size", 5, 30, 10)
    
    # Filter data based on date range
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_data = data[(data['date'].dt.date >= start_date) & (data['date'].dt.date <= end_date)]
    else:
        filtered_data = data
    
    # Check if we have enough data
    if len(filtered_data) < 5:
        st.warning("Selected date range has insufficient data. Please select a wider range.")
        filtered_data = data.tail(30)  # Default to last 30 days
    
    # Calculate statistics for the filtered data
    stats = calculate_statistics(filtered_data)
    
    # Create tabs for different analysis views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Price Charts", 
        "Technical Indicators", 
        "Performance Analysis", 
        "Trading Signals",
        "Raw Data"
    ])
    
    # Tab 1: Price Charts
    with tab1:
        st.markdown("<div class='sub-header'>Price Analysis</div>", unsafe_allow_html=True)
        
        # Display basic statistics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Current Price", 
                f"{stats['current_price']:.4f}", 
                f"{stats['daily_change_pct']:.2f}%"
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col2:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Period High", 
                f"{stats['period_high']:.4f}", 
                f"on {stats['high_date']}"
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col3:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Period Low", 
                f"{stats['period_low']:.4f}", 
                f"on {stats['low_date']}"
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col4:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Period Return", 
                f"{stats['period_return']:.2f}%",
                f"Trend: {stats['trend_signal']}"
            )
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Create candlestick chart
        candlestick_fig = create_candlestick_chart(
            filtered_data, 
            f"{selected_pair} Exchange Rate", 
            show_volume=show_volume
        )
        
        # Add support and resistance lines if requested
        if show_support_resistance:
            support_levels, resistance_levels = find_support_resistance(filtered_data, window=window_size)
            
            # Add support levels
            for date, level in support_levels:
                candlestick_fig.add_shape(
                    type="line",
                    x0=filtered_data['date'].min(),
                    y0=level,
                    x1=filtered_data['date'].max(),
                    y1=level,
                    line=dict(color="green", width=1, dash="dash"),
                    name="Support"
                )
            
            # Add resistance levels
            for date, level in resistance_levels:
                candlestick_fig.add_shape(
                    type="line",
                    x0=filtered_data['date'].min(),
                    y0=level,
                    x1=filtered_data['date'].max(),
                    y1=level,
                    line=dict(color="red", width=1, dash="dash"),
                    name="Resistance"
                )
        
        st.plotly_chart(candlestick_fig, use_container_width=True)
        
        # Price distribution
        st.subheader("Price Distribution")
        dist_fig = px.histogram(
            filtered_data, 
            x='close',
            nbins=50,
            title=f"{selected_pair} Price Distribution",
            labels={'close': 'Price'},
            color_discrete_sequence=['#1E88E5']
        )
        dist_fig.update_layout(height=300)
        st.plotly_chart(dist_fig, use_container_width=True)
    
    # Tab 2: Technical Indicators
    with tab2:
        st.markdown("<div class='sub-header'>Technical Indicators</div>", unsafe_allow_html=True)
        
        # Bollinger Bands
        st.subheader("Bollinger Bands")
        bb_fig = plot_bollinger_bands(filtered_data)
        st.plotly_chart(bb_fig, use_container_width=True)
        
        # MACD and RSI in two columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("MACD Indicator")
            macd_fig = plot_macd(filtered_data)
            st.plotly_chart(macd_fig, use_container_width=True)
        
        with col2:
            st.subheader("RSI Indicator")
            rsi_fig = plot_rsi(filtered_data)
            st.plotly_chart(rsi_fig, use_container_width=True)
    
    # Tab 3: Performance Analysis
    with tab3:
        st.markdown("<div class='sub-header'>Performance Analysis</div>", unsafe_allow_html=True)
        
        # Daily returns analysis
        st.subheader("Daily Returns Analysis")
        
        # Calculate daily returns
        daily_returns = filtered_data['daily_change_pct'].dropna()
        
        # Create columns for metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Average Daily Return", 
                f"{daily_returns.mean():.2f}%"
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col2:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Positive Days", 
                f"{(daily_returns > 0).sum()} ({(daily_returns > 0).mean()*100:.1f}%)"
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col3:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Negative Days", 
                f"{(daily_returns < 0).sum()} ({(daily_returns < 0).mean()*100:.1f}%)"
            )
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Daily returns distribution
        returns_fig = px.histogram(
            daily_returns,
            nbins=50,
            title="Daily Returns Distribution",
            labels={'value': 'Daily Return (%)'},
            color_discrete_sequence=['#1E88E5']
        )
        returns_fig.update_layout(height=300)
        st.plotly_chart(returns_fig, use_container_width=True)
        
        # Cumulative returns
        st.subheader("Cumulative Returns")
        filtered_data['cumulative_return'] = (1 + filtered_data['daily_change_pct']/100).cumprod() - 1
        cum_returns_fig = px.line(
            filtered_data,
            x='date',
            y='cumulative_return',
            title="Cumulative Returns Over Time",
            labels={'cumulative_return': 'Cumulative Return', 'date': 'Date'}
        )
        cum_returns_fig.update_traces(line_color='#26A69A')
        cum_returns_fig.update_layout(height=400)
        st.plotly_chart(cum_returns_fig, use_container_width=True)
        
        # Volatility analysis
        st.subheader("Volatility Analysis")
        
        # Create columns for volatility metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Average Daily Range", 
                f"{stats['avg_daily_range']:.2f}%"
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col2:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Max Daily Range", 
                f"{stats['max_daily_range']:.2f}%"
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col3:
            st.markdown("<div class='stat-container'>", unsafe_allow_html=True)
            st.metric(
                "Standard Deviation", 
                f"{stats['volatility']:.2f}%"
            )
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Rolling volatility
        filtered_data['rolling_volatility'] = filtered_data['daily_change_pct'].rolling(20).std()
        vol_fig = px.line(
            filtered_data,
            x='date',
            y='rolling_volatility',
            title="20-Day Rolling Volatility",
            labels={'rolling_volatility': 'Volatility (%)', 'date': 'Date'}
        )
        vol_fig.update_traces(line_color='#EF5350')
        vol_fig.update_layout(height=300)
        st.plotly_chart(vol_fig, use_container_width=True)
    
    # Tab 4: Trading Signals
    with tab4:
        st.markdown("<div class='sub-header'>Trading Signals</div>", unsafe_allow_html=True)

        if show_signals:
            if filtered_data.empty:
                st.warning("No data available for the selected currency pair and date range.")
            else:
                signals = get_trading_signals(filtered_data)
                # Handle insufficient data case
                if 'overall' in signals and 'Insufficient data' in signals['overall']:
                    st.warning("Insufficient data for reliable trading signals. Please select a wider date range or ensure your data file has at least 200 rows.")
                elif not signals:
                    st.warning("No trading signals available for the selected data.")
                else:
                    # Create a simple bar chart showing signal strength
                    signal_counts = {
                        'Buy': sum(1 for val in signals.values() if 'Buy' in val),
                        'Sell': sum(1 for val in signals.values() if 'Sell' in val),
                        'Neutral': sum(1 for val in signals.values() if 'Neutral' in val)
                    }
                    if all(v == 0 for v in signal_counts.values()):
                        st.info("No Buy, Sell, or Neutral signals detected in the current data.")
                    else:
                        signal_fig = px.bar(
                            x=list(signal_counts.keys()),
                            y=list(signal_counts.values()),
                            color=list(signal_counts.keys()),
                            color_discrete_map={
                                'Buy': '#26A69A',
                                'Sell': '#EF5350',
                                'Neutral': '#FFC107'
                            },
                            title="Signal Strength by Indicator",
                            labels={'x': 'Signal Type', 'y': 'Count'}
                        )
                        signal_fig.update_layout(showlegend=False)
                        st.plotly_chart(signal_fig, use_container_width=True)

            # Historical signal performance (simplified example)
            st.subheader("Historical Signal Performance")
            st.info("""
            Note: This is a simplified demonstration. A proper backtesting system would require 
            more sophisticated analysis of historical signal performance.
            """)
            # Create mock performance data
            performance_data = pd.DataFrame({
                'Signal Type': ['Buy', 'Sell', 'Hold'],
                'Average Return (%)': [0.15, -0.12, 0.02],
                'Win Rate (%)': [58.3, 54.1, 50.0]
            })
            # Display performance metrics
            st.dataframe(performance_data.style.format({
                'Average Return (%)': '{:.2f}%',
                'Win Rate (%)': '{:.1f}%'
            }))
    
    # Tab 5: Raw Data
    with tab5:
        st.markdown("<div class='sub-header'>Raw Data</div>", unsafe_allow_html=True)
        
        # Show raw data with option to download
        st.dataframe(filtered_data)
        
        # Download button
        csv = filtered_data.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name=f"{selected_pair.replace('/', '_')}_forex_data.csv",
            mime='text/csv'
        )
        
        # Data summary statistics
        st.subheader("Data Summary Statistics")
        st.write(filtered_data.describe())
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: gray;">
        Advanced Forex Data Dashboard • Data updates daily
    </div>
    """, unsafe_allow_html=True)

else:
    st.error("Failed to load data. Please check the data files and try again.")

# End of Streamlit app
