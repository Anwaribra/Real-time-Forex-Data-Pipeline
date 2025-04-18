import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set page config
st.set_page_config(
    page_title="Forex Data Dashboard - Snowflake",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("❄️ Forex Data Dashboard - Snowflake")
st.markdown("""
This dashboard visualizes forex data stored in Snowflake.
""")

# Sidebar
st.sidebar.header("Connection Settings")

# Snowflake connection settings
with st.sidebar.expander("Snowflake Connection", expanded=False):
    sf_account = st.text_input("Account", os.getenv("SNOWFLAKE_ACCOUNT", ""))
    sf_user = st.text_input("User", os.getenv("SNOWFLAKE_USER", ""))
    sf_password = st.text_input("Password", os.getenv("SNOWFLAKE_PASSWORD", ""), type="password")
    sf_database = st.text_input("Database", os.getenv("SNOWFLAKE_DATABASE", "FOREX_DATA"))
    sf_warehouse = st.text_input("Warehouse", os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    sf_schema = st.text_input("Schema", "GOLD")

# Connect to Snowflake function
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

# Function to load data from Snowflake
@st.cache_data(ttl=3600)
def load_data_from_snowflake(query):
    try:
        conn = connect_to_snowflake()
        if conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [col[0] for col in cursor.description]
            
            # Fetch data
            data = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            cursor.close()
            conn.close()
            
            return df
        else:
            return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Function to get available currency pairs
def get_currency_pairs():
    query = """
    SELECT DISTINCT currency_pair
    FROM GOLD.FACT_DAILY_RATES f
    JOIN GOLD.DIM_CURRENCY_PAIR c ON f.currency_pair_key = c.currency_pair_key
    ORDER BY currency_pair
    """
    
    df = load_data_from_snowflake(query)
    if df is not None and not df.empty:
        return df['CURRENCY_PAIR'].tolist()
    else:
        # Fallback to hardcoded pairs
        return ['EUR/USD', 'EUR/EGP', 'USD/EGP']

# Function to load forex data for a specific pair
def load_forex_data(currency_pair, start_date, end_date):
    query = f"""
    SELECT 
        c.date,
        p.currency_pair,
        f.open_rate,
        f.high_rate,
        f.low_rate,
        f.close_rate,
        f.daily_change,
        f.daily_change_percent,
        f.daily_range,
        f.volatility_percent
    FROM 
        GOLD.FACT_DAILY_RATES f
        JOIN GOLD.DIM_CALENDAR c ON f.date_key = c.date_key
        JOIN GOLD.DIM_CURRENCY_PAIR p ON f.currency_pair_key = p.currency_pair_key
    WHERE 
        p.currency_pair = '{currency_pair}'
        AND c.date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY 
        c.date
    """
    
    df = load_data_from_snowflake(query)
    if df is not None and not df.empty:
        # Convert column names to lowercase for consistency
        df.columns = [col.lower() for col in df.columns]
        
        # Make sure date is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    else:
        return None

# Function to load technical analysis data
def load_technical_analysis(currency_pair, start_date, end_date):
    query = f"""
    SELECT 
        c.date,
        p.currency_pair,
        t.close_rate,
        t.daily_change_percent,
        t.rsi_14,
        t.ema_9,
        t.ema_22,
        t.ema_50,
        t.macd_line,
        t.signal_line,
        t.macd_histogram,
        t.trend_signal
    FROM 
        GOLD.TECHNICAL_ANALYSIS t
        JOIN GOLD.DIM_CALENDAR c ON t.date_key = c.date_key
        JOIN GOLD.DIM_CURRENCY_PAIR p ON t.currency_pair_key = p.currency_pair_key
    WHERE 
        p.currency_pair = '{currency_pair}'
        AND c.date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY 
        c.date
    """
    
    df = load_data_from_snowflake(query)
    if df is not None and not df.empty:
        # Convert column names to lowercase for consistency
        df.columns = [col.lower() for col in df.columns]
        
        # Make sure date is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    else:
        return None

# Function to load volatility metrics
def load_volatility_metrics():
    query = """
    SELECT 
        c.year,
        c.month,
        c.month_name,
        p.currency_pair,
        v.avg_daily_volatility,
        v.max_daily_volatility,
        v.min_daily_volatility,
        v.std_dev_daily_change
    FROM 
        GOLD.VOLATILITY_METRICS v
        JOIN GOLD.DIM_CURRENCY_PAIR p ON v.currency_pair_key = p.currency_pair_key
    ORDER BY 
        c.year DESC, c.month DESC
    """
    
    df = load_data_from_snowflake(query)
    if df is not None and not df.empty:
        # Convert column names to lowercase for consistency
        df.columns = [col.lower() for col in df.columns]
        return df
    else:
        return None

# Function to create candlestick chart
def create_candlestick_chart(df, title):
    fig = go.Figure(data=[go.Candlestick(
        x=df['date'],
        open=df['open_rate'],
        high=df['high_rate'],
        low=df['low_rate'],
        close=df['close_rate'],
        increasing_line_color='green',
        decreasing_line_color='red'
    )])
    
    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title='Rate',
        height=600,
        xaxis_rangeslider_visible=False
    )
    
    return fig

# Function to create technical analysis chart
def create_technical_chart(df, indicators):
    fig = go.Figure()
    
    # Add price line
    fig.add_trace(go.Scatter(
        x=df['date'], 
        y=df['close_rate'],
        mode='lines',
        name='Close Price',
        line=dict(color='black', width=1)
    ))
    
    # Add selected indicators
    for indicator in indicators:
        if indicator == 'EMA 9' and 'ema_9' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'], 
                y=df['ema_9'],
                mode='lines',
                name='EMA 9',
                line=dict(color='blue', width=1)
            ))
        
        if indicator == 'EMA 22' and 'ema_22' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'], 
                y=df['ema_22'],
                mode='lines',
                name='EMA 22',
                line=dict(color='orange', width=1)
            ))
            
        if indicator == 'EMA 50' and 'ema_50' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'], 
                y=df['ema_50'],
                mode='lines',
                name='EMA 50',
                line=dict(color='green', width=1)
            ))
    
    fig.update_layout(
        title='Technical Analysis',
        xaxis_title='Date',
        yaxis_title='Price',
        height=500
    )
    
    return fig

# Function to create RSI chart
def create_rsi_chart(df):
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['date'], 
        y=df['rsi_14'],
        mode='lines',
        name='RSI (14)',
        line=dict(color='purple', width=1.5)
    ))
    
    # Add overbought and oversold lines
    fig.add_shape(
        type='line',
        x0=df['date'].min(),
        y0=70,
        x1=df['date'].max(),
        y1=70,
        line=dict(color='red', width=1, dash='dash')
    )
    
    fig.add_shape(
        type='line',
        x0=df['date'].min(),
        y0=30,
        x1=df['date'].max(),
        y1=30,
        line=dict(color='green', width=1, dash='dash')
    )
    
    fig.update_layout(
        title='Relative Strength Index (RSI)',
        xaxis_title='Date',
        yaxis_title='RSI Value',
        height=250,
        yaxis=dict(range=[0, 100])
    )
    
    return fig

# Function to create MACD chart
def create_macd_chart(df):
    fig = go.Figure()
    
    # MACD Line
    fig.add_trace(go.Scatter(
        x=df['date'], 
        y=df['macd_line'],
        mode='lines',
        name='MACD Line',
        line=dict(color='blue', width=1.5)
    ))
    
    # Signal Line
    fig.add_trace(go.Scatter(
        x=df['date'], 
        y=df['signal_line'],
        mode='lines',
        name='Signal Line',
        line=dict(color='red', width=1.5)
    ))
    
    # MACD Histogram
    colors = ['green' if val >= 0 else 'red' for val in df['macd_histogram']]
    fig.add_trace(go.Bar(
        x=df['date'], 
        y=df['macd_histogram'],
        name='MACD Histogram',
        marker_color=colors
    ))
    
    fig.update_layout(
        title='MACD Indicator',
        xaxis_title='Date',
        yaxis_title='MACD Value',
        height=250
    )
    
    return fig

# Dashboard Layout
try:
    # Only show data if connection is available
    if sf_account and sf_user and sf_password:
        # Try to get available currency pairs
        currency_pairs = get_currency_pairs()
        
        # Dashboard settings
        st.sidebar.header("Dashboard Settings")
        
        # Select currency pair
        selected_pair = st.sidebar.selectbox("Select Currency Pair", currency_pairs, index=0)
        
        # Date range
        default_end_date = datetime.now().date()
        default_start_date = default_end_date - timedelta(days=180)
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(default_start_date, default_end_date),
            min_value=datetime(2010, 1, 1).date(),
            max_value=default_end_date
        )
        
        # Ensure we have two dates
        if len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = default_start_date, default_end_date
        
        # Fetch data
        with st.spinner("Loading data from Snowflake..."):
            forex_data = load_forex_data(selected_pair, start_date, end_date)
            technical_data = load_technical_analysis(selected_pair, start_date, end_date)
        
        # Create tabs
        if forex_data is not None:
            tab1, tab2, tab3, tab4 = st.tabs(["Price Charts", "Technical Analysis", "Volatility", "Data Explorer"])
            
            with tab1:
                # Price Charts tab
                st.subheader(f"Price Charts - {selected_pair}")
                
                # Candlestick chart
                candlestick_fig = create_candlestick_chart(forex_data, f"{selected_pair} Exchange Rate")
                st.plotly_chart(candlestick_fig, use_container_width=True)
                
                # Summary metrics
                if not forex_data.empty:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Latest Close", f"{forex_data['close_rate'].iloc[-1]:.4f}")
                    with col2:
                        daily_change = forex_data['daily_change'].iloc[-1]
                        daily_change_pct = forex_data['daily_change_percent'].iloc[-1]
                        st.metric("Daily Change", f"{daily_change:.4f}", f"{daily_change_pct:.2f}%")
                    with col3:
                        st.metric("Period High", f"{forex_data['high_rate'].max():.4f}")
                    with col4:
                        st.metric("Period Low", f"{forex_data['low_rate'].min():.4f}")
            
            with tab2:
                # Technical Analysis tab
                st.subheader(f"Technical Analysis - {selected_pair}")
                
                if technical_data is not None:
                    # Select indicators
                    indicators = st.multiselect(
                        "Select Indicators", 
                        ["EMA 9", "EMA 22", "EMA 50"],
                        default=["EMA 9", "EMA 50"]
                    )
                    
                    # Technical chart
                    tech_fig = create_technical_chart(technical_data, indicators)
                    st.plotly_chart(tech_fig, use_container_width=True)
                    
                    # RSI and MACD charts
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'rsi_14' in technical_data.columns:
                            rsi_fig = create_rsi_chart(technical_data)
                            st.plotly_chart(rsi_fig, use_container_width=True)
                    
                    with col2:
                        if all(col in technical_data.columns for col in ['macd_line', 'signal_line', 'macd_histogram']):
                            macd_fig = create_macd_chart(technical_data)
                            st.plotly_chart(macd_fig, use_container_width=True)
                    
                    # Trade signals
                    if 'trend_signal' in technical_data.columns:
                        st.subheader("Recent Trade Signals")
                        
                        # Extract the last 10 signals
                        signals = technical_data[technical_data['trend_signal'] != 'Neutral'].sort_values('date', ascending=False).head(10)
                        
                        if not signals.empty:
                            for _, row in signals.iterrows():
                                signal_date = row['date'].strftime('%Y-%m-%d')
                                signal = row['trend_signal']
                                close = row['close_rate']
                                
                                if signal == 'Buy':
                                    st.success(f"**Buy Signal** on {signal_date} at {close:.4f}")
                                elif signal == 'Sell':
                                    st.error(f"**Sell Signal** on {signal_date} at {close:.4f}")
                        else:
                            st.info("No clear signals in the selected date range")
                else:
                    st.warning("Technical analysis data not available")
            
            with tab3:
                # Volatility tab
                st.subheader(f"Volatility Analysis - {selected_pair}")
                
                # Daily volatility chart
                vol_fig = px.line(
                    forex_data, 
                    x='date', 
                    y='volatility_percent',
                    title=f"{selected_pair} Daily Volatility %",
                    labels={'volatility_percent': 'Daily Range %', 'date': 'Date'}
                )
                
                vol_fig.update_layout(height=400)
                st.plotly_chart(vol_fig, use_container_width=True)
                
                # Volatility metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Avg. Volatility", f"{forex_data['volatility_percent'].mean():.2f}%")
                with col2:
                    st.metric("Max Volatility", f"{forex_data['volatility_percent'].max():.2f}%")
                with col3:
                    st.metric("Volatility (Std Dev)", f"{forex_data['daily_change_percent'].std():.2f}%")
                
                # Try to get monthly volatility from the VOLATILITY_METRICS view
                try:
                    volatility_metrics = load_volatility_metrics()
                    if volatility_metrics is not None:
                        pair_volatility = volatility_metrics[volatility_metrics['currency_pair'] == selected_pair]
                        
                        if not pair_volatility.empty:
                            st.subheader("Monthly Volatility")
                            
                            # Create period column for sorting
                            pair_volatility['period'] = pair_volatility['year'].astype(str) + '-' + pair_volatility['month'].astype(str).str.zfill(2)
                            pair_volatility = pair_volatility.sort_values('period')
                            
                            monthly_vol_fig = px.bar(
                                pair_volatility,
                                x='month_name',
                                y='avg_daily_volatility',
                                color='year',
                                title="Average Monthly Volatility by Year",
                                labels={'avg_daily_volatility': 'Avg Daily Volatility %', 'month_name': 'Month'}
                            )
                            
                            monthly_vol_fig.update_layout(height=400)
                            st.plotly_chart(monthly_vol_fig, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not load monthly volatility metrics: {str(e)}")
            
            with tab4:
                # Data Explorer tab
                st.subheader(f"Data Explorer - {selected_pair}")
                
                # Add data download option
                @st.cache_data
                def convert_df_to_csv(df):
                    return df.to_csv(index=False).encode('utf-8')
                
                csv = convert_df_to_csv(forex_data)
                st.download_button(
                    label="Download Data as CSV",
                    data=csv,
                    file_name=f"{selected_pair.replace('/', '_')}_data.csv",
                    mime='text/csv',
                )
                
                # Display data
                st.dataframe(forex_data.sort_values('date', ascending=False), use_container_width=True)
        else:
            st.warning("Could not fetch data from Snowflake. Please check your connection settings.")
    else:
        st.info("Please enter your Snowflake connection details in the sidebar to continue.")
        
        # Sample screenshot
        st.image("https://miro.medium.com/max/1400/1*7AOhGDnRL2eyJMUidQHmyw.gif", 
                 caption="Sample Forex Dashboard Visualization")
        
        st.markdown("""
        ### Features of this dashboard:
        - Connect directly to your Snowflake data warehouse
        - View price charts with candlestick visualization
        - Analyze technical indicators (EMA, RSI, MACD)
        - Monitor volatility metrics
        - Explore raw data and download as CSV
        """)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    
# Footer
st.markdown("---")
st.markdown("Created with Streamlit · Data from Snowflake") 