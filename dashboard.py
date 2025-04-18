import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
from pathlib import Path

# Set page config
st.set_page_config(
    page_title="Forex Data Dashboard",
    page_icon="💱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title(" Forex Data Dashboard")
st.markdown("""
This dashboard visualizes historical forex data for different currency pairs.
""")


@st.cache_data
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        # Rename columns if they have prefixes
        df.columns = [col.replace('1. ', '').replace('2. ', '').replace('3. ', '').replace('4. ', '') 
                      for col in df.columns]
        
        
        df['date'] = pd.to_datetime(df['date'])
        
        
        df = df.sort_values('date')
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None


def create_candlestick_chart(df, title):
    fig = go.Figure(data=[go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
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


def add_indicators(df):
    # Calculate simple moving averages
    df['SMA_20'] = df['close'].rolling(window=20).mean()
    df['SMA_50'] = df['close'].rolling(window=50).mean()
    df['SMA_200'] = df['close'].rolling(window=200).mean()
    
   
    df['daily_change'] = df['close'] - df['open']
    df['daily_change_pct'] = (df['close'] - df['open']) / df['open'] * 100
    
    
    df['daily_range'] = df['high'] - df['low']
    df['daily_range_pct'] = df['daily_range'] / df['open'] * 100
    
    return df


st.sidebar.header("Settings")


data_dir = Path("data")
forex_files = list(data_dir.glob("historical_*.csv"))
pair_options = [file.stem.replace('historical_', '').replace('_', '/') for file in forex_files]


selected_pair = st.sidebar.selectbox("Select Currency Pair", pair_options, index=0)
file_name = f"historical_{selected_pair.replace('/', '_')}.csv"
file_path = data_dir / file_name


data = load_data(file_path)

if data is not None:
   
    data = add_indicators(data)
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(data['date'].min().date(), data['date'].max().date()),
        min_value=data['date'].min().date(),
        max_value=data['date'].max().date()
    )
    

    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_data = data[(data['date'].dt.date >= start_date) & (data['date'].dt.date <= end_date)]
    else:
        filtered_data = data
    
    
    tab1, tab2, tab3, tab4 = st.tabs(["Candlestick Chart", "Trend Analysis", "Volatility Analysis", "Raw Data"])
    
    with tab1:
        # Candlestick Chart
        st.subheader(f"Candlestick Chart - {selected_pair}")
        candlestick_fig = create_candlestick_chart(filtered_data, f"{selected_pair} Exchange Rate")
        st.plotly_chart(candlestick_fig, use_container_width=True)
        
        # Display basic statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Latest Close", f"{filtered_data['close'].iloc[-1]:.4f}")
        with col2:
            daily_change = filtered_data['close'].iloc[-1] - filtered_data['close'].iloc[-2]
            daily_change_pct = (daily_change / filtered_data['close'].iloc[-2]) * 100
            st.metric("Daily Change", f"{daily_change:.4f}", f"{daily_change_pct:.2f}%")
        with col3:
            st.metric("Period High", f"{filtered_data['high'].max():.4f}")
        with col4:
            st.metric("Period Low", f"{filtered_data['low'].min():.4f}")
    
    with tab2:
        # Trend Analysis
        st.subheader(f"Trend Analysis - {selected_pair}")
        
        # Plot price with moving averages
        trend_fig = px.line(filtered_data, x='date', y=['close', 'SMA_20', 'SMA_50', 'SMA_200'],
                           title=f"{selected_pair} Price and Moving Averages",
                           labels={'value': 'Rate', 'variable': 'Indicator'})
        
        trend_fig.update_layout(height=600)
        st.plotly_chart(trend_fig, use_container_width=True)
        
        # Period performance
        period_return = ((filtered_data['close'].iloc[-1] - filtered_data['close'].iloc[0]) / 
                         filtered_data['close'].iloc[0]) * 100
        
        st.info(f"Period Performance: {period_return:.2f}%")
    
    with tab3:
        # Volatility Analysis
        st.subheader(f"Volatility Analysis - {selected_pair}")
        
        # Daily change chart
        volatility_fig = px.bar(filtered_data, x='date', y='daily_change_pct',
                               title=f"{selected_pair} Daily Price Change %",
                               labels={'daily_change_pct': 'Daily Change %'})
        
        volatility_fig.update_layout(height=400)
        st.plotly_chart(volatility_fig, use_container_width=True)
        
        # Volatility metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avg. Daily Range %", f"{filtered_data['daily_range_pct'].mean():.2f}%")
        with col2:
            st.metric("Max Daily Range %", f"{filtered_data['daily_range_pct'].max():.2f}%")
        with col3:
            st.metric("Volatility (Std Dev)", f"{filtered_data['daily_change_pct'].std():.2f}%")
        
        # Monthly volatility chart
        if len(filtered_data) > 30:
            filtered_data['month'] = filtered_data['date'].dt.strftime('%Y-%m')
            monthly_volatility = filtered_data.groupby('month')['daily_range_pct'].mean().reset_index()
            
            monthly_vol_fig = px.bar(monthly_volatility, x='month', y='daily_range_pct',
                                     title="Average Monthly Volatility",
                                     labels={'daily_range_pct': 'Avg Daily Range %', 'month': 'Month'})
            
            st.plotly_chart(monthly_vol_fig, use_container_width=True)
    
    with tab4:
        
        st.subheader(f"Raw Data - {selected_pair}")
        
        
        @st.cache_data
        def convert_df_to_csv(df):
            return df.to_csv(index=False).encode('utf-8')
        
        csv = convert_df_to_csv(filtered_data)
        st.download_button(
            label="Download Data as CSV",
            data=csv,
            file_name=f"{selected_pair.replace('/', '_')}_data.csv",
            mime='text/csv',
        )
        
        st.dataframe(filtered_data.sort_values('date', ascending=False), use_container_width=True)
else:
    st.error("Failed to load data. Please check the file path and format.")

st.header("Currency Pair Comparison")

if data is not None:
    selected_pairs_for_comparison = st.multiselect(
        "Select Currency Pairs to Compare",
        pair_options,
        default=[selected_pair]
    )
    
    if selected_pairs_for_comparison:
        comparison_data = pd.DataFrame()
        
        for pair in selected_pairs_for_comparison:
            pair_file = f"historical_{pair.replace('/', '_')}.csv"
            pair_path = data_dir / pair_file
            
            pair_data = load_data(pair_path)
            if pair_data is not None:
                pair_data['currency_pair'] = pair

                first_close = pair_data['close'].iloc[0]
                pair_data['normalized_close'] = pair_data['close'] / first_close * 100
                
                comparison_data = pd.concat([comparison_data, pair_data[['date', 'currency_pair', 'close', 'normalized_close']]])
        
        if not comparison_data.empty:
            comp_fig = px.line(comparison_data, x='date', y='normalized_close', color='currency_pair',
                              title="Normalized Performance Comparison (Base 100)",
                              labels={'normalized_close': 'Normalized Value (Base 100)', 'date': 'Date'})
            
            comp_fig.update_layout(height=500)
            st.plotly_chart(comp_fig, use_container_width=True)

            actual_fig = px.line(comparison_data, x='date', y='close', color='currency_pair',
                                 title="Actual Exchange Rates",
                                 labels={'close': 'Exchange Rate', 'date': 'Date'})
            
            actual_fig.update_layout(height=500)
            st.plotly_chart(actual_fig, use_container_width=True)

st.markdown("---")
st.markdown("Created with Streamlit · Data from Alpha Vantage API") 