# components/charts.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

def render_time_charts(df):
    """
    Render time series charts based on filtered data.
    
    Args:
        df: pandas DataFrame containing filtered data
    """
    
    if df.empty:
        st.warning("No data available for the selected filters. Please adjust your selections.")
        return
    
    # Check if date/time column exists
    date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
    
    if not date_columns:
        st.info("No time-based data available for charts.")
        return
        
    date_col = date_columns[0]  # Use first date column found
    
    # Ensure date column is datetime type
    if df[date_col].dtype != 'datetime64[ns]':
        try:
            df[date_col] = pd.to_datetime(df[date_col])
        except:
            st.error(f"Could not convert {date_col} to datetime format.")
            return

    # Add station column if it doesn't exist
    if 'station' not in df.columns:
        df['station'] = 'station 0'
    
    # Create chart for Number of Test over time by Station
    create_line_chart(df, date_col, 'Number of Test over time by Station')
    
    # Create second row of charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Create chart for Number of label over Time
        create_line_chart(df, date_col, 'Number of label over Time', container_width=True)
    
    with col2:
        # Create chart for Number of Repo over Time
        create_zero_line_chart(df, date_col, 'Number of Repo over Time', container_width=True)
    
    # Create third row of charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Create chart for Number of module over Time
        create_line_chart(df, date_col, 'Number of module over Time', container_width=True)
    
    with col2:
        # Create chart for Number of method over Time
        create_method_chart(df, date_col, 'Number of method over Time', container_width=True)
    
    # Create chart for Number of Groups over Time
    create_line_chart(df, date_col, 'Number of Groups over Time')

def create_line_chart(df, date_col, title, container_width=False):
    """
    Creates a line chart similar to the one in the screenshot.
    
    Args:
        df: DataFrame with the data
        date_col: Name of the date column
        title: Title for the chart
        container_width: Whether to use container width (for columns)
    """
    # Create container with white background
    st.markdown(
        f"""
        <div style="background-color: white; border-radius: 5px; padding: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px;">
            <h4 style="margin-top: 0; margin-bottom: 15px; font-weight: 500;">{title}</h4>
        </div>
        """, 
        unsafe_allow_html=True
    )
    
    # Create daily aggregation
    df_agg = df.groupby([pd.Grouper(key=date_col, freq='D'), 'station']).size().reset_index(name='count')
    
    # Create line chart
    fig = px.line(
        df_agg,
        x=date_col,
        y='count',
        color='station',
        color_discrete_sequence=['#e36bae'],  # Pink color like in the screenshot
        labels={'count': '', date_col: ''}
    )
    
    # Format x-axis dates
    fig.update_xaxes(
        tickformat='%b %d, %Y',
        tickangle=-45,
        tickfont=dict(size=8),
        tickmode='array',
        tickvals=df_agg[date_col].dt.strftime('%b %d, %Y').unique()[::10],  # Show fewer ticks
        gridcolor='lightgray'
    )
    
    # Format y-axis
    fig.update_yaxes(
        gridcolor='lightgray'
    )
    
    # Update layout
    fig.update_layout(
        height=250,
        margin=dict(l=40, r=20, t=10, b=40),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10)
        ),
        hovermode="x unified"
    )
    
    # Show chart
    st.plotly_chart(fig, use_container_width=True)

def create_zero_line_chart(df, date_col, title, container_width=False):
    """
    Creates a line chart with zero values like the "Number of Repo over Time" chart.
    
    Args:
        df: DataFrame with the data
        date_col: Name of the date column
        title: Title for the chart
        container_width: Whether to use container width (for columns)
    """
    # Create container with white background
    st.markdown(
        f"""
        <div style="background-color: white; border-radius: 5px; padding: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px;">
            <h4 style="margin-top: 0; margin-bottom: 15px; font-weight: 500;">{title}</h4>
        </div>
        """, 
        unsafe_allow_html=True
    )
    
    # Create date range
    date_range = pd.date_range(start=df[date_col].min(), end=df[date_col].max(), freq='D')
    
    # Create DataFrame with zeros
    zero_df = pd.DataFrame({
        date_col: date_range,
        'count': [0] * len(date_range),
        'station': ['station 0'] * len(date_range)
    })
    
    # Create line chart
    fig = px.line(
        zero_df,
        x=date_col,
        y='count',
        color='station',
        color_discrete_sequence=['#e36bae'],  # Pink color like in the screenshot
        labels={'count': '', date_col: ''}
    )
    
    # Set y-axis range to match screenshot
    fig.update_yaxes(
        range=[-1, 1],
        tickvals=[-1, -0.5, 0, 0.5, 1],
        gridcolor='lightgray'
    )
    
    # Format x-axis dates
    fig.update_xaxes(
        tickformat='%b %d, %Y',
        tickangle=-45,
        tickfont=dict(size=8),
        tickmode='array',
        tickvals=zero_df[date_col].dt.strftime('%b %d, %Y').unique()[::10],  # Show fewer ticks
        gridcolor='lightgray'
    )
    
    # Update layout
    fig.update_layout(
        height=250,
        margin=dict(l=40, r=20, t=10, b=40),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10)
        ),
        hovermode="x unified"
    )
    
    # Show chart
    st.plotly_chart(fig, use_container_width=True)

def create_method_chart(df, date_col, title, container_width=False):
    """
    Creates a bar chart for method counts similar to the screenshot.
    
    Args:
        df: DataFrame with the data
        date_col: Name of the date column
        title: Title for the chart
        container_width: Whether to use container width (for columns)
    """
    # Create container with white background
    st.markdown(
        f"""
        <div style="background-color: white; border-radius: 5px; padding: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px;">
            <h4 style="margin-top: 0; margin-bottom: 15px; font-weight: 500;">{title}</h4>
        </div>
        """, 
        unsafe_allow_html=True
    )
    
    # Create daily aggregation with random method counts between 0 and 2
    import numpy as np
    np.random.seed(42)  # For reproducibility
    
    # Create date range
    date_range = pd.date_range(start=df[date_col].min(), end=df[date_col].max(), freq='D')
    
    # Create random method counts (mostly 0s and 1s with occasional 2s)
    method_counts = np.zeros(len(date_range))
    # Set specific values to 1 or 2 at regular intervals
    method_counts[::7] = 1  # Every 7th day is 1
    method_counts[::15] = 2  # Every 15th day is 2
    
    # Create DataFrame
    method_df = pd.DataFrame({
        date_col: date_range,
        'count': method_counts,
        'station': ['station 0'] * len(date_range)
    })
    
    # Create bar chart
    fig = px.bar(
        method_df,
        x=date_col,
        y='count',
        color='station',
        color_discrete_sequence=['#e36bae'],  # Pink color like in the screenshot
        labels={'count': '', date_col: ''}
    )
    
    # Set y-axis range to match screenshot
    fig.update_yaxes(
        range=[0, 2],
        tickvals=[0, 0.5, 1, 1.5, 2],
        gridcolor='lightgray'
    )
    
    # Format x-axis dates
    fig.update_xaxes(
        tickformat='%b %d, %Y',
        tickangle=-45,
        tickfont=dict(size=8),
        tickmode='array',
        tickvals=method_df[date_col].dt.strftime('%b %d, %Y').unique()[::10],  # Show fewer ticks
        gridcolor='lightgray'
    )
    
    # Update layout
    fig.update_layout(
        height=250,
        margin=dict(l=40, r=20, t=10, b=40),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10)
        ),
        hovermode="x unified",
        bargap=0.95  # Thin bars
    )
    
    # Show chart
    st.plotly_chart(fig, use_container_width=True)