import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

def render_overview_summary():
    """Render the Overview Summary page"""
    
    # Get data from session state
    data = st.session_state.data
    
    st.markdown("## Overview Summary")
    
    # Summary Cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px;">
            <h4>Total Tests</h4>
            <h2>42,586</h2>
            <p>↑ 15% from last month</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px;">
            <h4>Average Test Duration</h4>
            <h2>3.2 minutes</h2>
            <p>↓ 5% from last month</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px;">
            <h4>Test Success Rate</h4>
            <h2>94.3%</h2>
            <p>↑ 2% from last month</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Station Activity
    st.subheader("Station Activity Overview")
    
    # Calculate total tests per station
    station_totals = data['test_data'].groupby('station')['test_count'].sum().reset_index()
    station_totals = station_totals.sort_values('test_count', ascending=False)
    
    fig1 = px.bar(
        station_totals, 
        x='station', 
        y='test_count',
        color='test_count',
        labels={'test_count': 'Total Tests', 'station': 'Station'},
        color_continuous_scale='Viridis',
        height=400
    )
    
    fig1.update_layout(
        title="Total Tests by Station",
        xaxis_title="Station",
        yaxis_title="Total Tests",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    
    st.plotly_chart(fig1, use_container_width=True)
    
    # Split the remaining visuals into two columns
    col1, col2 = st.columns(2)
    
    with col1:
        # Monthly trend chart
        monthly_data = data['test_data'].copy()
        monthly_data['month'] = monthly_data['date'].dt.strftime('%Y-%m')
        monthly_trend = monthly_data.groupby(['month', 'station'])['test_count'].sum().reset_index()
        
        # Keep only the last 12 months
        unique_months = sorted(monthly_trend['month'].unique())
        if len(unique_months) > 12:
            recent_months = unique_months[-12:]
            monthly_trend = monthly_trend[monthly_trend['month'].isin(recent_months)]
        
        fig2 = px.line(
            monthly_trend, 
            x='month', 
            y='test_count', 
            color='station',
            markers=True,
            labels={'test_count': 'Tests', 'month': 'Month', 'station': 'Station'},
            height=400
        )
        
        fig2.update_layout(
            title="Monthly Test Trend",
            xaxis_title="Month",
            yaxis_title="Tests",
            margin=dict(l=20, r=20, t=50, b=20)
        )
        
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        # Time distribution heatmap
        time_data = data['test_data'].copy()
        time_data['hour'] = time_data['date'].dt.hour
        time_data['day'] = time_data['date'].dt.day_name()
        
        # Order days of week
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Create pivot table
        time_pivot = time_data.pivot_table(
            index='day',
            columns='hour',
            values='test_count',
            aggfunc='sum'
        ).reindex(days_order)
        
        # Fill NaN values
        time_pivot = time_pivot.fillna(0)
        
        # Create heatmap
        fig3 = go.Figure(data=go.Heatmap(
            z=time_pivot.values,
            x=time_pivot.columns,
            y=time_pivot.index,
            colorscale='Viridis',
            showscale=True
        ))
        
        fig3.update_layout(
            title="Test Activity by Hour and Day",
            xaxis_title="Hour of Day",
            yaxis_title="Day of Week",
            margin=dict(l=20, r=20, t=50, b=20),
            height=400
        )
        
        st.plotly_chart(fig3, use_container_width=True)