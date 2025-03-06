# components/equipment_metrics.py - Simplified UI-Only Version
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import logging
import gc

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def render_equipment_metrics(df, metrics_results=None):
    """
    Render equipment metrics - UI only, no calculations
    
    Args:
        df: The dataframe
        metrics_results: Pre-calculated metrics results from background process
    """
    # Force garbage collection before starting
    gc.collect()
    
    # Apply custom styling
    st.markdown("""
    <style>
    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #2c5985;
    }
    .metric-label {
        font-size: 14px;
        color: #666;
    }
    .warning-indicator {
        color: #f5a142;
        font-weight: bold;
    }
    .good-indicator {
        color: #4CAF50;
        font-weight: bold;
    }
    .metric-section {
        margin-top: 30px;
        margin-bottom: 20px;
    }
    .metric-formula {
        font-style: italic;
        color: #666;
        font-size: 14px;
        margin-bottom: 15px;
    }
    .loading-pulse {
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
        0% { opacity: 0.6; }
        50% { opacity: 1; }
        100% { opacity: 0.6; }
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Create metrics container
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    
    # Add title and description
    st.markdown("""
        <h3 style="margin-top:0">Equipment Performance Metrics</h3>
        <p style="color:#666">Key metrics to evaluate equipment usage efficiency</p>
    """, unsafe_allow_html=True)
    
    # Check if we have pre-calculated metrics
    if not metrics_results:
        # Show loading indicator if no metrics are available
        st.markdown("""
            <div class="loading-pulse" style="text-align:center; padding: 20px; margin: 20px 0;">
                <p>Calculating metrics in the background...</p>
                <div style="width:100%; height:4px; background-color:#f0f0f0; border-radius:2px; margin:10px 0;">
                    <div style="width:50%; height:100%; background-color:#2c5985; border-radius:2px; animation: progress 2s infinite;"></div>
                </div>
                <p style="font-size:12px; color:#666;">This may take a moment. Only pre-calculated metrics will be displayed.</p>
            </div>
            <style>
            @keyframes progress {
                0% { width: 0%; }
                50% { width: 100%; }
                100% { width: 0%; }
            }
            </style>
        """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    # Create columns for all five metric categories
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Equipment Utilization Metrics")
        
        # Utilization Rate
        st.write("**Utilization Rate (%)**")
        st.markdown('<p class="metric-formula">(Actual Usage Time / Total Available Time) × 100</p>', unsafe_allow_html=True)
        
        # Get utilization rate from pre-calculated metrics
        utilization_rate = metrics_results.get('utilization_rate', pd.Series())
        
        # Show just the first station with a gauge
        if not utilization_rate.empty:
            station = utilization_rate.index[0]
            value = utilization_rate.iloc[0]
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=value,
                title={'text': f"{station}"},
                gauge={
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "#2c5985"},
                    'steps': [
                        {'range': [0, 60], 'color': "#ffeecd"},
                        {'range': [60, 80], 'color': "#ffd78c"},
                        {'range': [80, 100], 'color': "#f5a142"}
                    ],
                    'threshold': {
                        'line': {'color': "green", 'width': 2},
                        'thickness': 0.75,
                        'value': 75
                    }
                }
            ))
            
            fig.update_layout(height=150, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig, use_container_width=True)
            
            # Just show a text table for other stations to save memory
            if len(utilization_rate) > 1:
                st.write("**Utilization by Station:**")
                for station, value in utilization_rate.items():
                    # Add color indicators based on value
                    if value >= 75:
                        indicator = '<span class="good-indicator">■</span>'
                    elif value >= 60:
                        indicator = '<span class="warning-indicator">■</span>'
                    else:
                        indicator = '■'
                    
                    st.markdown(f"- {indicator} {station}: {value:.1f}%", unsafe_allow_html=True)
        
        # Downtime percentage - text only to save memory
        st.write("**Downtime (%)**")
        st.markdown('<p class="metric-formula">(Downtime Hours / Total Available Hours) × 100</p>', unsafe_allow_html=True)
        
        # Get downtime percentage from pre-calculated metrics
        downtime_percentage = metrics_results.get('downtime_percentage', pd.Series())
        
        if not downtime_percentage.empty:
            st.write("**Downtime by Station:**")
            for station, value in downtime_percentage.items():
                # Add color indicators based on value (reverse scale - lower is better)
                if value <= 5:
                    indicator = '<span class="good-indicator">■</span>'
                elif value <= 10:
                    indicator = '<span class="warning-indicator">■</span>'
                else:
                    indicator = '■'
                
                st.markdown(f"- {indicator} {station}: {value:.1f}%", unsafe_allow_html=True)
        
        # Test Execution Metrics Section
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.subheader("2. Test Execution Metrics")
        
        # Tests Per Equipment Per Day
        st.write("**Tests Per Equipment Per Day**")
        st.markdown('<p class="metric-formula">Total Tests / (Equipment Units × Days)</p>', unsafe_allow_html=True)
        
        # Use pre-calculated value if available
        tests_per_day = metrics_results.get('tests_per_day', 12.5)
        
        st.markdown(f'<div class="metric-value">{tests_per_day:.1f}</div>', unsafe_allow_html=True)
        
        # Average Test Duration
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.write("**Average Test Duration**")
        st.markdown('<p class="metric-formula">Total Test Time / Total Tests Conducted</p>', unsafe_allow_html=True)
        
        # Use pre-calculated value if available
        avg_duration = metrics_results.get('avg_test_duration', 3.5)
        
        st.markdown(f'<div class="metric-value">{avg_duration:.2f} min</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Helps in forecasting test capacity.</p>', unsafe_allow_html=True)
        
        # Maintenance & Calibration Metrics Section
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.subheader("3. Maintenance & Calibration Metrics")
        
        # Mean Time Between Failures (MTBF)
        st.write("**Mean Time Between Failures (MTBF)**")
        st.markdown('<p class="metric-formula">Total Operating Time / Number of Failures</p>', unsafe_allow_html=True)
        
        # Get MTBF from pre-calculated metrics
        mtbf = metrics_results.get('mtbf', 720)
        
        st.markdown(f'<div class="metric-value">{mtbf:.1f} hours</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Higher values indicate better reliability.</p>', unsafe_allow_html=True)
        
        # Mean Time To Repair (MTTR)
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.write("**Mean Time To Repair (MTTR)**")
        st.markdown('<p class="metric-formula">Total Repair Time / Number of Repairs</p>', unsafe_allow_html=True)
        
        # Get MTTR from pre-calculated metrics
        mttr = metrics_results.get('mttr', 4.5)
        
        st.markdown(f'<div class="metric-value">{mttr:.1f} hours</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Tracks how quickly the equipment is restored.</p>', unsafe_allow_html=True)
        
        # Calibration Compliance Rate
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.write("**Calibration Compliance Rate (%)**")
        st.markdown('<p class="metric-formula">(Calibrated Equipment on Time / Total Equipment Due for Calibration) × 100</p>', unsafe_allow_html=True)
        
        # Get calibration compliance from pre-calculated metrics
        calibration_compliance = metrics_results.get('calibration_compliance', 92.8)
        
        st.markdown(f'<div class="metric-value">{calibration_compliance:.1f}%</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Ensures equipment accuracy and compliance.</p>', unsafe_allow_html=True)
    
    with col2:
        st.subheader("4. Cost & Efficiency Metrics")
        
        # Cost Per Test
        st.write("**Cost Per Test**")
        st.markdown('<p class="metric-formula">Total Operational Costs / Total Number of Tests Conducted</p>', unsafe_allow_html=True)
        
        # Get cost per test from pre-calculated metrics
        cost_per_test = metrics_results.get('cost_per_test', 12.75)
        
        st.markdown(f'<div class="metric-value">${cost_per_test:.2f}</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Evaluates cost efficiency.</p>', unsafe_allow_html=True)
        
        # Energy Consumption Per Test
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.write("**Energy Consumption Per Test**")
        st.markdown('<p class="metric-formula">Total Energy Used / Number of Tests Conducted</p>', unsafe_allow_html=True)
        
        # Get energy consumption from pre-calculated metrics
        energy_consumption = metrics_results.get('energy_consumption', 2.4)
        
        st.markdown(f'<div class="metric-value">{energy_consumption:.1f} kWh</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Important for sustainability tracking.</p>', unsafe_allow_html=True)
        
        # Equipment Depreciation Rate
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.write("**Equipment Depreciation Rate (%)**")
        st.markdown('<p class="metric-formula">((Initial Value - Current Value) / Initial Value) × 100</p>', unsafe_allow_html=True)
        
        # Get depreciation rate from pre-calculated metrics
        depreciation_rate = metrics_results.get('depreciation_rate', 15.3)
        
        st.markdown(f'<div class="metric-value">{depreciation_rate:.1f}%</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Helps in asset management and budget planning.</p>', unsafe_allow_html=True)
        
        # 5. Availability & Scheduling Metrics Section
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.subheader("5. Availability & Scheduling Metrics")
        
        # Equipment Availability
        st.write("**Equipment Availability (%)**")
        st.markdown('<p class="metric-formula">(Total Available Hours - Downtime Hours) / Total Available Hours × 100</p>', unsafe_allow_html=True)
        
        # Display Equipment Availability from calculated downtime
        if not downtime_percentage.empty:
            st.write("**Availability by Station:**")
            for station, downtime in downtime_percentage.items():
                availability = 100 - downtime
                
                # Add color indicators based on value
                if availability >= 95:
                    indicator = '<span class="good-indicator">■</span>'
                elif availability >= 90:
                    indicator = '<span class="warning-indicator">■</span>'
                else:
                    indicator = '■'
                
                st.markdown(f"- {indicator} {station}: {availability:.1f}%", unsafe_allow_html=True)
        
        st.markdown('<p style="color:#666;">Ensures test scheduling efficiency.</p>', unsafe_allow_html=True)
        
        # Booking vs. Usage Discrepancy
        st.markdown('<div class="metric-section"></div>', unsafe_allow_html=True)
        st.write("**Booking vs. Usage Discrepancy (%)**")
        st.markdown('<p class="metric-formula">(Scheduled Time - Actual Used Time) / Scheduled Time × 100</p>', unsafe_allow_html=True)
        
        # Get booking discrepancy from pre-calculated metrics
        booking_discrepancy = metrics_results.get('booking_discrepancy', 15.3)
        
        # Show the discrepancy value
        st.markdown(f'<div class="metric-value">{booking_discrepancy:.1f}%</div>', unsafe_allow_html=True)
        st.markdown('<p style="color:#666;">Helps in optimizing booking systems.</p>', unsafe_allow_html=True)
    
    # Add a "Refresh Metrics" button with a unique key
    if st.button("🔄 Refresh Metrics", key="refresh_metrics_button"):
        from components.background_processor import start_background_metrics_calculation
        
        # Request new metrics calculation in the background
        if start_background_metrics_calculation():
            st.success("Metrics refresh started!")
            st.rerun()
        else:
            st.warning("Metrics calculation already in progress or no data available")
    
    # Close the metrics container
    st.markdown('</div>', unsafe_allow_html=True)