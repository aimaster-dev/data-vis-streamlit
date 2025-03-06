# app.py - Simple direct version that works with metrics
import streamlit as st
import os
import gc
import psutil
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pymongo
from bson.objectid import ObjectId

# Memory monitoring function
def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # Convert to MB

# Set page configuration
st.set_page_config(
    page_title="Test Equipment Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# MongoDB connection details
CONNECTION_STRING = "mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/"
DATABASE_NAME = "equipment"
METRICS_COLLECTION = "equipment_metrics"

# Initialize session state
if "selected_station" not in st.session_state:
    st.session_state.selected_station = "StationL"  # Default to StationL which has data

# Force garbage collection at start
gc.collect()

# App title
st.title("Test Equipment Analysis Dashboard")

# Sidebar
with st.sidebar:
    st.title("Control Panel")
    
    # Station selection
    st.subheader("Station")
    
    # Get available stations from metrics collection
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_available_stations():
        try:
            client = pymongo.MongoClient(CONNECTION_STRING)
            db = client[DATABASE_NAME]
            metrics_col = db[METRICS_COLLECTION]
            stations = metrics_col.distinct("station")
            client.close()
            return sorted(stations)
        except Exception as e:
            st.error(f"Error getting stations: {e}")
            return []
    
    station_options = get_available_stations()
    if not station_options:
        station_options = ["StationA", "StationB", "StationC", "StationD", "ADBFI", "KAAPP2Q", "StationL", "StationS", "StationW"]
    
    selected_station = st.selectbox(
        "Select Station",
        station_options,
        index=station_options.index(st.session_state.selected_station) if st.session_state.selected_station in station_options else 0
    )
    
    # Update station selection
    if selected_station != st.session_state.selected_station:
        st.session_state.selected_station = selected_station
        st.rerun()
    
    # Add memory usage indicator
    mem_usage = get_memory_usage()
    st.info(f"**Memory Usage:** {mem_usage:.1f} MB")
    
    # Get metrics timestamp
    @st.cache_data(ttl=60)  # Cache for 1 minute
    def get_metrics_timestamp(station):
        try:
            client = pymongo.MongoClient(CONNECTION_STRING)
            db = client[DATABASE_NAME]
            metrics_col = db[METRICS_COLLECTION]
            
            # Get most recent metrics document for this station
            metrics_doc = metrics_col.find_one(
                {"station": station},
                sort=[("timestamp", -1)]
            )
            
            client.close()
            
            if metrics_doc and "timestamp" in metrics_doc:
                return metrics_doc["timestamp"]
            return None
        except Exception as e:
            return None
    
    # Display metrics last updated
    metrics_timestamp = get_metrics_timestamp(selected_station)
    if metrics_timestamp:
        st.success(f"Metrics last updated: {metrics_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.warning("Metrics not yet calculated")
    
    # Memory cleanup button
    if st.button("Clear Cache", key="clear_cache"):
        st.cache_data.clear()
        gc.collect()
        st.success("Cache cleared!")
        st.rerun()

# Main content
# Function to get metrics for the selected station
@st.cache_data(ttl=60)  # Cache for 1 minute
def get_station_metrics(station):
    """Get metrics for the selected station"""
    try:
        client = pymongo.MongoClient(CONNECTION_STRING)
        db = client[DATABASE_NAME]
        metrics_col = db[METRICS_COLLECTION]
        
        # Get most recent metrics document for this station
        metrics_doc = metrics_col.find_one(
            {"station": station},
            sort=[("timestamp", -1)]
        )
        
        client.close()
        
        if not metrics_doc:
            return {}
        
        # Convert from BSON to regular Python dict
        metrics = dict(metrics_doc)
        
        # Handle ObjectId and other non-serializable types
        for key, value in list(metrics.items()):
            if isinstance(value, ObjectId):
                metrics[key] = str(value)
            elif isinstance(value, datetime):
                metrics[key] = str(value)
        
        return metrics
    except Exception as e:
        st.error(f"Error getting metrics: {e}")
        return {}

# Get metrics for the selected station
metrics = get_station_metrics(st.session_state.selected_station)

# Display refresh button
if st.button("🔄 Refresh Data"):
    get_station_metrics.clear()
    st.rerun()

# Show metrics dashboard if metrics exist
if metrics:
    st.success(f"Showing metrics for {st.session_state.selected_station}")
    
    # Create metrics dashboard
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Equipment Utilization Metrics")
        
        # Utilization Rate card
        utilization = float(metrics.get('utilization_rate', 0))
        utilization_color = "#4CAF50" if utilization >= 70 else "#FFC107" if utilization >= 50 else "#F44336"
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Utilization Rate (%)</h4>
            <div style="font-size:28px; font-weight:bold; color:{utilization_color};">{utilization:.1f}%</div>
            <div style="font-size:12px; color:#666;">
                (Actual Usage Time / Total Available Time) × 100
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Downtime card
        downtime = float(metrics.get('downtime_percentage', 0))
        downtime_color = "#4CAF50" if downtime <= 10 else "#FFC107" if downtime <= 20 else "#F44336"
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Downtime (%)</h4>
            <div style="font-size:28px; font-weight:bold; color:{downtime_color};">{downtime:.1f}%</div>
            <div style="font-size:12px; color:#666;">
                (Downtime Hours / Total Available Hours) × 100
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Test Execution Metrics
        st.subheader("2. Test Execution Metrics")
        
        # Tests Per Day
        tests_per_day = float(metrics.get('tests_per_day', 0))
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Tests Per Equipment Per Day</h4>
            <div style="font-size:28px; font-weight:bold;">{tests_per_day:.1f}</div>
            <div style="font-size:12px; color:#666;">
                Total Tests / (Equipment Units × Days)
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Average Test Duration
        avg_duration = float(metrics.get('avg_test_duration_minutes', metrics.get('avg_test_duration', 3.5)))
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Average Test Duration</h4>
            <div style="font-size:28px; font-weight:bold;">{avg_duration:.1f} minutes</div>
            <div style="font-size:12px; color:#666;">
                Total Test Time / Total Tests Conducted
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.subheader("3. Maintenance & Calibration Metrics")
        
        # MTBF
        mtbf = float(metrics.get('mtbf_hours', metrics.get('mtbf', 0)))
        mtbf_color = "#4CAF50" if mtbf >= 500 else "#FFC107" if mtbf >= 200 else "#F44336"
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Mean Time Between Failures (MTBF)</h4>
            <div style="font-size:28px; font-weight:bold; color:{mtbf_color};">{mtbf:.1f} hours</div>
            <div style="font-size:12px; color:#666;">
                Total Operating Time / Number of Failures
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # MTTR
        mttr = float(metrics.get('mttr_hours', metrics.get('mttr', 4.5)))
        mttr_color = "#4CAF50" if mttr <= 3 else "#FFC107" if mttr <= 6 else "#F44336"
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Mean Time To Repair (MTTR)</h4>
            <div style="font-size:28px; font-weight:bold; color:{mttr_color};">{mttr:.1f} hours</div>
            <div style="font-size:12px; color:#666;">
                Total Repair Time / Number of Repairs
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Calibration Compliance
        calibration = float(metrics.get('calibration_compliance', 92.8))
        calibration_color = "#4CAF50" if calibration >= 90 else "#FFC107" if calibration >= 80 else "#F44336"
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Calibration Compliance Rate (%)</h4>
            <div style="font-size:28px; font-weight:bold; color:{calibration_color};">{calibration:.1f}%</div>
            <div style="font-size:12px; color:#666;">
                (Calibrated Equipment on Time / Total Due for Calibration) × 100
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Second row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("4. Cost & Efficiency Metrics")
        
        # Cost Per Test
        cost = float(metrics.get('estimated_cost_per_test', metrics.get('cost_per_test', 12.75)))
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Cost Per Test</h4>
            <div style="font-size:28px; font-weight:bold;">${cost:.2f}</div>
            <div style="font-size:12px; color:#666;">
                Total Operational Costs / Total Tests Conducted
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Energy Consumption
        energy = float(metrics.get('estimated_energy_per_test_kwh', metrics.get('energy_consumption', 2.4)))
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Energy Consumption Per Test</h4>
            <div style="font-size:28px; font-weight:bold;">{energy:.1f} kWh</div>
            <div style="font-size:12px; color:#666;">
                Total Energy Used / Number of Tests Conducted
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Depreciation Rate
        depreciation = float(metrics.get('equipment_depreciation_rate', metrics.get('depreciation_rate', 15.3)))
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Equipment Depreciation Rate (%)</h4>
            <div style="font-size:28px; font-weight:bold;">{depreciation:.1f}%</div>
            <div style="font-size:12px; color:#666;">
                ((Initial Value - Current Value) / Initial Value) × 100
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.subheader("5. Availability & Scheduling Metrics")
        
        # Equipment Availability
        availability = 100 - downtime
        availability_color = "#4CAF50" if availability >= 90 else "#FFC107" if availability >= 80 else "#F44336"
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Equipment Availability (%)</h4>
            <div style="font-size:28px; font-weight:bold; color:{availability_color};">{availability:.1f}%</div>
            <div style="font-size:12px; color:#666;">
                ((Total Available Hours - Downtime Hours) / Total Available Hours) × 100
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Booking vs Usage Discrepancy
        booking_discrepancy = float(metrics.get('booking_discrepancy', 15.3))
        booking_color = "#4CAF50" if booking_discrepancy <= 10 else "#FFC107" if booking_discrepancy <= 20 else "#F44336"
        
        st.markdown(f"""
        <div style="background-color:white; padding:20px; border-radius:5px; margin-bottom:10px;">
            <h4>Booking vs. Usage Discrepancy (%)</h4>
            <div style="font-size:28px; font-weight:bold; color:{booking_color};">{booking_discrepancy:.1f}%</div>
            <div style="font-size:12px; color:#666;">
                (Scheduled Time - Actual Used Time) / Scheduled Time × 100
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Additional metrics section
    st.subheader("Additional Station Metrics")
    
    # Display some of the raw metrics in a more detailed format
    col1, col2, col3 = st.columns(3)
    
    with col1:
        record_count = int(metrics.get('record_count', 0))
        st.metric("Total Records", f"{record_count:,}")
    
    with col2:
        group_count = int(metrics.get('_group_count', 0))
        st.metric("Unique Groups", group_count)
    
    with col3:
        label_count = int(metrics.get('_label_count', 0))
        st.metric("Unique Labels", f"{label_count:,}")
    
    # Show additional metrics if they exist
    more_metrics = {}
    for key in metrics:
        if key not in ['_id', 'station', 'timestamp', 'record_count', 'raw_stats'] and not key.startswith('_'):
            # Skip already displayed metrics
            if key not in [
                'utilization_rate', 'downtime_percentage', 'tests_per_day', 'avg_test_duration', 'mtbf', 'mttr',
                'calibration_compliance', 'cost_per_test', 'energy_consumption', 'equipment_depreciation_rate',
                'booking_discrepancy', 'estimated_test_duration_minutes', 'mtbf_hours', 'mttr_hours',
                'estimated_cost_per_test', 'estimated_energy_per_test_kwh'
            ]:
                more_metrics[key] = metrics[key]
    
    if more_metrics:
        with st.expander("View All Available Metrics"):
            for key, value in sorted(more_metrics.items()):
                if not isinstance(value, dict) and not isinstance(value, list):
                    st.text(f"{key}: {value}")
else:
    st.warning(f"No metrics found for {st.session_state.selected_station}. Please run the metrics calculator first.")
    
    # Suggest running metrics_calculator_script
    st.info("""
    Please run the metrics calculator script:
    ```
    python enhanced_metrics_calculator.py
    ```
    This will calculate and store metrics for your test equipment in the database.
    """)

# Final garbage collection
gc.collect()