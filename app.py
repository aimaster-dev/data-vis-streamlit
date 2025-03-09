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
from datetime import datetime
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

st.markdown("""
<style>
    .main-header {
        font-size: 26px;
        font-weight: bold;
        color: #1E3A8A;
        margin-bottom: 20px;
    }
    .section-header {
        font-size: 20px;
        font-weight: bold;
        color: #1E3A8A;
        margin-top: 10px;
        margin-bottom: 10px;
    }
    .metric-card {
        background-color: white;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    .metric-title {
        font-size: 16px;
        font-weight: 500;
        color: #666;
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #1E3A8A;
    }
    .metric-formula {
        font-size: 12px;
        color: #888;
        font-style: italic;
        margin-bottom: 10px;
    }
    .trend-up {
        color: #4CAF50;
        font-weight: bold;
    }
    .trend-down {
        color: #F44336;
        font-weight: bold;
    }
    .trend-neutral {
        color: #9E9E9E;
        font-weight: bold;
    }
    .missing-data {
        color: #9E9E9E;
        font-style: italic;
    }
    .chart-container {
        background-color: white;
        border-radius: 8px;
        padding: 15px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# MongoDB connection details
CONNECTION_STRING = "mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/"
DATABASE_NAME = "equipment"
METRICS_COLLECTION = "equipment_metrics"
cutoff_date = datetime(2024, 1, 1)

# Initialize session state
if "selected_station" not in st.session_state:
    st.session_state.selected_station = "ADBFI"

if "selected_group" not in st.session_state:
    st.session_state.selected_group = "All Groups"

if "selected_label" not in st.session_state:
    st.session_state.selected_label = "All Labels"

if "selected_repo" not in st.session_state:
    st.session_state.selected_repo = "All Repos"

if "selected_module" not in st.session_state:
    st.session_state.selected_module = "All Modules"

def create_gauge_chart(value, title, min_val=0, max_val=100, good_threshold=75, warning_threshold=50, is_missing=False):
    """
    Create a gauge chart for metrics visualization
    
    Args:
        value: Value to display on gauge
        title: Title for the gauge
        min_val: Minimum value on gauge scale
        max_val: Maximum value on gauge scale
        good_threshold: Threshold for good performance (green)
        warning_threshold: Threshold for warning performance (yellow)
        is_missing: Whether the data is missing
        
    Returns:
        plotly.graph_objects.Figure: Gauge chart figure
    """
    # Handle None values
    value = 0 if value is None else value
    
    if is_missing:
        # Gray color for missing data
        color = "#CCCCCC"
        title = f"{title} (No Data)"
    else:
        # Determine color based on thresholds
        if value >= good_threshold:
            color = "#4CAF50"  # Good - Green
        elif value >= warning_threshold:
            color = "#FFC107"  # Warning - Yellow/Amber
        else:
            color = "#F44336"  # Danger - Red
    
    # Create the gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title, 'font': {'size': 14, 'color': '#666'}},
        number={'suffix': "%", 'font': {'size': 20, 'color': '#1E3A8A'}},
        gauge={
            'axis': {'range': [min_val, max_val], 'tickwidth': 1, 'tickcolor': "#666"},
            'bar': {'color': color},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "#DDDDDD",
            'steps': [
                {'range': [min_val, warning_threshold], 'color': '#FFECB3'},
                {'range': [warning_threshold, good_threshold], 'color': '#E6EE9C'},
                {'range': [good_threshold, max_val], 'color': '#C8E6C9'}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 2},
                'thickness': 0.75,
                'value': value
            }
        }
    ))
    
    # Update layout
    fig.update_layout(
        height=250,  # Increased height
        margin=dict(l=30, r=30, t=60, b=40),  # Increased top and bottom margins
        paper_bgcolor="white",
        font={'color': "#333", 'family': "Arial, sans-serif"},  # Better font
        autosize=True,  # Allow auto-sizing
    )
    
    return fig

# Helper function to safely get values
def safe_get_metric(metrics, key, default=None):
    """Get a metric value safely, handling None values and missing data"""
    if not metrics:
        return {"value": default, "is_missing": True}
    
    value = metrics.get(key)
    if value is None:
        # Check if we have missing data flag
        if metrics.get("missing_data", False):
            return {"value": default, "is_missing": True}
        return {"value": default, "is_missing": False}
    
    return {"value": value, "is_missing": False}

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
            metrics_meta = db["metrics_metadata"]
            cand_stations = metrics_col.distinct("station")
            stations = []
            for station in cand_stations:
                last_processed = metrics_meta.find_one({"station": station})
                if last_processed["last_processed"] < cutoff_date:
                    continue
                else:
                    stations.append(station)
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
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_available_groups(station):
        try:
            client = pymongo.MongoClient(CONNECTION_STRING)
            db = client[DATABASE_NAME]
            metrics_col = db[METRICS_COLLECTION]
            groups = ["All Groups"]
            groups.extend(metrics_col.distinct("group", {"station": station}))
            client.close()
            return groups
        except Exception as e:
            st.error(f"Error getting groups: {e}")
            return []

    st.subheader("Group")
    group_options = get_available_groups(selected_station)
    selected_group = st.selectbox(
        "Select Group", 
        group_options, 
        index=station_options.index(st.session_state.selected_group) if st.session_state.selected_group in station_options else 0
    )

    if selected_station != st.session_state.selected_station:
        st.session_state.selected_station = selected_station
        st.rerun()

    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_available_labels(station, group):
        try:
            client = pymongo.MongoClient(CONNECTION_STRING)
            db = client[DATABASE_NAME]
            metrics_col = db[station]
            labels = ["All Labels"]
            labels.extend(metrics_col.distinct("_label", {"_group": group}))
            client.close()
            return labels
        except Exception as e:
            st.error(f"Error getting groups: {e}")
            return []

    st.subheader("Label")
    label_options = get_available_labels(selected_station, selected_group)
    if len(label_options) > 100:
        st.info(f"Showing first 100 labels only")
    selected_label = st.selectbox(
        "Select Label", 
        label_options,
        index=station_options.index(st.session_state.selected_label) if st.session_state.selected_label in station_options else 0
    )
    if selected_label != st.session_state.selected_label:
        st.session_state.selected_label = selected_label
        st.rerun()

    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_available_repositories(station, group, label):
        try:
            client = pymongo.MongoClient(CONNECTION_STRING)
            db = client[DATABASE_NAME]
            metrics_col = db[station]
            repos = ["All Repos"]
            repos.extend(metrics_col.distinct("repo", {"_group": group, "_label": label}))
            client.close()
            return repos
        except Exception as e:
            st.error(f"Error getting groups: {e}")
            return []

    st.subheader("Repository")
    repo_options = get_available_repositories(selected_station, selected_group, selected_label)
    selected_repo = st.selectbox(
        "Select Repository", 
        repo_options,
        index=station_options.index(st.session_state.selected_repo) if st.session_state.selected_repo in station_options else 0
    )
    if selected_repo != st.session_state.selected_repo:
        st.session_state.selected_repo = selected_repo
        st.rerun()

    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_available_modules(station, group, label, repo):
        try:
            client = pymongo.MongoClient(CONNECTION_STRING)
            db = client[DATABASE_NAME]
            metrics_col = db[station]
            modules = ["All Modules"]
            modules.extend(metrics_col.distinct("module", {"_group": group, "_label": label, "repo": repo}))
            client.close()
            return modules
        except Exception as e:
            st.error(f"Error getting groups: {e}")
            return []

    st.subheader("Module")
    module_options = get_available_modules(selected_station, selected_group, selected_label, selected_repo)
    selected_module = st.selectbox(
        "Select Module", 
        module_options,
        index=station_options.index(st.session_state.selected_module) if st.session_state.selected_module in station_options else 0
    )
    if selected_module != st.session_state.selected_module:
        st.session_state.selected_module = selected_module
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

Dashboard, Metrics_Calculation_API = st.tabs(["Dashboard", "Metrics Calculation API"])
    
with Dashboard:
    st.success(f"Metrics last updated: {metrics_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    Hour, Day, Month = st.tabs(["Hourly", "Daily", "Monthly"])
    @st.cache_data(ttl=60)
    def sum_by_hour(collection_name):
        try:
            with open(f'forgraph/{collection_name}_graph.json') as file:
                data = json.load(file)
            return data
        except Exception as e:
            logger.error(f"Error in background data loading: {e}")
            return False
    
    @st.cache_data(ttl=60)
    def sum_by_day(collection_name):
        try:
            with open(f'forgraph/{collection_name}_graph.json') as file:
                hourly_data = json.load(file)
            # print("0asdfasdfasdfasdfasdfasdfasfefefefe")
            module = {**hourly_data["module_counts_per_hour"]}
            del module["module_index"]
            label = {**hourly_data["label_counts_per_hour"]}
            del label["_label_index"]
            method = {**hourly_data["method_counts_per_hour"]}
            del method["method_index"]
            repo = {**hourly_data["repo_counts_per_hour"]}
            del repo["repo_index"]
            log = {"log_counts_per_hour": hourly_data["log_counts_per_hour"]}
            group = {**hourly_data["group_counts_per_hour"]}
            del group["_group_index"]
            # print("1asdfasdfasdfasdfasdfasdfasfefefefe")
            daily_data = pd.DataFrame({
                "date": [d[:10] for d in hourly_data["dates"]],  # Extract YYYY-MM
                **module,
                **label,
                **method,
                **repo,
                **log,
                **group,
            })
            daily_aggregated = daily_data.groupby("date").sum().reset_index()
            daily_result = {
                "dates": daily_aggregated["date"].tolist()
            }
            # print("2asdfasdfasdfasdfasdfasdfasfefefefe")
            module_count_per_day = {}
            module_count_per_day["module_index"] = hourly_data["module_counts_per_hour"]["module_index"]
            for index in hourly_data["module_counts_per_hour"]["module_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                module_count_per_day[index_t] = daily_aggregated[index_t].tolist()
            
            label_count_per_day = {}
            label_count_per_day["_label_index"] = hourly_data["label_counts_per_hour"]["_label_index"]
            for index in hourly_data["label_counts_per_hour"]["_label_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                label_count_per_day[index_t] = daily_aggregated[index_t].tolist()

            method_count_per_day = {}
            method_count_per_day["method_index"] = hourly_data["method_counts_per_hour"]["method_index"]
            for index in hourly_data["method_counts_per_hour"]["method_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                method_count_per_day[index_t] = daily_aggregated[index_t].tolist()

            repo_count_per_day = {}
            repo_count_per_day["repo_index"] = hourly_data["repo_counts_per_hour"]["repo_index"]
            for index in hourly_data["repo_counts_per_hour"]["repo_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                repo_count_per_day[index_t] = daily_aggregated[index_t].tolist()

            group_count_per_day = {}
            group_count_per_day["_group_index"] = hourly_data["group_counts_per_hour"]["_group_index"]
            for index in hourly_data["group_counts_per_hour"]["_group_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                group_count_per_day[index_t] = daily_aggregated[index_t].tolist()

            daily_result["module_counts_per_day"] = module_count_per_day
            daily_result["label_counts_per_day"] = label_count_per_day
            daily_result["method_counts_per_day"] = method_count_per_day
            daily_result["repo_counts_per_day"] = repo_count_per_day
            daily_result["group_counts_per_day"] = group_count_per_day
            daily_result["log_counts_per_day"] = daily_aggregated["log_counts_per_hour"].tolist()

            return daily_result
        except Exception as e:
            logger.error(f"Error in summing data by day: {e}")
            return False
        
    @st.cache_data(ttl=60)
    def sum_by_month(collection_name):
        try:
            with open(f'forgraph/{collection_name}_graph.json') as file:
                hourly_data = json.load(file)
            # print("0asdfasdfasdfasdfasdfasdfasfefefefe")
            module = {**hourly_data["module_counts_per_hour"]}
            del module["module_index"]
            label = {**hourly_data["label_counts_per_hour"]}
            del label["_label_index"]
            method = {**hourly_data["method_counts_per_hour"]}
            del method["method_index"]
            repo = {**hourly_data["repo_counts_per_hour"]}
            del repo["repo_index"]
            log = {"log_counts_per_hour": hourly_data["log_counts_per_hour"]}
            group = {**hourly_data["group_counts_per_hour"]}
            del group["_group_index"]
            # print("1asdfasdfasdfasdfasdfasdfasfefefefe")
            monthly_data = pd.DataFrame({
                "date": [d[:7] for d in hourly_data["dates"]],  # Extract YYYY-MM
                **module,
                **label,
                **method,
                **repo,
                **log,
                **group,
            })
            monthly_aggregated = monthly_data.groupby("date").sum().reset_index()
            monthly_result = {
                "dates": monthly_aggregated["date"].tolist()
            }
            # print("2asdfasdfasdfasdfasdfasdfasfefefefe")
            module_count_per_month = {}
            module_count_per_month["module_index"] = hourly_data["module_counts_per_hour"]["module_index"]
            for index in hourly_data["module_counts_per_hour"]["module_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                module_count_per_month[index_t] = monthly_aggregated[index_t].tolist()
            
            label_count_per_month = {}
            label_count_per_month["_label_index"] = hourly_data["label_counts_per_hour"]["_label_index"]
            for index in hourly_data["label_counts_per_hour"]["_label_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                label_count_per_month[index_t] = monthly_aggregated[index_t].tolist()

            method_count_per_month = {}
            method_count_per_month["method_index"] = hourly_data["method_counts_per_hour"]["method_index"]
            for index in hourly_data["method_counts_per_hour"]["method_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                method_count_per_month[index_t] = monthly_aggregated[index_t].tolist()

            repo_count_per_month = {}
            repo_count_per_month["repo_index"] = hourly_data["repo_counts_per_hour"]["repo_index"]
            for index in hourly_data["repo_counts_per_hour"]["repo_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                repo_count_per_month[index_t] = monthly_aggregated[index_t].tolist()

            group_count_per_month = {}
            group_count_per_month["_group_index"] = hourly_data["group_counts_per_hour"]["_group_index"]
            for index in hourly_data["group_counts_per_hour"]["_group_index"]:
                if index == None:
                    index_t = "null"
                else:
                    index_t = str(index)
                group_count_per_month[index_t] = monthly_aggregated[index_t].tolist()

            monthly_result["module_counts_per_month"] = module_count_per_month
            monthly_result["label_counts_per_month"] = label_count_per_month
            monthly_result["method_counts_per_month"] = method_count_per_month
            monthly_result["repo_counts_per_month"] = repo_count_per_month
            monthly_result["group_counts_per_month"] = group_count_per_month
            monthly_result["log_counts_per_month"] = monthly_aggregated["log_counts_per_hour"].tolist()

            return monthly_result
        except Exception as e:
            logger.error(f"Error in summing data by day: {e}")
            return False
    
    with Hour:
        st.header("Number of Test over time by Station")
        data = sum_by_hour(st.session_state.selected_station)
        time_logs = pd.DataFrame({
            "timestamp": data["dates"],
            "log_counts_per_hour": data["log_counts_per_hour"]
        })
        fig = go.Figure()
        fig.add_trace(go.Scatter(x = time_logs['timestamp'], y = time_logs['log_counts_per_hour'], mode = 'lines', name = 'Log counts per hour', showlegend=True))
        fig.update_layout(
            title="Number of Test over time by Station",
            xaxis_title="Timestamp",
            yaxis_title="Log counts per hour",
            hovermode="closest",
            xaxis_rangeslider_visible=False,
            legend=dict(
                title="Tests",  # Title for the legend
                x=0.5,           # Center the legend horizontally
                y=1.1,         # Position the legend below the plot
                xanchor="center",  # Anchor the x position at the center
                yanchor="bottom",     # Anchor the y position at the top
                traceorder="normal",  # Order the traces in the legend normally
                font=dict(size=12),   # Font size for legend items
            ),
            xaxis=dict(
                tickmode='array',  # Use a specific set of tick values
                tickvals=time_logs['timestamp'][::96],  # Show every 24th timestamp (change this number as needed)
                ticktext=time_logs['timestamp'][::96],  # Custom labels for tick marks (you can customize this)
                tickangle=45  # Rotate the tick labels to make them more readable
            )
        )
        st.plotly_chart(fig)
        # st.line_chart(time_logs, x="timestamp", y="log_counts_per_hour")

        hour_group, hour_label = st.columns(2)

        with hour_group:
            st.header("Number of Groups over Time")
            group_time_json = data["group_counts_per_hour"].copy()
            del group_time_json["_group_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            fig = go.Figure()
            for group in group_time_json:
                if group != "_group_index":  # Avoid plotting the '_group_index'
                    fig.add_trace(go.Scatter(x=time_groups['timestamp'], y=time_groups[group], mode='lines', name=group, showlegend=True))
            fig.update_layout(
                title="Number of Groups over Time",
                xaxis_title="Timestamp",
                yaxis_title="Group Counts",
                hovermode="closest",
                xaxis_rangeslider_visible=False,
                legend=dict(
                    title="Groups",  # Title for the legend
                    x=0.5,           # Center the legend horizontally
                    y=1.1,         # Position the legend below the plot
                    xanchor="center",  # Anchor the x position at the center
                    yanchor="bottom",     # Anchor the y position at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                ),
                xaxis=dict(
                    tickmode='array',  # Use a specific set of tick values
                    tickvals=time_groups['timestamp'][::192],  # Show every 24th timestamp (change this number as needed)
                    ticktext=time_groups['timestamp'][::192],  # Custom labels for tick marks (you can customize this)
                    tickangle=45  # Rotate the tick labels to make them more readable
                )
            )
            st.plotly_chart(fig)

        with hour_label:
            st.header("Number of Labels over Time")
            label_time_json = data["label_counts_per_hour"].copy()
            del label_time_json["_label_index"]
            time_labels = pd.DataFrame({
                "timestamp": data["dates"],
                **label_time_json
            })
            fig = go.Figure()
            for label in label_time_json:
                if label != "_label_index":  # Avoid plotting the '_group_index'
                    fig.add_trace(go.Scatter(x=time_labels['timestamp'], y=time_labels[label], mode='lines', name=label, showlegend=True))
            fig.update_layout(
                title="Number of Labels over Time",
                xaxis_title="Timestamp",
                yaxis_title="Label Counts",
                hovermode="closest",
                xaxis_rangeslider_visible=False,
                legend=dict(
                    title="Labels",  # Title for the legend
                    x=0.5,           # Center the legend horizontally
                    y=1.1,         # Position the legend below the plot
                    xanchor="center",  # Anchor the x position at the center
                    yanchor="bottom",     # Anchor the y position at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                ),
                xaxis=dict(
                    tickmode='array',  # Use a specific set of tick values
                    tickvals=time_labels['timestamp'][::192],  # Show every 24th timestamp (change this number as needed)
                    ticktext=time_labels['timestamp'][::192],  # Custom labels for tick marks (you can customize this)
                    tickangle=45  # Rotate the tick labels to make them more readable
                )
            )
            st.plotly_chart(fig)

        hour_repo, hour_method = st.columns(2)

        with hour_repo:
            st.header("Number of Repos over Time")
            # print(data["group_counts_per_hour"])
            # print("1q1q1q1q1", data["group_counts_per_hour"])
            repo_time_json = data["repo_counts_per_hour"].copy()
            del repo_time_json["repo_index"]
            time_repos = pd.DataFrame({
                "timestamp": data["dates"],
                **repo_time_json
            })
            repository_index = data["repo_counts_per_hour"]["repo_index"]
            fig = go.Figure()

            # Add a trace for each repository
            for repo in repository_index:
                if repo is None:
                    fig.add_trace(go.Scatter(
                        x=time_repos['timestamp'],
                        y=[0] * len(time_repos),  # Replace None with zero or a default value
                        mode='lines',
                        name="null repo",  # Label the trace as "null repo" in the legend
                        showlegend=True
                    ))
                else:
                    fig.add_trace(go.Scatter(
                        x=time_repos['timestamp'],
                        y=time_repos[repo],  # Use the repository's count values
                        mode='lines',
                        name=repo,  # Use the repository name in the legend
                        showlegend=True
                    ))

            # Customize the layout
            fig.update_layout(
                title="Number of Repos over Time",
                xaxis_title="Timestamp",
                yaxis_title="Repo Counts",
                hovermode="closest",
                xaxis_rangeslider_visible=False,  # Adds a range slider for zoom functionality
                legend=dict(
                    title="Repositories",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                ),
                xaxis=dict(
                    tickmode='array',  # Use a specific set of tick values
                    tickvals=time_repos['timestamp'][::192],  # Show every 24th timestamp (change this number as needed)
                    ticktext=time_repos['timestamp'][::192],  # Custom labels for tick marks (you can customize this)
                    tickangle=45  # Rotate the tick labels to make them more readable
                )
            )

            # Display the plot in Streamlit using Plotly
            st.plotly_chart(fig)

        with hour_method:
            st.header("Number of Methods over Time")
            methods_time_json = data["method_counts_per_hour"].copy()
            del methods_time_json["method_index"]
            time_methods = pd.DataFrame({
                "timestamp": data["dates"],
                **methods_time_json
            })
            method_index = data["method_counts_per_hour"]["method_index"]
            # Create the Plotly figure
            fig = go.Figure()
            # Add a trace for each method
            for method in method_index:
                fig.add_trace(go.Scatter(
                    x=time_methods['timestamp'],
                    y=time_methods[method],  # Use the method count values for each method
                    mode='lines',
                    name=method,  # Use the method name in the legend
                    showlegend=True
                ))
            # Customize the layout for better readability and style
            fig.update_layout(
                title="Number of Methods over Time",
                xaxis_title="Timestamp",
                yaxis_title="Method Count",
                showlegend=True,  # Display the legend
                legend=dict(
                    title="Methods",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                ),
                xaxis=dict(
                    tickmode='array',  # Use a specific set of tick values
                    tickvals=time_methods['timestamp'][::192],  # Show every 24th timestamp (change this number as needed)
                    ticktext=time_methods['timestamp'][::192],  # Custom labels for tick marks (you can customize this)
                    tickangle=45  # Rotate the tick labels to make them more readable
                )
            )
            # Display the plotly chart in Streamlit
            st.plotly_chart(fig)

        st.header("Number of Modules over Time")
        module_time_json = data["module_counts_per_hour"].copy()
        del module_time_json["module_index"]
        time_modules = pd.DataFrame({
            "timestamp": data["dates"],
            **module_time_json
        })
        module_index = data["module_counts_per_hour"]["module_index"]

        # Create the Plotly figure
        fig = go.Figure()

        # Add a trace for each module
        for module in module_index:
            fig.add_trace(go.Scatter(
                x=time_modules['timestamp'],
                y=time_modules[module],  # Use the module count values for each module
                mode='lines',
                name=module,  # Use the module name in the legend
                showlegend=True
            ))

        # Customize the layout for better readability and style
        fig.update_layout(
            title="Number of Modules over Time",
            xaxis_title="Timestamp",
            yaxis_title="Module Count",
            showlegend=True,  # Display the legend
            legend=dict(
                    title="Modules",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                ),
                xaxis=dict(
                    tickmode='array',  # Use a specific set of tick values
                    tickvals=time_methods['timestamp'][::96],  # Show every 24th timestamp (change this number as needed)
                    ticktext=time_methods['timestamp'][::96],  # Custom labels for tick marks (you can customize this)
                    tickangle=45  # Rotate the tick labels to make them more readable
                )
        )

        # Display the plotly chart in Streamlit
        st.plotly_chart(fig)

    with Day:
        st.header("Number of Test over time by Station")
        data = sum_by_day(st.session_state.selected_station)
        time_logs = pd.DataFrame({
            "timestamp": data["dates"],
            "log_counts_per_day": data["log_counts_per_day"]
        })
        fig = go.Figure()
        fig.add_trace(go.Scatter(x = time_logs['timestamp'], y = time_logs['log_counts_per_day'], mode = 'lines', name = 'Log counts per day', showlegend=True))
        fig.update_layout(
            title="Number of Test over time by Station",
            xaxis_title="Timestamp",
            yaxis_title="Log counts per day",
            hovermode="closest",
            xaxis_rangeslider_visible=False,
            legend=dict(
                title="Tests",  # Title for the legend
                x=0.5,           # Center the legend horizontally
                y=1.1,         # Position the legend below the plot
                xanchor="center",  # Anchor the x position at the center
                yanchor="bottom",     # Anchor the y position at the top
                traceorder="normal",  # Order the traces in the legend normally
                font=dict(size=12),   # Font size for legend items
            )
        )
        st.plotly_chart(fig)
        # st.line_chart(time_logs, x="timestamp", y="log_counts_per_hour")

        day_group, day_label = st.columns(2)

        with day_group:
            st.header("Number of Groups over Time")
            group_time_json = data["group_counts_per_day"].copy()
            del group_time_json["_group_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            fig = go.Figure()
            for group in group_time_json:
                if group != "_group_index":  # Avoid plotting the '_group_index'
                    fig.add_trace(go.Scatter(x=time_groups['timestamp'], y=time_groups[group], mode='lines', name=group, showlegend=True))
            fig.update_layout(
                title="Number of Groups over Time",
                xaxis_title="Timestamp",
                yaxis_title="Group Counts Per Day",
                hovermode="closest",
                xaxis_rangeslider_visible=False,
                legend=dict(
                    title="Groups",  # Title for the legend
                    x=0.5,           # Center the legend horizontally
                    y=1.1,         # Position the legend below the plot
                    xanchor="center",  # Anchor the x position at the center
                    yanchor="bottom",     # Anchor the y position at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )
            st.plotly_chart(fig)

        with day_label:
            st.header("Number of Labels over Time")
            label_time_json = data["label_counts_per_day"].copy()
            del label_time_json["_label_index"]
            time_labels = pd.DataFrame({
                "timestamp": data["dates"],
                **label_time_json
            })
            fig = go.Figure()
            for label in label_time_json:
                if label != "_label_index":  # Avoid plotting the '_group_index'
                    fig.add_trace(go.Scatter(x=time_labels['timestamp'], y=time_labels[label], mode='lines', name=label, showlegend=True))
            fig.update_layout(
                title="Number of Labels over Time",
                xaxis_title="Timestamp",
                yaxis_title="Label Counts Per Day",
                hovermode="closest",
                xaxis_rangeslider_visible=False,
                legend=dict(
                    title="Labels",  # Title for the legend
                    x=0.5,           # Center the legend horizontally
                    y=1.1,         # Position the legend below the plot
                    xanchor="center",  # Anchor the x position at the center
                    yanchor="bottom",     # Anchor the y position at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )
            st.plotly_chart(fig)

        day_repo, day_method = st.columns(2)

        with day_repo:
            st.header("Number of Repos over Time")
            # print(data["group_counts_per_hour"])
            # print("1q1q1q1q1", data["group_counts_per_hour"])
            repo_time_json = data["repo_counts_per_day"].copy()
            del repo_time_json["repo_index"]
            time_repos = pd.DataFrame({
                "timestamp": data["dates"],
                **repo_time_json
            })
            repository_index = data["repo_counts_per_day"]["repo_index"]
            fig = go.Figure()

            # Add a trace for each repository
            for repo in repository_index:
                if repo is None:
                    fig.add_trace(go.Scatter(
                        x=time_repos['timestamp'],
                        y=[0] * len(time_repos),  # Replace None with zero or a default value
                        mode='lines',
                        name="null repo",  # Label the trace as "null repo" in the legend
                        showlegend=True
                    ))
                else:
                    fig.add_trace(go.Scatter(
                        x=time_repos['timestamp'],
                        y=time_repos[repo],  # Use the repository's count values
                        mode='lines',
                        name=repo,  # Use the repository name in the legend
                        showlegend=True
                    ))

            # Customize the layout
            fig.update_layout(
                title="Number of Repos over Time",
                xaxis_title="Timestamp",
                yaxis_title="Repo Counts Per Day",
                hovermode="closest",
                xaxis_rangeslider_visible=False,  # Adds a range slider for zoom functionality
                legend=dict(
                    title="Repositories",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )

            # Display the plot in Streamlit using Plotly
            st.plotly_chart(fig)

        with day_method:
            st.header("Number of Methods over Time")
            methods_time_json = data["method_counts_per_day"].copy()
            del methods_time_json["method_index"]
            time_methods = pd.DataFrame({
                "timestamp": data["dates"],
                **methods_time_json
            })
            method_index = data["method_counts_per_day"]["method_index"]
            # Create the Plotly figure
            fig = go.Figure()
            # Add a trace for each method
            for method in method_index:
                fig.add_trace(go.Scatter(
                    x=time_methods['timestamp'],
                    y=time_methods[method],  # Use the method count values for each method
                    mode='lines',
                    name=method,  # Use the method name in the legend
                    showlegend=True
                ))
            # Customize the layout for better readability and style
            fig.update_layout(
                title="Number of Methods over Time",
                xaxis_title="Timestamp",
                yaxis_title="Method Count Per Day",
                showlegend=True,  # Display the legend
                legend=dict(
                    title="Methods",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )
            # Display the plotly chart in Streamlit
            st.plotly_chart(fig)

        st.header("Number of Modules over Time")
        module_time_json = data["module_counts_per_day"].copy()
        del module_time_json["module_index"]
        time_modules = pd.DataFrame({
            "timestamp": data["dates"],
            **module_time_json
        })
        module_index = data["module_counts_per_day"]["module_index"]

        # Create the Plotly figure
        fig = go.Figure()

        # Add a trace for each module
        for module in module_index:
            fig.add_trace(go.Scatter(
                x=time_modules['timestamp'],
                y=time_modules[module],  # Use the module count values for each module
                mode='lines',
                name=module,  # Use the module name in the legend
                showlegend=True
            ))

        # Customize the layout for better readability and style
        fig.update_layout(
            title="Number of Modules over Time",
            xaxis_title="Timestamp",
            yaxis_title="Module Count Per Day",
            showlegend=True,  # Display the legend
            legend=dict(
                    title="Modules",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
        )

        # Display the plotly chart in Streamlit
        st.plotly_chart(fig)

    with Month:
        st.header("Number of Test over time by Station")
        data = sum_by_month(st.session_state.selected_station)
        time_logs = pd.DataFrame({
            "timestamp": data["dates"],
            "log_counts_per_month": data["log_counts_per_month"]
        })
        fig = go.Figure()
        fig.add_trace(go.Scatter(x = time_logs['timestamp'], y = time_logs['log_counts_per_month'], mode = 'lines', name = 'Log counts per month', showlegend=True))
        fig.update_layout(
            title="Number of Test over time by Station",
            xaxis_title="Timestamp",
            yaxis_title="Log counts per month",
            hovermode="closest",
            xaxis_rangeslider_visible=False,
            legend=dict(
                title="Tests",  # Title for the legend
                x=0.5,           # Center the legend horizontally
                y=1.1,         # Position the legend below the plot
                xanchor="center",  # Anchor the x position at the center
                yanchor="bottom",     # Anchor the y position at the top
                traceorder="normal",  # Order the traces in the legend normally
                font=dict(size=12),   # Font size for legend items
            )
        )
        st.plotly_chart(fig)
        # st.line_chart(time_logs, x="timestamp", y="log_counts_per_hour")

        month_group, month_label = st.columns(2)

        with month_group:
            st.header("Number of Groups over Time")
            group_time_json = data["group_counts_per_month"].copy()
            del group_time_json["_group_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            fig = go.Figure()
            for group in group_time_json:
                if group != "_group_index":  # Avoid plotting the '_group_index'
                    fig.add_trace(go.Scatter(x=time_groups['timestamp'], y=time_groups[group], mode='lines', name=group, showlegend=True))
            fig.update_layout(
                title="Number of Groups over Time",
                xaxis_title="Timestamp",
                yaxis_title="Group Counts Per Month",
                hovermode="closest",
                xaxis_rangeslider_visible=False,
                legend=dict(
                    title="Groups",  # Title for the legend
                    x=0.5,           # Center the legend horizontally
                    y=1.1,         # Position the legend below the plot
                    xanchor="center",  # Anchor the x position at the center
                    yanchor="bottom",     # Anchor the y position at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )
            st.plotly_chart(fig)

        with month_label:
            st.header("Number of Labels over Time")
            label_time_json = data["label_counts_per_month"].copy()
            del label_time_json["_label_index"]
            time_labels = pd.DataFrame({
                "timestamp": data["dates"],
                **label_time_json
            })
            fig = go.Figure()
            for label in label_time_json:
                if label != "_label_index":  # Avoid plotting the '_group_index'
                    fig.add_trace(go.Scatter(x=time_labels['timestamp'], y=time_labels[label], mode='lines', name=label, showlegend=True))
            fig.update_layout(
                title="Number of Labels over Time",
                xaxis_title="Timestamp",
                yaxis_title="Label Counts Per Month",
                hovermode="closest",
                xaxis_rangeslider_visible=False,
                legend=dict(
                    title="Labels",  # Title for the legend
                    x=0.5,           # Center the legend horizontally
                    y=1.1,         # Position the legend below the plot
                    xanchor="center",  # Anchor the x position at the center
                    yanchor="bottom",     # Anchor the y position at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )
            st.plotly_chart(fig)

        month_repo, month_method = st.columns(2)

        with month_repo:
            st.header("Number of Repos over Time")
            # print(data["group_counts_per_hour"])
            # print("1q1q1q1q1", data["group_counts_per_hour"])
            repo_time_json = data["repo_counts_per_month"].copy()
            del repo_time_json["repo_index"]
            time_repos = pd.DataFrame({
                "timestamp": data["dates"],
                **repo_time_json
            })
            repository_index = data["repo_counts_per_month"]["repo_index"]
            fig = go.Figure()

            # Add a trace for each repository
            for repo in repository_index:
                if repo is None:
                    fig.add_trace(go.Scatter(
                        x=time_repos['timestamp'],
                        y=[0] * len(time_repos),  # Replace None with zero or a default value
                        mode='lines',
                        name="null repo",  # Label the trace as "null repo" in the legend
                        showlegend=True
                    ))
                else:
                    fig.add_trace(go.Scatter(
                        x=time_repos['timestamp'],
                        y=time_repos[repo],  # Use the repository's count values
                        mode='lines',
                        name=repo,  # Use the repository name in the legend
                        showlegend=True
                    ))

            # Customize the layout
            fig.update_layout(
                title="Number of Repos over Time",
                xaxis_title="Timestamp",
                yaxis_title="Repo Counts Per Month",
                hovermode="closest",
                xaxis_rangeslider_visible=False,  # Adds a range slider for zoom functionality
                legend=dict(
                    title="Repositories",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )

            # Display the plot in Streamlit using Plotly
            st.plotly_chart(fig)

        with month_method:
            st.header("Number of Methods over Time")
            methods_time_json = data["method_counts_per_month"].copy()
            del methods_time_json["method_index"]
            time_methods = pd.DataFrame({
                "timestamp": data["dates"],
                **methods_time_json
            })
            method_index = data["method_counts_per_month"]["method_index"]
            # Create the Plotly figure
            fig = go.Figure()
            # Add a trace for each method
            for method in method_index:
                fig.add_trace(go.Scatter(
                    x=time_methods['timestamp'],
                    y=time_methods[method],  # Use the method count values for each method
                    mode='lines',
                    name=method,  # Use the method name in the legend
                    showlegend=True
                ))
            # Customize the layout for better readability and style
            fig.update_layout(
                title="Number of Methods over Time",
                xaxis_title="Timestamp",
                yaxis_title="Method Count Per Month",
                showlegend=True,  # Display the legend
                legend=dict(
                    title="Methods",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
            )
            # Display the plotly chart in Streamlit
            st.plotly_chart(fig)

        st.header("Number of Modules over Time")
        module_time_json = data["module_counts_per_month"].copy()
        del module_time_json["module_index"]
        time_modules = pd.DataFrame({
            "timestamp": data["dates"],
            **module_time_json
        })
        module_index = data["module_counts_per_month"]["module_index"]

        # Create the Plotly figure
        fig = go.Figure()

        # Add a trace for each module
        for module in module_index:
            fig.add_trace(go.Scatter(
                x=time_modules['timestamp'],
                y=time_modules[module],  # Use the module count values for each module
                mode='lines',
                name=module,  # Use the module name in the legend
                showlegend=True
            ))

        # Customize the layout for better readability and style
        fig.update_layout(
            title="Number of Modules over Time",
            xaxis_title="Timestamp",
            yaxis_title="Module Count Per Month",
            showlegend=True,  # Display the legend
            legend=dict(
                    title="Modules",  # Title for the legend
                    x=0.5,                # Center the legend horizontally
                    y=1.1,                # Position the legend above the plot
                    xanchor='center',     # Ensure the legend is centered horizontally
                    yanchor='bottom',     # Position the legend at the top
                    traceorder="normal",  # Order the traces in the legend normally
                    font=dict(size=12),   # Font size for legend items
                )
        )

        # Display the plotly chart in Streamlit
        st.plotly_chart(fig)

with Metrics_Calculation_API:
    # Metrics Tab
    # Function to get metrics for the selected station
    @st.cache_data(ttl=60)  # Cache for 1 minute
    def get_station_metrics(station, group=None, label=None, repo=None, module=None):
        """Get metrics for the selected station with optional filters"""
        try:
            client = pymongo.MongoClient(CONNECTION_STRING)
            db = client[DATABASE_NAME]
            metrics_col = db[METRICS_COLLECTION]
            
            # Build query based on filters
            query = None
            
            # Check for dimension-specific data first (group, label, repo, module)
            if group:
                query = {"station": station, "group": group, "has_dimension_data": True}
            elif label:
                query = {"station": station, "label": label, "has_dimension_data": True}
            elif repo:
                query = {"station": station, "repo": repo, "has_dimension_data": True}
            elif module:
                query = {"station": station, "module": module, "has_dimension_data": True}
            else:
                # No dimension filters, get station-level metrics
                query = {"station": station}
            
            # Get the metrics document
            metrics_doc = metrics_col.find_one(
                query,
                sort=[("timestamp", -1)]
            )
            
            # If no dimension-specific metrics found but dimension filter was applied, fall back to station-level metrics
            if not metrics_doc and any([group, label, repo, module]):
                fallback_query = {"station": station, "has_dimension_data": {"$exists": False}}
                metrics_doc = metrics_col.find_one(
                    fallback_query,
                    sort=[("timestamp", -1)]
                )
            
            # If still no metrics found and this might be an individual station from a group, 
            # try looking for metrics with is_split_station flag
            if not metrics_doc:
                split_query = {"station": station, "is_split_station": True}
                metrics_doc = metrics_col.find_one(
                    split_query,
                    sort=[("timestamp", -1)]
                )
                
                # If found, add an indicator that this is from a split station
                if metrics_doc:
                    metrics_doc["from_split"] = True
            
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
            
            # Handle missing data
            if metrics.get("missing_data", False):
                # Set reasonable defaults for missing values to prevent UI errors
                # But maintain the "missing_data" flag so UI can indicate this properly
                if metrics.get("utilization_rate") is None:
                    metrics["utilization_rate"] = 0
                
                if metrics.get("downtime_percentage") is None:
                    metrics["downtime_percentage"] = 0
                
                if metrics.get("mtbf_hours") is None:
                    metrics["mtbf_hours"] = 0
                
                if metrics.get("mttr_hours") is None:
                    metrics["mttr_hours"] = 0
                
                if metrics.get("avg_test_duration_minutes") is None:
                    metrics["avg_test_duration_minutes"] = 0
            
            return metrics
        except Exception as e:
            st.error(f"Error getting metrics: {e}")
            return {}

    @st.cache_data(ttl=60)
    def get_group_metrics(station):
        """Get metrics broken down by groups for the selected station"""
        try:
            metrics = get_station_metrics(station)
            return metrics.get("group_metrics", {})
        except Exception as e:
            st.error(f"Error getting group metrics: {e}")
            return {}

    # Get metrics for the selected station
    metrics = get_station_metrics(
        st.session_state.selected_station,
        group=st.session_state.selected_group,
        label=st.session_state.selected_label,
        repo=st.session_state.selected_repo,
        module=st.session_state.selected_module
    )

    group_metrics = None
    if st.session_state.selected_station and not any([
        st.session_state.selected_group, 
        st.session_state.selected_label,
        st.session_state.selected_repo,
        st.session_state.selected_module
    ]):
        group_metrics = get_group_metrics(st.session_state.selected_station)

    # Display refresh button
    if st.button("🔄 Refresh Data"):
        get_station_metrics.clear()
        st.rerun()

    # Show metrics dashboard if metrics exist
    if metrics:
        success_msg = f"Showing metrics for {st.session_state.selected_station}"
        
        if st.session_state.selected_group:
            success_msg += f" → Group: {st.session_state.selected_group}"
        
        if st.session_state.selected_label:
            success_msg += f" → Label: {st.session_state.selected_label}"
        
        if st.session_state.selected_repo:
            success_msg += f" → Repository: {st.session_state.selected_repo}"
            
        if st.session_state.selected_module:
            success_msg += f" → Module: {st.session_state.selected_module}"
        
        # Check if we're showing dimension-specific metrics
        if metrics.get("has_dimension_data", False):
            success_msg += " (Using dimension-specific metrics)"
        
        st.success(success_msg)

        # Create metrics dashboard
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="section-header">1. Equipment Utilization Metrics</div>', unsafe_allow_html=True)
            
            # Utilization Rate card with gauge chart
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Utilization Rate (%)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">(Actual Usage Time / Total Available Time) × 100</div>', unsafe_allow_html=True)
            
            # Get utilization rate safely
            utilization_data = safe_get_metric(metrics, 'utilization_rate', 0)
            utilization = utilization_data["value"]
            is_missing = utilization_data["is_missing"]
            
            # Create gauge chart for utilization rate
            util_fig = create_gauge_chart(
                value=utilization,
                title="Equipment Utilization",
                good_threshold=75,
                warning_threshold=50,
                is_missing=is_missing
            )
            st.plotly_chart(util_fig, use_container_width=True)
            
            # Display utilization by groups if available
            if group_metrics and len(group_metrics) > 0:
                st.markdown("<b>Utilization by Group:</b>", unsafe_allow_html=True)
                
                # Check if we have any groups with utilization values
                has_util_data = any(group_data.get("utilization") is not None for group_data in group_metrics.values())
                
                if has_util_data:
                    # Define safe sort key
                    def util_sort_key(item):
                        util = item[1].get("utilization")
                        return util if util is not None else -1  # Put None values at the end
                    
                    # Sort groups by utilization
                    sorted_groups = sorted(group_metrics.items(), key=util_sort_key, reverse=True)
                    
                    # Display top 5 groups
                    for group, group_data in sorted_groups[:5]:
                        util_value = group_data.get("utilization")
                        if util_value is not None:
                            # Add color indicators based on value
                            if util_value >= 75:
                                indicator = '<span class="good-indicator">■</span>'
                            elif util_value >= 50:
                                indicator = '<span class="warning-indicator">■</span>'
                            else:
                                indicator = '<span class="danger-indicator">■</span>'
                            
                            st.markdown(f"- {indicator} {group}: {util_value:.1f}%", unsafe_allow_html=True)
                    
                    # Show count message if there are more
                    if len(sorted_groups) > 5:
                        st.markdown(f"<i>and {len(sorted_groups) - 5} more groups...</i>", unsafe_allow_html=True)
                else:
                    st.markdown("<i>No utilization data available for groups</i>", unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Downtime card
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Downtime (%)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">(Downtime Hours / Total Available Hours) × 100</div>', unsafe_allow_html=True)
            
            # Get downtime percentage safely
            downtime_data = safe_get_metric(metrics, 'downtime_percentage', 0)
            downtime = downtime_data["value"]
            is_missing = downtime_data["is_missing"]
            
            # Create gauge chart for downtime
            downtime_fig = create_gauge_chart(
                value=downtime,
                title="Equipment Downtime",
                good_threshold=5,  # Lower is better for downtime
                warning_threshold=15,
                is_missing=is_missing
            )
            st.plotly_chart(downtime_fig, use_container_width=True)
            
            # Show downtime by station if available
            if not is_missing:
                st.markdown(f"<b>Current Downtime:</b> {downtime:.1f}%", unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
             # Test Execution Metrics
            st.markdown('<div class="section-header">2. Test Execution Metrics</div>', unsafe_allow_html=True)
            
            # Tests Per Day
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Tests Per Equipment Per Day</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">Total Tests / (Equipment Units × Days)</div>', unsafe_allow_html=True)
            
            # Get tests per day safely
            tests_per_day_data = safe_get_metric(metrics, 'tests_per_day', 0)
            tests_per_day = tests_per_day_data["value"]
            is_missing = tests_per_day_data["is_missing"]
            
            if is_missing:
                st.markdown('<div class="metric-value missing-data">No Data</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="metric-value">{tests_per_day:.1f}</div>', unsafe_allow_html=True)
            
            # Show tests by group if available
            if group_metrics and len(group_metrics) > 0:
                st.markdown("<b>Test Counts by Group:</b>", unsafe_allow_html=True)
                
                # Define safe sort key for count
                def count_sort_key(item):
                    count = item[1].get("count")
                    return count if count is not None else -1
                
                # Sort groups by count
                sorted_by_count = sorted(group_metrics.items(), key=count_sort_key, reverse=True)
                
                # Display top 5 groups
                for group, group_data in sorted_by_count[:5]:
                    if group_data.get("count") is not None:
                        st.markdown(f"- {group}: {group_data['count']} tests", unsafe_allow_html=True)
                
                # Show count message if there are more
                if len(sorted_by_count) > 5:
                    st.markdown(f"<i>and {len(sorted_by_count) - 5} more groups...</i>", unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Average Test Duration
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Average Test Duration</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">Total Test Time / Total Tests Conducted</div>', unsafe_allow_html=True)
            
            # Get average test duration safely
            avg_duration_data = safe_get_metric(metrics, 'avg_test_duration_minutes', 0)
            avg_duration = avg_duration_data["value"]
            is_missing = avg_duration_data["is_missing"]
            
            if is_missing:
                st.markdown('<div class="metric-value missing-data">No Data</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="metric-value">{avg_duration:.2f} min</div>', unsafe_allow_html=True)
            
            # Show duration by group if available
            if group_metrics and len(group_metrics) > 0:
                st.markdown("<b>Test Duration by Group:</b>", unsafe_allow_html=True)
                
                # Check if we have any groups with avg_duration values
                has_duration_data = any(group_data.get("avg_duration") is not None for group_data in group_metrics.values())
                
                if has_duration_data:
                    # Define safe sort key for duration
                    def duration_sort_key(item):
                        duration = item[1].get("avg_duration")
                        return duration if duration is not None else -1
                    
                    # Sort groups by duration
                    sorted_by_duration = sorted(group_metrics.items(), key=duration_sort_key, reverse=True)
                    
                    # Display top 5 groups
                    for group, group_data in sorted_by_duration[:5]:
                        duration = group_data.get("avg_duration")
                        if duration is not None:
                            st.markdown(f"- {group}: {duration:.2f} min", unsafe_allow_html=True)
                    
                    # Show count message if there are more
                    if len(sorted_by_duration) > 5:
                        st.markdown(f"<i>and {len(sorted_by_duration) - 5} more groups...</i>", unsafe_allow_html=True)
                else:
                    st.markdown("<i>No duration data available for groups</i>", unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="section-header">3. Maintenance & Calibration Metrics</div>', unsafe_allow_html=True)
            
            # MTBF
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Mean Time Between Failures (MTBF)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">Total Operating Time / Number of Failures</div>', unsafe_allow_html=True)
            
            # Get MTBF safely
            mtbf_data = safe_get_metric(metrics, 'mtbf_hours', 0)
            mtbf = mtbf_data["value"]
            is_missing = mtbf_data["is_missing"]
            
            if is_missing:
                # Create gauge with missing data indicator
                mtbf_fig = create_gauge_chart(
                    value=0,
                    title="MTBF (hours)",
                    good_threshold=75,
                    warning_threshold=40,
                    is_missing=True
                )
                st.plotly_chart(mtbf_fig, use_container_width=True)
            else:
                # Create mtbf display that shows the actual value and the gauge scaled to 0-100%
                scaled_mtbf = min(100, mtbf/10)  # Scale to percentage (max 1000 hours = 100%)
                
                # Create gauge for MTBF (higher is better)
                mtbf_fig = create_gauge_chart(
                    value=scaled_mtbf,
                    title=f"MTBF: {mtbf:.1f} hours",
                    good_threshold=75,
                    warning_threshold=40
                )
                st.plotly_chart(mtbf_fig, use_container_width=True)
            
            st.markdown('<div class="info-text">Higher values indicate better equipment reliability.</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            
            # MTTR
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Mean Time To Repair (MTTR)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">Total Repair Time / Number of Repairs</div>', unsafe_allow_html=True)
            
            # Get MTTR safely
            mttr_data = safe_get_metric(metrics, 'mttr_hours', 0)
            mttr = mttr_data["value"]
            is_missing = mttr_data["is_missing"]
            
            if is_missing:
                # Create gauge with missing data indicator
                mttr_fig = create_gauge_chart(
                    value=0,
                    title="MTTR (hours)",
                    good_threshold=75,
                    warning_threshold=40,
                    is_missing=True
                )
                st.plotly_chart(mttr_fig, use_container_width=True)
            else:
                # Scale for gauge - lower is better, so we invert the scale
                # MTTR of 0 = 100%, MTTR of 10+ = 0%
                scaled_mttr = max(0, 100 - (mttr * 10))
                
                # Create gauge for MTTR (lower is better)
                mttr_fig = create_gauge_chart(
                    value=scaled_mttr,
                    title=f"MTTR: {mttr:.1f} hours",
                    good_threshold=75,  # Higher on gauge = lower actual MTTR
                    warning_threshold=40
                )
                st.plotly_chart(mttr_fig, use_container_width=True)
            
            st.markdown('<div class="info-text">Lower values indicate faster repair times.</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Calibration Compliance
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Calibration Compliance Rate (%)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">(Calibrated Equipment on Time / Total Due for Calibration) × 100</div>', unsafe_allow_html=True)
            
            # Get calibration compliance safely
            cal_data = safe_get_metric(metrics, 'calibration_compliance', 0)
            calibration = cal_data["value"]
            is_missing = cal_data["is_missing"]
            
            # Create gauge for Calibration Compliance
            cal_fig = create_gauge_chart(
                value=calibration,
                title="Calibration Compliance",
                good_threshold=90,
                warning_threshold=80,
                is_missing=is_missing
            )
            st.plotly_chart(cal_fig, use_container_width=True)
            
            st.markdown('<div class="info-text">Higher values indicate better compliance with calibration schedules.</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Second row
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="section-header">4. Cost & Efficiency Metrics</div>', unsafe_allow_html=True)
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
            # depreciation = float(metrics.get('equipment_depreciation_rate', metrics.get('depreciation_rate', 15.3)))

            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Equipment Depreciation Rate (%)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">(Initial Value - Current Value / Initial Value) × 100</div>', unsafe_allow_html=True)

            depreciation_data = safe_get_metric(metrics, 'equipment_depreciation_rate', 0)
            depreciation = depreciation_data["value"]
            is_missing = depreciation_data["is_missing"]
            
            # Create gauge for Calibration Compliance
            cal_fig = create_gauge_chart(
                value=depreciation,
                title="Equipment Depreciation Rate",
                good_threshold=1,
                warning_threshold=20,
                is_missing=is_missing
            )
            st.plotly_chart(cal_fig, use_container_width=True)
        
        with col2:
            st.markdown('<div class="section-header">5. Availability & Scheduling Metrics</div>', unsafe_allow_html=True)
            # Utilization Rate card with gauge chart
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Equipment Availability (%)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">(Total Available Hours - Downtime Hours / Total Available Hours) × 100</div>', unsafe_allow_html=True)
            availability = 100 - downtime
            # Get utilization rate safely
            utilization_data = safe_get_metric(metrics, 'utilization_rate', 0)
            utilization = utilization_data["value"]
            is_missing = utilization_data["is_missing"]
            
            # Create gauge chart for utilization rate
            util_fig = create_gauge_chart(
                value=utilization,
                title="Equipment Availability",
                good_threshold=75,
                warning_threshold=50,
                is_missing=is_missing
            )
            st.plotly_chart(util_fig, use_container_width=True)
            
            # Booking vs Usage Discrepancy
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown('<div class="metric-title">Booking vs Usage Discrepancy (%)</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-formula">(Scheduled Time - Actual Used Time) / Scheduled Time × 100</div>', unsafe_allow_html=True)

            booking_discrepancy_data = safe_get_metric(metrics, 'booking_discrepancy', 15.3)
            booking_discrepancy = booking_discrepancy_data["value"]
            is_missing = booking_discrepancy_data["is_missing"]
            util_fig = create_gauge_chart(
                value=booking_discrepancy,
                title="Booking Discrepancy",
                good_threshold=10,
                warning_threshold=15,
                is_missing=is_missing
            )
            st.plotly_chart(util_fig, use_container_width=True)
        
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