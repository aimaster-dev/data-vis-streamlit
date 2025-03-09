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
                )
            )
            st.plotly_chart(fig)

        hour_repo, hour_method = st.columns(2)

        with hour_repo:
            st.header("Number of Repos over Time")
            # print(data["group_counts_per_hour"])
            # print("1q1q1q1q1", data["group_counts_per_hour"])
            group_time_json = data["repo_counts_per_hour"].copy()
            del group_time_json["repo_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            repository_index = data["repo_counts_per_hour"]["repo_index"]
            if None in repository_index:
                # If there's a None, display a default "null repo" value for y-axis
                st.line_chart(time_groups, x="timestamp", y=["null"])
            else:
                st.line_chart(time_groups, x="timestamp", y=repository_index)

        with hour_method:
            st.header("Number of Methods over Time")
            # print(data["group_counts_per_hour"])
            # print("1q1q1q1q1", data["group_counts_per_hour"])
            group_time_json = data["method_counts_per_hour"].copy()
            del group_time_json["method_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            st.line_chart(time_groups, x="timestamp", y=data["method_counts_per_hour"]["method_index"])

        st.header("Number of Modules over Time")
        # print(data["group_counts_per_hour"])
        # print("1q1q1q1q1", data["group_counts_per_hour"])
        group_time_json = data["module_counts_per_hour"].copy()
        del group_time_json["module_index"]
        time_groups = pd.DataFrame({
            "timestamp": data["dates"],
            **group_time_json
        })
        st.line_chart(time_groups, x="timestamp", y=data["module_counts_per_hour"]["module_index"])

    with Day:
        start_date = datetime.now() - timedelta(days=90)
        st.header("Number of Test over time by Station")
        data = sum_by_day(st.session_state.selected_station)
        time_logs = pd.DataFrame({
            "timestamp": data["dates"],
            "log_counts_per_day": data["log_counts_per_day"]
        })

        st.line_chart(time_logs, x="timestamp", y="log_counts_per_day")

        day_group, day_label = st.columns(2)

        with day_group:
            st.header("Number of Groups over Time")
            group_time_json = data["group_counts_per_day"].copy()
            del group_time_json["_group_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            st.line_chart(time_groups, x="timestamp", y=data["group_counts_per_day"]["_group_index"])

        with day_label:
            st.header("Number of Labels over Time")
            label_time_json = data["label_counts_per_day"].copy()
            del label_time_json["_label_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **label_time_json
            })
            st.line_chart(time_groups, x="timestamp", y=data["label_counts_per_day"]["_label_index"])

        day_repo, day_method = st.columns(2)

        with day_repo:
            st.header("Number of Repos over Time")
            group_time_json = data["repo_counts_per_day"].copy()
            del group_time_json["repo_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            repository_index = data["repo_counts_per_day"]["repo_index"]
            if None in repository_index:
                # If there's a None, display a default "null repo" value for y-axis
                st.line_chart(time_groups, x="timestamp", y=["null"])
            else:
                st.line_chart(time_groups, x="timestamp", y=repository_index)

        with day_method:
            st.header("Number of Methods over Time")
            # print(data["group_counts_per_hour"])
            # print("1q1q1q1q1", data["group_counts_per_hour"])
            group_time_json = data["method_counts_per_day"].copy()
            del group_time_json["method_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            st.line_chart(time_groups, x="timestamp", y=data["method_counts_per_day"]["method_index"])

        st.header("Number of Modules over Time")
        # print(data["group_counts_per_hour"])
        # print("1q1q1q1q1", data["group_counts_per_hour"])
        group_time_json = data["module_counts_per_day"].copy()
        del group_time_json["module_index"]
        time_groups = pd.DataFrame({
            "timestamp": data["dates"],
            **group_time_json
        })
        st.line_chart(time_groups, x="timestamp", y=data["module_counts_per_day"]["module_index"])

    with Month:
        start_date = datetime.now() - timedelta(days=90)
        st.header("Number of Test over time by Station")
        data = sum_by_month(st.session_state.selected_station)
        time_logs = pd.DataFrame({
            "timestamp": data["dates"],
            "log_counts_per_month": data["log_counts_per_month"]
        })

        st.line_chart(time_logs, x="timestamp", y="log_counts_per_month")

        month_group, month_label = st.columns(2)

        with month_group:
            st.header("Number of Groups over Time")
            group_time_json = data["group_counts_per_month"].copy()
            del group_time_json["_group_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            st.line_chart(time_groups, x="timestamp", y=data["group_counts_per_month"]["_group_index"])

        with month_label:
            st.header("Number of Labels over Time")
            label_time_json = data["label_counts_per_month"].copy()
            del label_time_json["_label_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **label_time_json
            })
            st.line_chart(time_groups, x="timestamp", y=data["label_counts_per_month"]["_label_index"])

        month_repo, month_method = st.columns(2)

        with month_repo:
            st.header("Number of Repos over Time")
            group_time_json = data["repo_counts_per_month"].copy()
            del group_time_json["repo_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            repository_index = data["repo_counts_per_month"]["repo_index"]
            if None in repository_index:
                # If there's a None, display a default "null repo" value for y-axis
                st.line_chart(time_groups, x="timestamp", y=["null"])
            else:
                st.line_chart(time_groups, x="timestamp", y=repository_index)

        with month_method:
            st.header("Number of Methods over Time")
            # print(data["group_counts_per_hour"])
            # print("1q1q1q1q1", data["group_counts_per_hour"])
            group_time_json = data["method_counts_per_month"].copy()
            del group_time_json["method_index"]
            time_groups = pd.DataFrame({
                "timestamp": data["dates"],
                **group_time_json
            })
            st.line_chart(time_groups, x="timestamp", y=data["method_counts_per_month"]["method_index"])

        st.header("Number of Modules over Time")
        # print(data["group_counts_per_hour"])
        # print("1q1q1q1q1", data["group_counts_per_hour"])
        group_time_json = data["module_counts_per_month"].copy()
        del group_time_json["module_index"]
        time_groups = pd.DataFrame({
            "timestamp": data["dates"],
            **group_time_json
        })
        st.line_chart(time_groups, x="timestamp", y=data["module_counts_per_month"]["module_index"])

with Metrics_Calculation_API:
    # Metrics Tab
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