# components/metrics.py
import streamlit as st
import pandas as pd
import sys
sys.path.append(".")

def render_metrics(df, db, get_collections_func, get_unique_values_func):
    """
    Render metric cards based on filtered data.
    
    Args:
        df: pandas DataFrame containing filtered data
        db: MongoDB database connection
        get_collections_func: Function to get collections
        get_unique_values_func: Function to get unique values
    """
    
    # Calculate metrics from filtered data
    if df.empty:
        # When no data matches the filters, fetch total counts
        try:
            num_stations = len(get_collections_func(db))
            
            # For other metrics, we'll count by getting all unique values
            num_groups = len(get_unique_values_func(db, "_group"))
            num_labels = len(get_unique_values_func(db, "_label"))
            num_repos = len(get_unique_values_func(db, "repo"))
            num_methods = len(get_unique_values_func(db, "module"))  # Using module as method
        except Exception as e:
            st.error(f"Error fetching metrics: {str(e)}")
            num_stations = 1.0
            num_groups = 1.0
            num_labels = 2.0
            num_repos = 0.0
            num_methods = 3.0
    else:
        # When we have filtered data, count from the DataFrame
        try:
            num_stations = df['station'].nunique() if 'station' in df.columns else 1.0
            num_groups = df['_group'].nunique() if '_group' in df.columns else 1.0
            num_labels = df['_label'].nunique() if '_label' in df.columns else 2.0
            num_repos = df['repo'].nunique() if 'repo' in df.columns else 0.0
            num_methods = df['module'].nunique() if 'module' in df.columns else 3.0
        except Exception as e:
            st.error(f"Error calculating metrics from data: {str(e)}")
            num_stations = 1.0
            num_groups = 1.0
            num_labels = 2.0
            num_repos = 0.0
            num_methods = 3.0
    
    # Create a container with custom styling to match the screenshot
    st.markdown(
        """
        <style>
        .metrics-container {
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            display: flex;
            justify-content: space-between;
        }
        .metric-box {
            text-align: center;
            flex: 1;
            padding: 0 10px;
        }
        .metric-title {
            color: #666;
            font-size: 14px;
            margin-bottom: 8px;
        }
        .metric-value {
            color: #333;
            font-size: 28px;
            font-weight: bold;
        }
        </style>
        """, 
        unsafe_allow_html=True
    )
    
    # Create the metrics container
    st.markdown(
        f"""
        <div class="metrics-container">
            <div class="metric-box">
                <div class="metric-title">No. of Stations</div>
                <div class="metric-value">{num_stations}</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">No. of Groups</div>
                <div class="metric-value">{num_groups}</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">No. of Labels</div>
                <div class="metric-value">{num_labels}</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">No. of Repo</div>
                <div class="metric-value">{num_repos}</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">No. of Method</div>
                <div class="metric-value">{num_methods}</div>
            </div>
        </div>
        """, 
        unsafe_allow_html=True
    )