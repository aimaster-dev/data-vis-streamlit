# components/filters.py
import streamlit as st
import sys
sys.path.append(".")
from utils.database import get_collections, get_unique_values

def render_filters(db):
    """
    Render filters dynamically and update options based on selections.
    
    Args:
        db: MongoDB database connection
    """
    
    # Get filter options based on current selections
    stations = ["Select Station"] + get_collections(db)
    groups = ["Select Group"] + get_unique_values(
        db, 
        "_group", 
        station_filter=st.session_state.selected_station if st.session_state.selected_station != "Select Station" else None
    )
    labels = ["Select Label"] + get_unique_values(
        db,
        "_label", 
        station_filter=st.session_state.selected_station if st.session_state.selected_station != "Select Station" else None, 
        group_filter=st.session_state.selected_group if st.session_state.selected_group != "Select Group" else None
    )
    repos = ["Select Repo"] + get_unique_values(
        db,
        "repo", 
        station_filter=st.session_state.selected_station if st.session_state.selected_station != "Select Station" else None, 
        group_filter=st.session_state.selected_group if st.session_state.selected_group != "Select Group" else None,
        label_filter=st.session_state.selected_label if st.session_state.selected_label != "Select Label" else None
    )
    modules = ["Select Module"] + get_unique_values(
        db,
        "module", 
        station_filter=st.session_state.selected_station if st.session_state.selected_station != "Select Station" else None, 
        group_filter=st.session_state.selected_group if st.session_state.selected_group != "Select Group" else None,
        label_filter=st.session_state.selected_label if st.session_state.selected_label != "Select Label" else None
    )

    # Create custom CSS for filters to match the screenshot
    st.markdown(
        """
        <style>
        .filters-container {
            display: flex;
            justify-content: space-between;
            margin-bottom: 20px;
            gap: 10px;
        }
        </style>
        """, 
        unsafe_allow_html=True
    )
    
    # Create columns for filters
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        selected_station = st.selectbox(
            "Select Station", 
            stations, 
            index=stations.index(st.session_state.selected_station) if st.session_state.selected_station in stations else 0,
            key="station_select"
        )
        if selected_station != st.session_state.selected_station:
            st.session_state.selected_station = selected_station
            # Reset dependent filters
            st.session_state.selected_group = "Select Group"
            st.session_state.selected_label = "Select Label"
            st.session_state.selected_repo = "Select Repo"
            st.session_state.selected_module = "Select Module"
            # Mark filter as changed to trigger data reload
            st.session_state.filter_changed = True
            st.rerun()

    with col2:
        selected_group = st.selectbox(
            "Select Group", 
            groups, 
            index=groups.index(st.session_state.selected_group) if st.session_state.selected_group in groups else 0,
            key="group_select"
        )
        if selected_group != st.session_state.selected_group:
            st.session_state.selected_group = selected_group
            # Reset dependent filters
            st.session_state.selected_label = "Select Label"
            st.session_state.selected_repo = "Select Repo"
            st.session_state.selected_module = "Select Module"
            # Mark filter as changed to trigger data reload
            st.session_state.filter_changed = True
            st.rerun()

    with col3:
        selected_label = st.selectbox(
            "Select Label", 
            labels, 
            index=labels.index(st.session_state.selected_label) if st.session_state.selected_label in labels else 0,
            key="label_select"
        )
        if selected_label != st.session_state.selected_label:
            st.session_state.selected_label = selected_label
            # Reset dependent filters
            st.session_state.selected_repo = "Select Repo"
            st.session_state.selected_module = "Select Module"
            # Mark filter as changed to trigger data reload
            st.session_state.filter_changed = True
            st.rerun()

    with col4:
        selected_repo = st.selectbox(
            "Select Repo", 
            repos,
            index=repos.index(st.session_state.selected_repo) if st.session_state.selected_repo in repos else 0,
            key="repo_select"
        )
        if selected_repo != st.session_state.selected_repo:
            st.session_state.selected_repo = selected_repo
            # Reset dependent filter
            st.session_state.selected_module = "Select Module"
            # Mark filter as changed to trigger data reload
            st.session_state.filter_changed = True
            st.rerun()

    with col5:
        selected_module = st.selectbox(
            "Select Module", 
            modules,
            index=modules.index(st.session_state.selected_module) if st.session_state.selected_module in modules else 0,
            key="module_select"
        )
        if selected_module != st.session_state.selected_module:
            st.session_state.selected_module = selected_module
            # Mark filter as changed to trigger data reload
            st.session_state.filter_changed = True
            st.rerun()