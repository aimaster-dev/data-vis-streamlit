# components/date_filter.py
import streamlit as st
from datetime import datetime, timedelta
import pandas as pd

def render_date_filter():
    """
    Render a date range filter component with preset options and a calendar picker.
    
    Returns:
        tuple: (start_date, end_date) as datetime objects, both can be None if no date filter is applied
    """
    # Initialize date states if not already in session state
    if "start_date" not in st.session_state:
        st.session_state.start_date = None
    if "end_date" not in st.session_state:
        st.session_state.end_date = None
    if "date_range_selection" not in st.session_state:
        st.session_state.date_range_selection = "Select Date Range"
    
    # Create the date filter container with custom styling
    st.markdown("""
    <style>
    .date-filter-container {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 20px;
        border: 1px solid #ddd;
    }
    .date-filter-title {
        font-weight: bold;
        margin-bottom: 10px;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="date-filter-container">', unsafe_allow_html=True)
    
    # Create a two-column layout for the date filter
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown('<div class="date-filter-title">Date Range:</div>', unsafe_allow_html=True)
        
        # Date range selector (dropdown)
        date_range_options = [
            "Select Date Range",
            "Today", 
            "Yesterday",
            "Last 7 Days",
            "Last 30 Days",
            "This Month",
            "Last Month",
            "Custom Range"
        ]
        
        # Find the index of the current selection
        try:
            current_index = date_range_options.index(st.session_state.date_range_selection)
        except ValueError:
            current_index = 0  # Default to "Select Date Range" if not found
        
        selected_range = st.selectbox(
            "Select Date Range",
            date_range_options,
            index=current_index,
            label_visibility="collapsed"
        )
        
        # Process preset date range selections
        if selected_range != st.session_state.date_range_selection:
            st.session_state.date_range_selection = selected_range
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            if selected_range == "Select Date Range":
                # Clear date filters
                st.session_state.start_date = None
                st.session_state.end_date = None
                st.session_state.filter_changed = True
            
            elif selected_range == "Today":
                st.session_state.start_date = today
                st.session_state.end_date = today + timedelta(days=1) - timedelta(microseconds=1)
                st.session_state.filter_changed = True
            
            elif selected_range == "Yesterday":
                st.session_state.start_date = today - timedelta(days=1)
                st.session_state.end_date = today - timedelta(microseconds=1)
                st.session_state.filter_changed = True
            
            elif selected_range == "Last 7 Days":
                st.session_state.start_date = today - timedelta(days=7)
                st.session_state.end_date = today + timedelta(days=1) - timedelta(microseconds=1)
                st.session_state.filter_changed = True
            
            elif selected_range == "Last 30 Days":
                st.session_state.start_date = today - timedelta(days=30)
                st.session_state.end_date = today + timedelta(days=1) - timedelta(microseconds=1)
                st.session_state.filter_changed = True
            
            elif selected_range == "This Month":
                st.session_state.start_date = today.replace(day=1)
                next_month = today.replace(day=28) + timedelta(days=4)
                st.session_state.end_date = next_month.replace(day=1) - timedelta(microseconds=1)
                st.session_state.filter_changed = True
            
            elif selected_range == "Last Month":
                this_month_start = today.replace(day=1)
                last_month_end = this_month_start - timedelta(microseconds=1)
                last_month_start = last_month_end.replace(day=1)
                st.session_state.start_date = last_month_start
                st.session_state.end_date = last_month_end
                st.session_state.filter_changed = True
            
            # For "Custom Range", initialize with today if dates are None
            elif selected_range == "Custom Range" and (st.session_state.start_date is None or st.session_state.end_date is None):
                st.session_state.start_date = today
                st.session_state.end_date = today
            
            # Trigger a rerun to update the UI
            st.rerun()
        
    with col2:
        # Only show date pickers if "Custom Range" is selected
        if selected_range == "Custom Range":
            # Create columns for start and end date
            date_col1, date_col2 = st.columns(2)
            
            with date_col1:
                # Use today's date as the default if start_date is None
                default_start = st.session_state.start_date if st.session_state.start_date is not None else datetime.now()
                
                new_start_date = st.date_input(
                    "Start Date",
                    value=default_start,
                    key="start_date_picker"
                )
                if new_start_date != (st.session_state.start_date.date() if st.session_state.start_date else None):
                    st.session_state.start_date = datetime.combine(new_start_date, datetime.min.time())
            
            with date_col2:
                # Use today's date as the default if end_date is None
                default_end = st.session_state.end_date if st.session_state.end_date is not None else datetime.now()
                
                new_end_date = st.date_input(
                    "End Date",
                    value=default_end,
                    key="end_date_picker",
                    min_value=new_start_date  # Ensure end date is not before start date
                )
                if new_end_date != (st.session_state.end_date.date() if st.session_state.end_date else None):
                    # Set to end of the selected day
                    st.session_state.end_date = datetime.combine(new_end_date, datetime.max.time())
        
        elif st.session_state.start_date is not None and st.session_state.end_date is not None:
            # Show the selected date range as text
            start_str = st.session_state.start_date.strftime("%b %d, %Y")
            end_str = st.session_state.end_date.strftime("%b %d, %Y")
            st.markdown(f"**Selected Period:** {start_str} to {end_str}")
        else:
            # Show that no date filter is applied
            st.markdown("**No date filter applied**")
    
    # Add an Apply button for Custom Range
    if selected_range == "Custom Range":
        if st.button("Apply Date Range", key="apply_date_range"):
            # Ensure filter_changed is set to trigger data reload
            st.session_state.filter_changed = True
            st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    return st.session_state.start_date, st.session_state.end_date