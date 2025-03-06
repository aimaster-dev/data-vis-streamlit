import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def format_number(number):
    """Format number with K, M suffix as appropriate"""
    if number >= 1_000_000:
        return f"{number/1_000_000:.1f}M"
    elif number >= 1_000:
        return f"{number/1_000:.1f}K"
    return f"{number:.1f}"

def date_to_str(date):
    """Format date to string in a consistent format"""
    return date.strftime("%b %d, %Y")

def calculate_percentage_change(current, previous):
    """Calculate percentage change between current and previous values"""
    if previous == 0:
        return 100 if current > 0 else 0
    
    return ((current - previous) / previous) * 100

def get_trend_icon(change):
    """Return HTML for trend icon based on percentage change"""
    if change > 0:
        return f'<span style="color: green;">↑ {abs(change):.1f}%</span>'
    elif change < 0:
        return f'<span style="color: red;">↓ {abs(change):.1f}%</span>'
    else:
        return f'<span style="color: gray;">→ 0%</span>'