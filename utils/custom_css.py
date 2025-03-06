# utils/custom_css.py
import streamlit as st

def apply_custom_css():
    """Apply custom CSS styles to the Streamlit app."""
    st.markdown(
        """
        <style>
        .metric-card {
            background-color: white;
            border-radius: 5px;
            padding: 20px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            text-align: center;
        }
        .metric-label {
            color: #666;
            font-size: 14px;
            margin-bottom: 10px;
        }
        .metric-value {
            color: #333;
            font-size: 36px;
            font-weight: bold;
        }
        [data-testid="stSidebar"] {
            background-color: #f5f7f9;
        }
        </style>
        """, 
        unsafe_allow_html=True
    )