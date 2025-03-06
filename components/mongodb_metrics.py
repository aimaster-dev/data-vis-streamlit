# components/mongodb_metrics.py
import streamlit as st
import pandas as pd
import logging
import pymongo
from datetime import datetime, timedelta
import gc

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBMetrics:
    """Class to retrieve pre-calculated metrics from MongoDB"""
    
    def __init__(self, connection_string, database_name, metrics_collection="equipment_metrics"):
        """Initialize the MongoDB metrics connector"""
        self.connection_string = connection_string
        self.database_name = database_name
        self.metrics_collection = metrics_collection
        self.client = None
        self.db = None
    
    def connect(self):
        """Connect to MongoDB"""
        try:
            if not self.client:
                self.client = pymongo.MongoClient(self.connection_string)
                self.db = self.client[self.database_name]
                return True
            return True
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
    
    def get_latest_metrics(self, station=None, max_age_minutes=60):
        """
        Get the latest metrics for the specified station
        
        Args:
            station: Station name (optional, if None returns metrics for all stations)
            max_age_minutes: Maximum age of metrics in minutes (default: 60)
            
        Returns:
            dict: Dictionary of metrics or None if no metrics found
        """
        try:
            if not self.connect():
                return None
            
            # Get metrics collection
            metrics_col = self.db[self.metrics_collection]
            
            # Calculate minimum timestamp
            min_timestamp = datetime.now() - timedelta(minutes=max_age_minutes)
            
            # Build query
            query = {"timestamp": {"$gte": min_timestamp}}
            if station:
                query["station"] = station
            
            # Get latest metrics
            if station:
                # For specific station, get the most recent document
                metrics = metrics_col.find_one(
                    query,
                    sort=[("timestamp", pymongo.DESCENDING)]
                )
                
                return metrics
            else:
                # For all stations, get the most recent document for each station
                pipeline = [
                    {"$match": query},
                    {"$sort": {"timestamp": -1}},
                    {"$group": {
                        "_id": "$station",
                        "doc": {"$first": "$$ROOT"}
                    }},
                    {"$replaceRoot": {"newRoot": "$doc"}}
                ]
                
                cursor = metrics_col.aggregate(pipeline)
                all_metrics = list(cursor)
                
                return all_metrics
                
        except Exception as e:
            logger.error(f"Error getting latest metrics: {e}")
            return None
        finally:
            # Force garbage collection
            gc.collect()
    
    def get_metrics_history(self, station, metric_name, days=7):
        """
        Get historical metrics for a specific station and metric
        
        Args:
            station: Station name
            metric_name: Name of the metric to retrieve
            days: Number of days of history to retrieve
            
        Returns:
            pandas.DataFrame: DataFrame with timestamp and metric value
        """
        try:
            if not self.connect():
                return pd.DataFrame()
            
            # Get metrics collection
            metrics_col = self.db[self.metrics_collection]
            
            # Calculate minimum timestamp
            min_timestamp = datetime.now() - timedelta(days=days)
            
            # Build query
            query = {
                "station": station,
                "timestamp": {"$gte": min_timestamp},
                metric_name: {"$exists": True}
            }
            
            # Get metrics
            cursor = metrics_col.find(
                query,
                {"timestamp": 1, metric_name: 1, "_id": 0}
            ).sort("timestamp", pymongo.ASCENDING)
            
            # Convert to DataFrame
            df = pd.DataFrame(list(cursor))
            
            if df.empty:
                return pd.DataFrame()
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting metrics history: {e}")
            return pd.DataFrame()
        finally:
            # Force garbage collection
            gc.collect()

def get_mongodb_metrics_singleton():
    """Get or create a MongoDBMetrics instance (singleton pattern)"""
    if "mongodb_metrics" not in st.session_state:
        # MongoDB connection details
        connection_string = "mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/"
        database_name = "equipment"
        metrics_collection = "equipment_metrics"
        
        # Create instance
        st.session_state.mongodb_metrics = MongoDBMetrics(
            connection_string,
            database_name,
            metrics_collection
        )
    
    return st.session_state.mongodb_metrics

def get_equipment_metrics(station=None):
    """
    Get equipment metrics from MongoDB
    
    Args:
        station: Station name (optional)
        
    Returns:
        dict: Dictionary of metrics results
    """
    try:
        # Get MongoDB metrics connector
        mongodb_metrics = get_mongodb_metrics_singleton()
        
        # Get latest metrics
        metrics = mongodb_metrics.get_latest_metrics(station)
        
        if not metrics:
            # No metrics found
            return {}
        
        # If we got a list (all stations), convert to dictionary keyed by station
        if isinstance(metrics, list):
            # Process metrics for different visualizations
            metrics_results = {}
            
            # Extract utilization rate
            utilization_rates = {}
            for m in metrics:
                station_name = m.get("station")
                if station_name and "utilization_rate" in m:
                    utilization_rates[station_name] = m["utilization_rate"]
            
            if utilization_rates:
                metrics_results["utilization_rate"] = pd.Series(utilization_rates)
            
            # Extract downtime percentage
            downtime_percentages = {}
            for m in metrics:
                station_name = m.get("station")
                if station_name and "downtime_percentage" in m:
                    downtime_percentages[station_name] = m["downtime_percentage"]
            
            if downtime_percentages:
                metrics_results["downtime_percentage"] = pd.Series(downtime_percentages)
            
            # Add scalar metrics (averages across stations)
            scalar_metrics = [
                "mtbf", "mttr", "calibration_compliance", "cost_per_test",
                "energy_consumption", "depreciation_rate", "booking_discrepancy",
                "tests_per_day", "avg_test_duration"
            ]
            
            for metric in scalar_metrics:
                values = [m.get(metric, 0) for m in metrics if metric in m]
                if values:
                    metrics_results[metric] = sum(values) / len(values)
            
            return metrics_results
        else:
            # Single station metrics
            metrics_results = {}
            
            # Handle utilization_rate
            if "utilization_rate" in metrics:
                metrics_results["utilization_rate"] = pd.Series({metrics["station"]: metrics["utilization_rate"]})
            
            # Handle downtime_percentage
            if "downtime_percentage" in metrics:
                metrics_results["downtime_percentage"] = pd.Series({metrics["station"]: metrics["downtime_percentage"]})
            
            # Add scalar metrics
            scalar_metrics = [
                "mtbf", "mttr", "calibration_compliance", "cost_per_test",
                "energy_consumption", "depreciation_rate", "booking_discrepancy",
                "tests_per_day", "avg_test_duration"
            ]
            
            for metric in scalar_metrics:
                if metric in metrics:
                    metrics_results[metric] = metrics[metric]
            
            return metrics_results
        
    except Exception as e:
        logger.error(f"Error getting equipment metrics: {e}")
        return {}

def render_mongodb_equipment_metrics(df, station=None):
    """
    Render equipment metrics from MongoDB
    
    Args:
        df: DataFrame with raw data (used for fallback and context)
        station: Station name (optional)
    """
    try:
        # Get metrics from MongoDB
        metrics_results = get_equipment_metrics(station)
        
        # Render metrics with the existing function
        from components.equipment_metrics import render_equipment_metrics
        render_equipment_metrics(df, metrics_results)
        
    except Exception as e:
        logger.error(f"Error rendering MongoDB equipment metrics: {e}")
        # Fall back to the original function
        from components.equipment_metrics import render_equipment_metrics
        render_equipment_metrics(df, None)