#!/usr/bin/env python3
# continuous_metrics_calculator.py - Continuously monitor and calculate metrics
import time
import logging
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import gc
import signal
import sys
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("continuous_metrics.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ContinuousMetricsCalculator")

# MongoDB connection details
CONNECTION_STRING = "mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/"
DATABASE_NAME = "equipment"
METRICS_COLLECTION = "equipment_metrics"

# Global variables
stop_event = False
CHECK_INTERVAL = 300  # Check for new data every 5 minutes

class ContinuousMetricsCalculator:
    """Class to continuously calculate and store equipment metrics"""
    
    def __init__(self, connection_string, database_name, metrics_collection):
        """Initialize the continuous metrics calculator"""
        self.connection_string = connection_string
        self.database_name = database_name
        self.metrics_collection = metrics_collection
        self.client = None
        self.db = None
        self.station_collections = []
        self.last_processed = {}  # Track last processed timestamp for each station
        
    def connect(self):
        """Connect to MongoDB"""
        try:
            logger.info("Connecting to MongoDB...")
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            
            # Get list of station collections
            self.station_collections = self.db.list_collection_names()
            
            # Exclude the metrics collection and system collections
            self.station_collections = [col for col in self.station_collections 
                                       if col != self.metrics_collection and not col.startswith("system.")]
            
            logger.info(f"Connected to MongoDB. Found {len(self.station_collections)} station collections: {', '.join(self.station_collections)}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            return False

    def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client is not None:
            self.client.close()
            logger.info("Disconnected from MongoDB")
    
    def ensure_metrics_collection(self):
        """Ensure metrics collection exists with proper indexes"""
        try:
            # Create the metrics collection if it doesn't exist
            if self.metrics_collection not in self.db.list_collection_names():
                self.db.create_collection(self.metrics_collection)
                logger.info(f"Created metrics collection: {self.metrics_collection}")
            
            # Create indexes
            metrics_col = self.db[self.metrics_collection]
            metrics_col.create_index("station", unique=False)
            metrics_col.create_index("timestamp", unique=False)
            metrics_col.create_index([("station", 1), ("timestamp", 1)], unique=True)
            
            logger.info("Ensured metrics collection exists with proper indexes")
            return True
        except Exception as e:
            logger.error(f"Error ensuring metrics collection: {e}")
            return False
    
    def load_last_processed_timestamps(self):
        """Load the last processed timestamps from the metrics collection"""
        try:
            metrics_col = self.db[self.metrics_collection]
            
            # For each station, find the most recent metrics document
            for station in self.station_collections:
                latest = metrics_col.find_one(
                    {"station": station},
                    sort=[("timestamp", -1)]
                )
                
                if latest and "timestamp" in latest:
                    self.last_processed[station] = latest["timestamp"]
                    logger.info(f"Last processed timestamp for {station}: {latest['timestamp']}")
                else:
                    # If no metrics yet, use a timestamp from 30 days ago
                    self.last_processed[station] = datetime.now() - timedelta(days=30)
                    logger.info(f"No previous metrics for {station}, starting from 30 days ago")
            
            return True
        except Exception as e:
            logger.error(f"Error loading last processed timestamps: {e}")
            return False
    
    def detect_failure_patterns(self, collection, since_timestamp=None):
        """
        Detect patterns in data that might indicate failures
        
        Args:
            collection: MongoDB collection
            since_timestamp: Only look at documents since this timestamp
            
        Returns:
            int: Estimated number of failures
        """
        try:
            # Build query to only look at new documents
            query = {}
            if since_timestamp:
                query["dtime"] = {"$gt": since_timestamp}
            
            # Check if any documents match the query
            doc_count = collection.count_documents(query)
            if doc_count == 0:
                return 0
            
            # Look for known error patterns in method names, labels, or any other text field
            failure_count = 0
            
            # Check method field for failure indicators
            if collection.count_documents({"method": {"$exists": True}, **query}) > 0:
                failure_methods = collection.count_documents({
                    "method": {"$regex": "failure|error|exception|crash|fix|repair", "$options": "i"},
                    **query
                })
                failure_count += failure_methods
                
            # Check _label field for failure indicators
            if collection.count_documents({"_label": {"$exists": True}, **query}) > 0:
                failure_labels = collection.count_documents({
                    "_label": {"$regex": "failure|error|exception|crash|fix|repair", "$options": "i"},
                    **query
                })
                failure_count += failure_labels
            
            # Look for anomalously low 'count' values which might indicate failures
            if collection.count_documents({"count": {"$exists": True}, **query}) > 0:
                # Get average count
                avg_count_results = collection.aggregate([
                    {"$match": {"count": {"$exists": True}, **query}},
                    {"$group": {"_id": None, "avg": {"$avg": "$count"}}}
                ])
                
                avg_count_list = list(avg_count_results)
                if avg_count_list:
                    avg_count = avg_count_list[0]["avg"]
                    # Count documents with count less than 10% of average as potential failures
                    low_count_failures = collection.count_documents({
                        "count": {"$lt": avg_count * 0.1, "$gt": 0},
                        **query
                    })
                    failure_count += low_count_failures
            
            return failure_count
        except Exception as e:
            logger.error(f"Error detecting failure patterns: {e}")
            return 0
    
    def analyze_new_data(self, station, since_timestamp=None):
        """
        Analyze new data for a station since the last processed timestamp
        
        Args:
            station: Station name
            since_timestamp: Only analyze data since this timestamp
            
        Returns:
            dict: Dictionary of stats for the station
        """
        try:
            logger.info(f"Analyzing new data for station: {station}")
            collection = self.db[station]
            
            # Build query to only look at new documents
            query = {}
            if since_timestamp:
                query["dtime"] = {"$gt": since_timestamp}
            
            # Count new documents
            new_docs = collection.count_documents(query)
            logger.info(f"Found {new_docs} new documents for {station} since {since_timestamp}")
            
            if new_docs == 0:
                logger.info(f"No new data for {station}, skipping analysis")
                return None
            
            # Get total document count
            total_docs = collection.count_documents({})
            logger.info(f"Collection {station} has {total_docs} total documents")
            
            # Sample documents to determine available fields
            sample_docs = list(collection.find().limit(10))
            available_fields = set()
            for doc in sample_docs:
                available_fields.update(doc.keys())
            
            logger.info(f"Available fields in {station}: {', '.join(available_fields)}")
            
            # Initialize stats dictionary
            stats = {
                'total_docs': total_docs,
                'record_count': total_docs,
                'new_docs': new_docs
            }
            
            # 1. Time-based analysis using dtime field
            if 'dtime' in available_fields:
                # Calculate time range (min/max dtime)
                pipeline = [
                    {"$match": {"dtime": {"$exists": True}}},
                    {"$group": {
                        "_id": None,
                        "min_date": {"$min": "$dtime"},
                        "max_date": {"$max": "$dtime"}
                    }}
                ]
                
                time_range = list(collection.aggregate(pipeline))
                
                if time_range and len(time_range) > 0:
                    try:
                        min_date = time_range[0]['min_date']
                        max_date = time_range[0]['max_date']
                        
                        # Convert to datetime if they are strings
                        if isinstance(min_date, str):
                            min_date = datetime.fromisoformat(min_date.replace('Z', '+00:00'))
                        if isinstance(max_date, str):
                            max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))
                        
                        # Calculate time range
                        time_diff = max_date - min_date
                        time_diff_hours = time_diff.total_seconds() / 3600
                        
                        stats['time_range_hours'] = time_diff_hours
                        stats['min_date'] = min_date
                        stats['max_date'] = max_date
                        
                        # Calculate days for test per day metrics
                        days = max(1, time_diff.days)
                        stats['days'] = days
                        stats['tests_per_day'] = total_docs / days
                        
                        logger.info(f"Time range for {station}: {time_diff_hours:.1f} hours, {days} days")
                        
                        # 1.1. Estimate "available time" as the full time range in hours
                        stats['available_hours'] = time_diff_hours
                        
                        # 1.2. Estimate "actual usage time" based on test counts and average duration
                        # Assuming each test takes a reasonable amount of time (e.g., 15 minutes)
                        estimated_test_duration_hours = 0.25  # 15 minutes per test
                        stats['estimated_usage_hours'] = total_docs * estimated_test_duration_hours
                        
                        # Cap usage at available hours
                        stats['estimated_usage_hours'] = min(stats['estimated_usage_hours'], stats['available_hours'])
                        
                        # 1.3. Calculate downtime (assuming gaps between tests are downtime)
                        stats['estimated_downtime_hours'] = stats['available_hours'] - stats['estimated_usage_hours']
                    except Exception as e:
                        logger.error(f"Error calculating time range: {e}")
            
            # 2. Test execution metrics
            if 'count' in available_fields:
                # Count statistics
                pipeline = [
                    {"$match": {"count": {"$exists": True}}},
                    {"$group": {
                        "_id": None,
                        "avg_count": {"$avg": "$count"},
                        "max_count": {"$max": "$count"},
                        "min_count": {"$min": "$count"},
                        "sum_count": {"$sum": "$count"}
                    }}
                ]
                
                count_stats = list(collection.aggregate(pipeline))
                
                if count_stats and len(count_stats) > 0:
                    stats['avg_count'] = count_stats[0]['avg_count']
                    stats['max_count'] = count_stats[0]['max_count']
                    stats['min_count'] = count_stats[0]['min_count']
                    stats['sum_count'] = count_stats[0]['sum_count']
                    
                    logger.info(f"Count statistics for {station}: avg={stats['avg_count']:.1f}, total={stats['sum_count']}")
                    
                    # Estimate test duration based on count (higher count = longer duration)
                    # This is a rough estimate - adjust as needed
                    base_duration_minutes = 5  # minimum test duration
                    count_factor = 0.001  # scaling factor
                    estimated_duration = base_duration_minutes + (stats['avg_count'] * count_factor)
                    stats['estimated_test_duration_minutes'] = estimated_duration
                    
                    # Calculate total test time
                    stats['estimated_total_test_time_hours'] = (stats['estimated_test_duration_minutes'] * total_docs) / 60
            
            # 3. Line statistics - might relate to code or test complexity
            if 'line' in available_fields:
                pipeline = [
                    {"$match": {"line": {"$exists": True}}},
                    {"$group": {
                        "_id": None,
                        "avg_line": {"$avg": "$line"},
                        "max_line": {"$max": "$line"},
                        "min_line": {"$min": "$line"}
                    }}
                ]
                
                line_stats = list(collection.aggregate(pipeline))
                
                if line_stats and len(line_stats) > 0:
                    stats['avg_line'] = line_stats[0]['avg_line']
                    stats['max_line'] = line_stats[0]['max_line']
                    stats['min_line'] = line_stats[0]['min_line']
                    
                    logger.info(f"Line statistics for {station}: avg={stats['avg_line']:.1f}")
            
            # 4. Categorization metrics
            for field in ['_group', '_label', 'repo', 'module', 'method']:
                if field in available_fields:
                    distinct_count = len(collection.distinct(field))
                    stats[f'{field}_count'] = distinct_count
                    logger.info(f"Found {distinct_count} unique {field}s in {station}")
            
            # 5. Detect failures and estimate MTBF
            # Only look at failures since the last processed timestamp
            failure_count = self.detect_failure_patterns(collection, since_timestamp)
            stats['failures_since_last'] = failure_count
            
            # Get total failures (for MTBF calculation)
            total_failures = self.detect_failure_patterns(collection)
            stats['estimated_failures'] = total_failures
            
            if total_failures > 0 and 'estimated_usage_hours' in stats:
                stats['estimated_mtbf'] = stats['estimated_usage_hours'] / total_failures
                logger.info(f"Estimated {total_failures} failures, MTBF: {stats['estimated_mtbf']:.1f} hours")
            
            logger.info(f"Completed analysis for station {station}")
            return stats
            
        except Exception as e:
            logger.error(f"Error analyzing data for station {station}: {e}")
            return None
    
    def calculate_metrics(self, station, stats):
        """
        Calculate standardized equipment metrics based on analyzed data
        
        Args:
            station: Station name
            stats: Dictionary of statistics from analyze_station_data
            
        Returns:
            dict: Dictionary of metrics
        """
        try:
            if not stats:
                logger.warning(f"No stats available for station {station}")
                return {}
            
            logger.info(f"Calculating metrics for station: {station}")
            
            metrics = {
                "station": station,
                "timestamp": datetime.now(),
                "record_count": stats.get('total_docs', 0),
                "new_records_since_last": stats.get('new_docs', 0)
            }
            
            # 1. Equipment Utilization Metrics
            # Utilization Rate (%) = (Actual Usage Time / Total Available Time) × 100
            if 'estimated_usage_hours' in stats and 'available_hours' in stats and stats['available_hours'] > 0:
                metrics['utilization_rate'] = (stats['estimated_usage_hours'] / stats['available_hours']) * 100
                metrics['utilization_rate'] = min(100, metrics['utilization_rate'])  # Cap at 100%
            
            # Downtime (%) = (Downtime Hours / Total Available Hours) × 100
            if 'estimated_downtime_hours' in stats and 'available_hours' in stats and stats['available_hours'] > 0:
                metrics['downtime_percentage'] = (stats['estimated_downtime_hours'] / stats['available_hours']) * 100
                metrics['downtime_percentage'] = min(100, metrics['downtime_percentage'])  # Cap at 100%
            
            # 2. Test Execution Metrics
            # Tests Per Equipment Per Day
            if 'tests_per_day' in stats:
                metrics['tests_per_day'] = stats['tests_per_day']
            
            # Average Test Duration
            if 'estimated_test_duration_minutes' in stats:
                metrics['avg_test_duration_minutes'] = stats['estimated_test_duration_minutes']
            
            # 3. Maintenance & Calibration Metrics
            # Mean Time Between Failures (MTBF)
            if 'estimated_mtbf' in stats:
                metrics['mtbf_hours'] = stats['estimated_mtbf']
            
            # We don't have direct data for MTTR and Calibration Compliance
            # So we'll use reasonable default values
            metrics['mttr_hours'] = 4.0  # Default value
            metrics['calibration_compliance'] = 95.0  # Default value
            
            # 4. Cost & Efficiency Metrics
            # We don't have direct cost data, so we'll use estimates
            
            # Cost Per Test (estimate based on test complexity)
            if 'avg_count' in stats:
                # Higher count = more complex test = higher cost
                base_cost = 10  # Base cost per test in dollars
                complexity_factor = 0.0001  # Scaling factor
                metrics['estimated_cost_per_test'] = base_cost + (stats['avg_count'] * complexity_factor)
            else:
                metrics['estimated_cost_per_test'] = 12.75  # Default value
            
            # Energy Consumption Per Test (estimate)
            metrics['estimated_energy_per_test_kwh'] = 2.4  # Default value
            
            # Equipment Depreciation Rate (standard value)
            metrics['equipment_depreciation_rate'] = 15.0  # Default value
            
            # 5. Availability & Scheduling Metrics
            # Equipment Availability (%) = ((Total Available Hours - Downtime Hours) / Total Available Hours) × 100
            if 'available_hours' in stats and 'estimated_downtime_hours' in stats and stats['available_hours'] > 0:
                metrics['equipment_availability'] = ((stats['available_hours'] - stats['estimated_downtime_hours']) / stats['available_hours']) * 100
                metrics['equipment_availability'] = min(100, metrics['equipment_availability'])  # Cap at 100%
            
            # Booking vs. Usage Discrepancy (no direct data, use default)
            metrics['booking_discrepancy'] = 15.0  # Default value
            
            # 6. Additional Metrics from Data Analysis
            
            # Test Complexity (based on line and count values)
            if 'avg_line' in stats and 'avg_count' in stats:
                # Normalize and combine line and count metrics
                metrics['test_complexity_score'] = (stats['avg_line'] / 100) + (stats['avg_count'] / 1000)
            
            # Field counts for reporting
            for field in ['_group_count', '_label_count', 'repo_count', 'module_count', 'method_count']:
                if field in stats:
                    metrics[field] = stats[field]
            
            logger.info(f"Metrics calculation completed for station {station}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating metrics for station {station}: {e}")
            return {}
    
    def store_metrics(self, metrics):
        """Store calculated metrics in MongoDB"""
        try:
            if not metrics:
                logger.warning("No metrics to store")
                return False
            
            station = metrics.get('station')
            if not station:
                logger.error("No station specified in metrics")
                return False
                
            logger.info(f"Storing metrics for station: {station}")
            
            # Get metrics collection
            metrics_col = self.db[self.metrics_collection]
            
            # Use upsert to update or insert
            result = metrics_col.update_one(
                {"station": station, "timestamp": metrics["timestamp"]},
                {"$set": metrics},
                upsert=True
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated metrics for station {station}")
            elif result.upserted_id:
                logger.info(f"Inserted new metrics for station {station}")
            else:
                logger.info(f"No changes for station {station}")
            
            # Update last processed timestamp
            self.last_processed[station] = metrics["timestamp"]
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing metrics: {e}")
            return False
    
    def process_station(self, station):
        """
        Process a single station - analyze data, calculate metrics, store results
        
        Args:
            station: Station name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Processing station: {station}")
            
            # Get last processed timestamp for this station
            since_timestamp = self.last_processed.get(station)
            
            # Analyze new data
            stats = self.analyze_new_data(station, since_timestamp)
            
            # If no stats or no new data, skip
            if not stats:
                logger.warning(f"No new data for station {station}, skipping")
                return False
            
            # If no new documents since last time, skip
            if stats.get('new_docs', 0) == 0:
                logger.info(f"No new documents for station {station} since last processing, skipping")
                return False
            
            # Calculate metrics
            metrics = self.calculate_metrics(station, stats)
            
            # Store metrics
            result = self.store_metrics(metrics)
            
            # Clean up
            del stats, metrics
            gc.collect()
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing station {station}: {e}")
            return False
    
    def process_all_stations(self):
        """Process all stations to calculate and store metrics"""
        try:
            logger.info("Starting metrics calculation for all stations")
            
            # Ensure connection
            if self.client is None or self.db is None:
                if not self.connect():
                    return False
            
            # Ensure metrics collection exists
            self.ensure_metrics_collection()
            
            # Load last processed timestamps
            self.load_last_processed_timestamps()
            
            # Process each station
            success_count = 0
            for station in self.station_collections:
                if stop_event:
                    logger.info("Stop event detected, aborting processing")
                    break
                    
                success = self.process_station(station)
                if success:
                    success_count += 1
            
            logger.info(f"Completed metrics calculation for {success_count}/{len(self.station_collections)} stations")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error in processing all stations: {e}")
            return False
    
    def run_continuous_monitoring(self):
        """Run continuous monitoring of stations"""
        try:
            logger.info("Starting continuous metrics monitoring")
            
            # Ensure connection
            if self.client is None or self.db is None:
                if not self.connect():
                    return False
            
            # Ensure metrics collection exists
            self.ensure_metrics_collection()
            
            # Load last processed timestamps
            self.load_last_processed_timestamps()
            
            # Main monitoring loop
            while not stop_event:
                logger.info("Checking for new data...")
                
                # Process all stations
                self.process_all_stations()
                
                # Sleep until next check
                logger.info(f"Sleeping for {CHECK_INTERVAL} seconds until next check")
                for _ in range(CHECK_INTERVAL):
                    if stop_event:
                        break
                    time.sleep(1)
            
            logger.info("Continuous monitoring stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {e}")
            return False

def signal_handler(sig, frame):
    """Handle termination signals"""
    global stop_event
    logger.info(f"Received signal {sig}, shutting down...")
    stop_event = True

def main():
    """Main function to run the continuous metrics calculator"""
    try:
        logger.info("Starting Continuous Metrics Calculator Service")
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Initialize the calculator
        calculator = ContinuousMetricsCalculator(CONNECTION_STRING, DATABASE_NAME, METRICS_COLLECTION)
        
        # Run continuous monitoring
        calculator.run_continuous_monitoring()
        
        # Disconnect when done
        calculator.disconnect()
        
        logger.info("Continuous Metrics Calculator Service stopped")
        
    except Exception as e:
        logger.error(f"Error in Continuous Metrics Calculator Service: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())