#!/usr/bin/env python3
# metrics_diagnostic.py - Check metrics in MongoDB
import pymongo
import pprint
import sys

# MongoDB connection details
CONNECTION_STRING = "mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/"
DATABASE_NAME = "equipment"
METRICS_COLLECTION = "equipment_metrics"

def diagnose_metrics():
    """Check metrics in MongoDB"""
    try:
        # Connect to MongoDB
        print(f"Connecting to MongoDB at {CONNECTION_STRING}...")
        client = pymongo.MongoClient(CONNECTION_STRING)
        db = client[DATABASE_NAME]
        
        # Check if metrics collection exists
        print(f"Checking if '{METRICS_COLLECTION}' collection exists...")
        collections = db.list_collection_names()
        if METRICS_COLLECTION not in collections:
            print(f"Collection '{METRICS_COLLECTION}' does not exist!")
            return
            
        print(f"Collection '{METRICS_COLLECTION}' exists.")
        
        # Check metrics count
        metrics_col = db[METRICS_COLLECTION]
        metrics_count = metrics_col.count_documents({})
        print(f"Found {metrics_count} documents in '{METRICS_COLLECTION}' collection.")
        
        # List all stations with metrics
        stations = metrics_col.distinct("station")
        print(f"Stations with metrics: {', '.join(stations)}")
        
        # Check metrics for each station
        for station in stations:
            # Get latest metrics for this station
            latest = metrics_col.find_one({"station": station}, sort=[("timestamp", -1)])
            
            if latest:
                print(f"\n=== Latest metrics for '{station}' (from {latest.get('timestamp')}) ===")
                
                # Print key metrics
                print(f"Record count: {latest.get('record_count', 'N/A')}")
                
                # Equipment Utilization Metrics
                print("\n1. Equipment Utilization Metrics")
                print(f"Utilization rate: {latest.get('utilization_rate', 'N/A')}")
                print(f"Downtime percentage: {latest.get('downtime_percentage', 'N/A')}")
                
                # Test Execution Metrics
                print("\n2. Test Execution Metrics")
                print(f"Tests per day: {latest.get('tests_per_day', 'N/A')}")
                print(f"Avg test duration: {latest.get('avg_test_duration_minutes', latest.get('avg_test_duration', 'N/A'))}")
                
                # Maintenance & Calibration Metrics
                print("\n3. Maintenance & Calibration Metrics")
                print(f"MTBF: {latest.get('mtbf_hours', latest.get('mtbf', 'N/A'))}")
                print(f"MTTR: {latest.get('mttr_hours', latest.get('mttr', 'N/A'))}")
                print(f"Calibration compliance: {latest.get('calibration_compliance', 'N/A')}")
                
                # Print all field names for reference
                print("\nAll available fields:")
                field_names = list(latest.keys())
                field_names.sort()
                print(", ".join(field_names))
            else:
                print(f"No metrics found for station '{station}'")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close connection
        client.close()
        print("\nConnection closed.")

def main():
    diagnose_metrics()
    return 0

if __name__ == "__main__":
    sys.exit(main())