# utils/data_model.py
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
import random

def generate_equipment_metrics_fields(db, collection_names=None):
    """
    Update existing documents with equipment metrics fields
    
    Args:
        db: MongoDB database connection
        collection_names: Optional list of collection names to update (defaults to all)
    """
    if collection_names is None:
        collection_names = db.list_collection_names()
    
    # Track number of documents updated
    total_updated = 0
    
    for collection_name in collection_names:
        collection = db[collection_name]
        
        # Get count of documents in collection
        doc_count = collection.count_documents({})
        print(f"Collection {collection_name} has {doc_count} documents")
        
        if doc_count == 0:
            continue
        
        # Process documents in batches
        batch_size = 1000
        updates = []
        
        # Find documents without metrics fields
        query = {
            "$or": [
                {"usage_time": {"$exists": False}},
                {"available_time": {"$exists": False}},
                {"downtime_hours": {"$exists": False}},
                {"test_duration": {"$exists": False}}
            ]
        }
        
        for doc in collection.find(query).limit(5000):  # Limit to avoid processing too many
            # Generate test duration (3-30 minutes)
            test_duration = random.uniform(3, 30)
            
            # Generate usage and available time
            # Assuming dtime field exists, if not use current time
            if 'dtime' in doc:
                doc_time = doc['dtime']
                # Generate time window (24 hour period around test time)
                day_start = doc_time.replace(hour=0, minute=0, second=0, microsecond=0)
                next_day = day_start + timedelta(days=1)
                
                # Available time is 24 hours for the equipment
                available_time = 24.0
                
                # Usage time is a percentage of available time (30-90%)
                utilization_rate = random.uniform(30, 90)
                usage_time = available_time * (utilization_rate / 100)
                
                # Downtime is a percentage of available time (0-15%)
                downtime_rate = random.uniform(0, 15)
                downtime_hours = available_time * (downtime_rate / 100)
            else:
                # Default values if no time information
                available_time = 24.0
                usage_time = 18.0  # 75% utilization
                downtime_hours = 2.0  # 8.3% downtime
            
            # Create update operation
            update = UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {
                    "test_duration": test_duration,
                    "available_time": available_time,
                    "usage_time": usage_time,
                    "downtime_hours": downtime_hours,
                    "available_hours": available_time,  # Duplicate but keeps naming consistent with metrics
                }}
            )
            
            updates.append(update)
            
            # Execute batch update when we reach batch size
            if len(updates) >= batch_size:
                result = collection.bulk_write(updates)
                total_updated += result.modified_count
                print(f"Updated {result.modified_count} documents in {collection_name}")
                updates = []
        
        # Execute any remaining updates
        if updates:
            result = collection.bulk_write(updates)
            total_updated += result.modified_count
            print(f"Updated {result.modified_count} documents in {collection_name}")
    
    print(f"Total documents updated: {total_updated}")
    return total_updated

def create_equipment_utilization_index(db):
    """
    Create indexes to optimize equipment utilization queries
    
    Args:
        db: MongoDB database connection
    """
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        
        # Create indexes for equipment metrics
        collection.create_index("usage_time")
        collection.create_index("available_time")
        collection.create_index("downtime_hours")
        collection.create_index("test_duration")
        
        # Create compound indexes for common queries
        collection.create_index([("dtime", 1), ("test_duration", 1)])
        collection.create_index([("_group", 1), ("usage_time", 1)])
        
        print(f"Created indexes for collection {collection_name}")

def run_data_model_update():
    """
    Run a complete data model update - connect to database and update all collections
    """
    try:
        client = MongoClient("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/")
        db = client["equipment"]
        
        # Update documents with equipment metrics fields
        updated_count = generate_equipment_metrics_fields(db)
        print(f"Updated {updated_count} documents with equipment metrics fields")
        
        # Create indexes for equipment utilization queries
        create_equipment_utilization_index(db)
        print("Created indexes for equipment utilization queries")
        
        return True, f"Successfully updated {updated_count} documents"
    except Exception as e:
        print(f"Error updating data model: {str(e)}")
        return False, str(e)

if __name__ == "__main__":
    success, message = run_data_model_update()
    print(f"Data model update {'succeeded' if success else 'failed'}: {message}")