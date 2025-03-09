import logging
import pymongo
import pandas as pd
from datetime import datetime, timedelta
import json
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_data_background(connection_string, database_name, collection_name, filters=None):
    try:
        logger.info("Starting background data loading....")
        
        client = pymongo.MongoClient(
            connection_string,
            serverSelectionTimeoutMS=100000,  # Increase timeout to 30 seconds
            connectTimeoutMS=100000,          # Increase connection timeout to 30 seconds
            socketTimeoutMS=100000            # Increase socket timeout to 30 seconds
        )
        db = client[database_name]
        collection = db[collection_name]
        
        # Build query
        query = {}
        if filters:
            if filters.get('start_date') or filters.get('end_date'):
                date_query = {}
                if filters.get('start_date'):
                    date_query['$gte'] = filters['start_date']
                if filters.get('end_date'):
                    date_query['$lte'] = filters['end_date']
                if date_query:
                    query['dtime'] = date_query
        
        cursor = collection.find(query)
        data = list(cursor)
        df = pd.DataFrame(data)
        
        return data
        
    except Exception as e:
        logger.error(f"Error in background data loading: {e}")
        return False

if __name__ == '__main__':
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "ADBFI", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "KAAPP2Q", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationAA", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationK", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationL", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationS", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationW", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "VSPartial", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
