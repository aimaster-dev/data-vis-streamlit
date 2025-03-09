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

        if df.empty:
            logger.warning("No data found in the specified range.")

        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)

        df['station'] = collection_name
        if 'dtime' in df.columns:
            df['dtime'] = pd.to_datetime(df['dtime'], errors='coerce')
            df["dtime"] = pd.to_datetime(df["dtime"].apply(lambda x: x["$date"] if isinstance(x, dict) else x))
            df["datetime"] = df["dtime"]  # This column contains full datetime information
        
        df["start_time"] = df["nonce"].apply(lambda x: x["pt"] if isinstance(x, dict) else None)
        df["end_time"] = df["nonce"].apply(lambda x: x["dt"] if isinstance(x, dict) else None)

        df["total_usage_time_hours"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 3600
        logger.info(f"Actual usage time1: {df['total_usage_time_hours'].to_string()}")

        # measurement_df = df["KEYD"].apply(lambda x: x["measurement_times"] if isinstance(x, dict) else 0)
        # df["actual_usage_time"] = measurement_df.apply(lambda x: x["real_test_time"] / 3600 if isinstance(x, dict) else 0)

        # logger.info(f"Actual usage time: {df['actual_usage_time'].to_string()}")

        df = df.sort_values(by="datetime")
        
        start_date = filters.get('start_date') if 'start_date' in filters else datetime.now() - timedelta(days=90)
        end_date = filters.get('end_date') if 'end_date' in filters else datetime.now()
        
        # Generate hourly date range
        all_hours = pd.date_range(start=start_date, end=end_date, freq='h')
        
        # Convert all_hours to string format '%Y-%m-%d-%H'
        all_hours_str = all_hours.strftime('%Y-%m-%d-%H').tolist()
        
        # Filter records between start_date and end_date
        df_filtered = df[(df['datetime'] >= start_date) & (df['datetime'] <= end_date)]
        


        # Define a function to count records per hour for a distinct field
        def count_records_per_hour(field):
            counts_per_hour = df_filtered.groupby([df_filtered['datetime'].dt.floor('h'), field]).size()
            counts_per_hour = counts_per_hour.reset_index(name='count')
            counts_per_hour['datetime'] = counts_per_hour['datetime'].dt.strftime('%Y-%m-%d-%H')
            return counts_per_hour
        
        # Count records per hour for each field
        module_counts_per_hour = count_records_per_hour("module")
        label_counts_per_hour = count_records_per_hour("_label")
        method_counts_per_hour = count_records_per_hour("method")
        repo_counts_per_hour = count_records_per_hour("repo")
        group_counts_per_hour = count_records_per_hour("_group")

        # Function to convert counts into the desired dictionary format
        def to_dict_format(counts_per_hour, field):
            # Get the unique values for the field (module, label, etc.)
            field_values = df_filtered[field].unique()
            
            # Create a dictionary with field values as keys and lists of counts as values
            result = {
                f"{field}_index": field_values.tolist()
            }
            
            for value in field_values:
                # Get the counts for each hour for this specific value of the field
                result[value] = [
                    counts_per_hour[(counts_per_hour["datetime"] == hour) & (counts_per_hour[field] == value)]["count"].values[0] 
                    if ((counts_per_hour["datetime"] == hour) & (counts_per_hour[field] == value)).any() 
                    else 0
                    for hour in all_hours_str
                ]
            return result

        log_counts_per_hour = df_filtered.groupby(df_filtered['datetime'].dt.floor('h'))["_id"].nunique()
        log_counts_per_hour.index = log_counts_per_hour.index.strftime('%Y-%m-%d-%H')
        log_counts_per_hour = log_counts_per_hour.reindex(all_hours_str, fill_value=0)

        # Convert each field's counts to the desired format
        module_counts_filled = to_dict_format(module_counts_per_hour, "module")
        label_counts_filled = to_dict_format(label_counts_per_hour, "_label")
        method_counts_filled = to_dict_format(method_counts_per_hour, "method")
        repo_counts_filled = to_dict_format(repo_counts_per_hour, "repo")
        group_counts_filled = to_dict_format(group_counts_per_hour, "_group")

        # Prepare the data dictionary
        data = {
            "dates": all_hours_str,
            "module_counts_per_hour": module_counts_filled,
            "label_counts_per_hour": label_counts_filled,
            "method_counts_per_hour": method_counts_filled,
            "repo_counts_per_hour": repo_counts_filled,
            "log_counts_per_hour": log_counts_per_hour.tolist(),
            "group_counts_per_hour": group_counts_filled
        }

        # Convert any pandas/numpy types to standard Python types for JSON serialization
        def convert_to_python_types(data_dict):
            for key, value in data_dict.items():
                if isinstance(value, list):
                    data_dict[key] = [int(x) if isinstance(x, (np.int64, np.float64)) else x for x in value]
                elif isinstance(value, dict):
                    data_dict[key] = convert_to_python_types(value)
            return data_dict

        data = convert_to_python_types(data)

        # Save the data to a JSON file
        client.close()
        logger.info(f"Data is successfully processed")
        file_path = f"measurement/{collection_name}_graph.json"
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        logger.info(f"Data has been successfully saved to {file_path}")

        return data
        
    except Exception as e:
        logger.error(f"Error in background data loading: {e}")
        return False

if __name__ == '__main__':
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "ADBFI", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "KAAPP2Q", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationAA", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationK", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationL", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationS", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    # process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "StationW", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})
    process_data_background("mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/", "equipment", "VSPartial", {"start_date": datetime.now() - timedelta(days=90), "end_date": datetime.now()})