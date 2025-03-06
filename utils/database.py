# utils/database.py - Optimized for streaming data
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time
from datetime import datetime
import gc  # Garbage collector
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@st.cache_resource
def init_connection():
    """Initialize MongoDB connection with lower server timeouts"""
    return MongoClient(
        "mongodb+srv://piyushaaryan:dW2iDwGyu6GIIn5t@visdb.ukrkn.mongodb.net/",
        # Add connection pool settings to reduce memory
        maxPoolSize=1,
        minPoolSize=0,
        maxIdleTimeMS=5000,
        # Set lower network timeouts
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
    )

@st.cache_data(ttl=3600)  # Cache for 1 hour to reduce repeated calls
def get_collections(_db):
    """Get collection names with limit to reduce memory usage"""
    # Get at most 10 collections - add more as needed
    return _db.list_collection_names()[:10]

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_unique_values(_db, field, station_filter=None, group_filter=None, label_filter=None):
    """Get unique values with memory optimizations"""
    try:
        unique_values = set()
        
        # Limit collections to query
        if station_filter and station_filter != "Select Station":
            collections_to_query = [station_filter]
        else:
            # Only use the first 3 collections when not filtered to save memory
            collections_to_query = get_collections(_db)[:3]
        
        for collection_name in collections_to_query:
            if collection_name in get_collections(_db):
                collection = _db[collection_name]
                
                # Build query filter
                query = {}
                if group_filter and group_filter != "Select Group":
                    query["_group"] = group_filter
                if label_filter and label_filter != "Select Label":
                    query["_label"] = label_filter
                
                # Use distinct with aggressive limits
                if not query:
                    # Use projection and limit to reduce memory usage
                    projection = {field: 1, "_id": 0}
                    cursor = collection.find(
                        {field: {"$exists": True, "$ne": None}}, 
                        projection
                    ).limit(100)  # Only get 100 values max
                    
                    # Process cursor in chunks to avoid memory spikes
                    chunk_size = 20
                    current_chunk = []
                    
                    for i, doc in enumerate(cursor):
                        if field in doc:
                            current_chunk.append(doc[field])
                        
                        # Process in small chunks
                        if len(current_chunk) >= chunk_size:
                            unique_values.update(current_chunk)
                            current_chunk = []
                            # Force garbage collection after each chunk
                            gc.collect()
                    
                    # Add any remaining items
                    if current_chunk:
                        unique_values.update(current_chunk)
                else:
                    # With filters, use distinct but limit results
                    distinct_values = collection.distinct(field, query)
                    # Only take first 100 values
                    unique_values.update(distinct_values[:100])
        
        # Limit total results
        result = sorted([val for val in unique_values if val])[:100]
        return result
    except Exception as e:
        logger.error(f"Error fetching unique values: {str(e)}")
        return []

def get_filtered_data_chunk(_db, skip=0, limit=100, station=None, group=None, label=None, repo=None, module=None, start_date=None, end_date=None):
    """Get a small chunk of filtered data with pagination"""
    try:
        data = []
        
        # Use only specific collections to reduce memory
        if station and station != "Select Station":
            collections_to_query = [station]
        else:
            # Limit to max 2 collections when not filtered
            collections_to_query = get_collections(_db)[:2]
        
        # Track how many documents we've processed across collections
        total_processed = 0
        
        for collection_name in collections_to_query:
            if collection_name in get_collections(_db):
                collection = _db[collection_name]
                
                # Create minimal query to reduce memory
                query = {}
                
                # Only add filters that are actually specified
                if group and group != "Select Group":
                    query["_group"] = group
                if label and label != "Select Label":
                    query["_label"] = label
                if repo and repo != "Select Repo":
                    query["repo"] = repo
                if module and module != "Select Module":
                    query["module"] = module
                
                # Add date filtering if specified
                if start_date is not None or end_date is not None:
                    date_query = {}
                    if start_date is not None:
                        date_query["$gte"] = start_date
                    if end_date is not None:
                        date_query["$lte"] = end_date
                    
                    if date_query:
                        query["dtime"] = date_query
                
                # Calculate how many to skip in this collection
                collection_skip = max(0, skip - total_processed)
                
                # Calculate how many to take from this collection
                collection_limit = min(limit - len(data), 100)  # Never get more than 100 per collection
                
                if collection_limit <= 0:
                    break  # We already have enough data
                
                # Use projection to only get fields we need
                projection = {
                    "_id": 1,
                    "dtime": 1,
                    "_group": 1,
                    "_label": 1,
                    "repo": 1,
                    "module": 1,
                    "test_duration": 1,
                    "usage_time": 1,
                    "available_time": 1,
                    "downtime_hours": 1,
                    "available_hours": 1
                }
                
                # Get data with pagination
                cursor = collection.find(
                    query,
                    projection
                ).skip(collection_skip).limit(collection_limit)
                
                # Process in small batches to avoid memory spikes
                batch_size = 20
                current_batch = []
                
                for doc in cursor:
                    # Add station field
                    doc["station"] = collection_name
                    current_batch.append(doc)
                    
                    # Process in small batches
                    if len(current_batch) >= batch_size:
                        data.extend(current_batch)
                        current_batch = []
                        # Force garbage collection after each batch
                        gc.collect()
                
                # Add remaining items
                if current_batch:
                    data.extend(current_batch)
                
                # Update how many we've processed from this collection
                total_processed += collection.count_documents(query)
                
                # Break if we have enough data
                if len(data) >= limit:
                    break
        
        return data
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return []

def get_filtered_data_streaming(_db, offset=0, batch_size=100, station=None, group=None, label=None, repo=None, module=None, start_date=None, end_date=None):
    """Get filtered data in streaming mode to avoid memory issues"""
    return get_filtered_data_chunk(
        _db,
        skip=offset,
        limit=batch_size,
        station=station,
        group=group,
        label=label,
        repo=repo,
        module=module,
        start_date=start_date,
        end_date=end_date
    )

def get_filtered_data(_db, station=None, group=None, label=None, repo=None, module=None, start_date=None, end_date=None):
    """Get filtered data with enhanced streaming approach"""
    # Initialize or use existing offset from session state
    if "data_offset" not in st.session_state:
        st.session_state.data_offset = 0
    
    # Set a reasonable batch size
    batch_size = 200  # Increase from previous 100
    
    # Initialize data collection if not in session state
    if "accumulated_data" not in st.session_state:
        st.session_state.accumulated_data = []
    
    # Check if we need fresh data (filters changed)
    if st.session_state.filter_changed:
        # Reset accumulated data and offset
        st.session_state.accumulated_data = []
        st.session_state.data_offset = 0
    
    # Get initial batch of data
    new_data = get_filtered_data_streaming(
        _db,
        offset=st.session_state.data_offset,
        batch_size=batch_size,
        station=station,
        group=group,
        label=label,
        repo=repo,
        module=module,
        start_date=start_date,
        end_date=end_date
    )
    
    # Add to accumulated data
    if new_data:
        st.session_state.accumulated_data.extend(new_data)
        
        # Update offset for next time
        st.session_state.data_offset += len(new_data)
        
        # Check if we can get more data now
        if len(new_data) == batch_size and len(st.session_state.accumulated_data) < 1000:
            # We can potentially get more, but only if memory allows
            try:
                # Check memory usage - implement based on your needs
                import psutil
                process = psutil.Process()
                memory_usage = process.memory_info().rss / (1024 * 1024)  # MB
                
                # If memory usage is acceptable, get another batch
                if memory_usage < 200:  # Less than 200MB
                    more_data = get_filtered_data_streaming(
                        _db,
                        offset=st.session_state.data_offset,
                        batch_size=batch_size,
                        station=station,
                        group=group,
                        label=label,
                        repo=repo,
                        module=module,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if more_data:
                        st.session_state.accumulated_data.extend(more_data)
                        st.session_state.data_offset += len(more_data)
            except:
                # If memory check fails, just continue with what we have
                pass
    
    # Reset filter changed flag
    st.session_state.filter_changed = False
    
    # Return accumulated data
    return st.session_state.accumulated_data

def process_data(data, chunk_size=100):
    """Process data with enhanced memory optimizations"""
    if not data:
        return pd.DataFrame()
    
    try:
        # Convert to DataFrame in chunks to reduce memory usage
        dfs = []
        
        # Process in smaller chunks
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            
            # Create DataFrame for this chunk
            df_chunk = pd.DataFrame(chunk)
            
            # Convert ObjectId to string
            if '_id' in df_chunk.columns:
                df_chunk['_id'] = df_chunk['_id'].astype(str)
            
            # Handle dates
            if 'dtime' in df_chunk.columns:
                df_chunk['dtime'] = pd.to_datetime(df_chunk['dtime'], errors='coerce')
            
            # Use smaller data types where possible
            for col in df_chunk.columns:
                if df_chunk[col].dtype == 'float64':
                    df_chunk[col] = df_chunk[col].astype('float32')
                elif df_chunk[col].dtype == 'int64':
                    df_chunk[col] = df_chunk[col].astype('int32')
            
            dfs.append(df_chunk)
            
            # Force garbage collection
            gc.collect()
        
        # Combine all chunks
        if dfs:
            # More efficient concat by minimizing memory copies
            result = pd.concat(dfs, ignore_index=True, copy=False)
            
            # Clean up original list to free memory
            dfs.clear()
            gc.collect()
            
            return result
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return pd.DataFrame()

# New function for incrementally loading more data
def load_more_data(_db, station=None, group=None, label=None, repo=None, module=None, start_date=None, end_date=None):
    """Load additional data incrementally without full reload"""
    # Get current offset
    offset = st.session_state.get("data_offset", 0)
    batch_size = 200
    
    # Get next batch
    new_data = get_filtered_data_streaming(
        _db,
        offset=offset,
        batch_size=batch_size,
        station=station,
        group=group,
        label=label,
        repo=repo,
        module=module,
        start_date=start_date,
        end_date=end_date
    )
    
    if new_data:
        # Process new data
        new_df = process_data(new_data, chunk_size=50)
        
        # Update offset
        st.session_state.data_offset = offset + len(new_data)
        
        # Merge with existing DataFrame if it exists
        if "current_df" in st.session_state and st.session_state.current_df is not None:
            try:
                # Append new data to existing DataFrame
                combined_df = pd.concat([st.session_state.current_df, new_df], ignore_index=True)
                
                # Update session state
                st.session_state.current_df = combined_df
                
                # Clean up to free memory
                del new_df, combined_df
                gc.collect()
                
                return len(new_data)
            except Exception as e:
                logger.error(f"Error combining DataFrames: {str(e)}")
                return 0
        else:
            # Store new DataFrame in session state
            st.session_state.current_df = new_df
            
            # Clean up
            del new_df
            gc.collect()
            
            return len(new_data)
    
    return 0  # No new data

# Function to get estimated total document count (for progress indicators)
def get_estimated_document_count(_db, station=None, group=None, label=None, repo=None, module=None):
    """Get estimated document count without loading documents"""
    try:
        total_count = 0
        
        # Limit collections to query
        if station and station != "Select Station":
            collections_to_query = [station]
        else:
            collections_to_query = get_collections(_db)
        
        for collection_name in collections_to_query:
            if collection_name in get_collections(_db):
                collection = _db[collection_name]
                
                # Create query
                query = {}
                if group and group != "Select Group":
                    query["_group"] = group
                if label and label != "Select Label":
                    query["_label"] = label
                if repo and repo != "Select Repo":
                    query["repo"] = repo
                if module and module != "Select Module":
                    query["module"] = module
                
                # Use countDocuments instead of count (deprecated)
                collection_count = collection.count_documents(query)
                total_count += collection_count
        
        return total_count
    except Exception as e:
        logger.error(f"Error getting document count: {str(e)}")
        return 0