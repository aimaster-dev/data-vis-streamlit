import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample_data():
    """Generate sample data for the dashboard"""
    
    # Set seed for reproducibility
    np.random.seed(42)
    
    # Define stations
    stations = ['A', 'B', 'F', 'I', 'K', 'P', 'P2', 'Q']
    
    # Generate date range from Jul 12, 2020 to Nov 12, 2024
    start_date = datetime(2020, 7, 12)
    end_date = datetime(2024, 11, 12)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Function to generate spikes for a station
    def generate_station_data(station_index, metric, date_range):
        # Base signal
        base = np.zeros(len(date_range))
        
        # Add random spikes
        num_spikes = np.random.randint(5, 15)
        spike_indices = np.random.choice(range(len(date_range)), size=num_spikes, replace=False)
        
        for idx in spike_indices:
            # Random spike height between 500 and 5000
            height = np.random.randint(500, 5000)
            
            # Sometimes create a very tall spike
            if np.random.random() < 0.1:
                height = np.random.randint(5000, 10000)
                
            # Create a spike with some width
            width = np.random.randint(1, 5)
            for w in range(-width, width + 1):
                if 0 <= idx + w < len(base):
                    # Decrease height as we move away from center
                    factor = 1 - (abs(w) / (width + 1))
                    base[idx + w] += height * factor
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': date_range,
            'station': stations[station_index],
            f'{metric}_count': base
        })
        
        return df
    
    # Generate data for each station and each metric
    all_test_data = []
    all_label_data = []
    all_repo_data = []
    all_module_data = []
    all_method_data = []
    all_group_data = []
    
    for i, station in enumerate(stations):
        # Generate test data
        all_test_data.append(generate_station_data(i, 'test', date_range))
        
        # Generate label data
        all_label_data.append(generate_station_data(i, 'label', date_range))
        
        # Generate repo data
        all_repo_data.append(generate_station_data(i, 'repo', date_range))
        
        # Generate module data
        all_module_data.append(generate_station_data(i, 'module', date_range))
        
        # Generate method data (more binary in nature)
        method_data = generate_station_data(i, 'method', date_range)
        method_data['method_count'] = np.where(
            method_data['method_count'] > 0,
            np.random.choice([1, 2], size=len(method_data)),
            0
        )
        all_method_data.append(method_data)
        
        # Generate group data
        all_group_data.append(generate_station_data(i, 'group', date_range))
    
    # Combine all data
    test_data = pd.concat(all_test_data)
    label_data = pd.concat(all_label_data)
    repo_data = pd.concat(all_repo_data)
    module_data = pd.concat(all_module_data)
    method_data = pd.concat(all_method_data)
    group_data = pd.concat(all_group_data)
    
    # Return all data
    return {
        'stations': stations,
        'test_data': test_data,
        'label_data': label_data,
        'repo_data': repo_data,
        'module_data': module_data,
        'method_data': method_data,
        'group_data': group_data
    }