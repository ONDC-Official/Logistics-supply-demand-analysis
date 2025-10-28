import os
import pandas as pd
import h3
from config import base_dir 

def load_logistics_data(csv_path):
    """Load and process logistics data"""
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()
        
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed', errors='coerce')
        df = df.dropna(subset=['Timestamp'])
        
        df['Hour'] = df['Timestamp'].dt.hour
        df['Date'] = df['Timestamp'].dt.date
        df['Hour_Bin'] = df['Hour'].apply(lambda x: f"{x:02d}-{(x+1):02d}")
        
        df['Restaurant Latitude'] = pd.to_numeric(df['Restaurant Latitude'], errors='coerce')
        df['Restaurant Longitude'] = pd.to_numeric(df['Restaurant Longitude'], errors='coerce')
        df = df.dropna(subset=['Restaurant Latitude', 'Restaurant Longitude'])
        
        df['h3_index_id'] = df.apply(
            lambda row: h3.latlng_to_cell(
                row['Restaurant Latitude'], 
                row['Restaurant Longitude'],
                res=8
            ),
            axis=1
        )
        output_path = os.path.join(base_dir, 'datasets', 'data_with_h3.csv')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        print(f"Loaded {len(df)} valid logistics records")
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None