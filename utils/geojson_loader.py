"""
GeoJSON file loading utilities
"""

import json
from config import Config

def load_pincode_geojson():
    """Load pincode boundaries from GeoJSON file"""
    try:
        with open(Config.GEOJSON_FILE_PATH, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        print(f"Loaded {len(geojson_data['features'])} pincode boundaries")
        return geojson_data
    except FileNotFoundError:
        print("Pincode GeoJSON not found, skipping")
        return None
    except Exception as e:
        print(f"Error loading pincode boundaries: {e}")
        return None