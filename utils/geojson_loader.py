"""
GeoJSON file loading utilities
"""

import json
import logging
from config import Config
import os

logger = logging.getLogger(__name__)

def load_pincode_geojson():
    """Load pincode boundaries from GeoJSON file"""
    try:
        logger.info(f"Attempting to load GeoJSON file from: {Config.GEOJSON_FILE_PATH}")
        logger.info(f"File exists: {os.path.exists(Config.GEOJSON_FILE_PATH)}")
        with open(Config.GEOJSON_FILE_PATH, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        logger.info(f"Loaded {len(geojson_data['features'])} pincode boundaries")
        return geojson_data
    except FileNotFoundError:
        logger.error("Pincode GeoJSON not found, skipping")
        return None
    except Exception as e:
        logger.exception(f"Error loading pincode boundaries: {e}")
        return None