"""
Configuration Management
Loads environment variables and provides configuration objects
"""

import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Base configuration"""

    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'logistics_db1')
    MONGO_COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME', 'logistics_orders')

    FLASK_ENV = os.getenv('FLASK_ENV', 'production')
    FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5000))

    CSV_FILE_PATH = os.getenv('CSV_FILE_PATH', 'datasets/logistics_big_data.csv')
    GEOJSON_FILE_PATH = os.getenv('GEOJSON_FILE_PATH', 'datasets/pincode_simplified.geojson')

    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 100000))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000))
    MAX_CHUNKS = int(os.getenv('MAX_CHUNKS', 55))
    MIN_RECORDS_FOR_SKIP = int(os.getenv('MIN_RECORDS_FOR_SKIP', 200000))

    H3_RESOLUTIONS = [int(x) for x in os.getenv('H3_RESOLUTIONS', '6,7,8,9,10').split(',')]
    DEFAULT_H3_RESOLUTION = int(os.getenv('DEFAULT_H3_RESOLUTION', 8))

    DEFAULT_HEXAGON_LIMIT = int(os.getenv('DEFAULT_HEXAGON_LIMIT', 3000))
    DEFAULT_SUPPLY_POINT_LIMIT = int(os.getenv('DEFAULT_SUPPLY_POINT_LIMIT', 3000))

    GUNICORN_WORKERS = int(os.getenv('GUNICORN_WORKERS', 4))
    GUNICORN_THREADS = int(os.getenv('GUNICORN_THREADS', 2))
    GUNICORN_TIMEOUT = int(os.getenv('GUNICORN_TIMEOUT', 120))

    BASE_PATH = os.getenv('BASE_PATH', '')
    BASE_URL = os.getenv('BASE_URL', '')