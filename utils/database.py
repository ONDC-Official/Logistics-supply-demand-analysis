"""
Database utilities for MongoDB operations
"""

from pymongo import MongoClient
from config import Config

_client = None
_db = None
_collection = None

def get_db_collection():
    """Get MongoDB collection (singleton pattern)"""
    global _client, _db, _collection
    
    if _collection is None:
        _client = MongoClient(Config.MONGO_URI)
        _db = _client[Config.MONGO_DB_NAME]
        _collection = _db[Config.MONGO_COLLECTION_NAME]
    
    return _collection

def get_statistics(logistics_player='All', hour_bin='All'):
    """Get statistics with filters"""
    collection = get_db_collection()
    
    pipeline = []
    
    match_conditions = {}
    if logistics_player != 'All':
        match_conditions['logistics_player'] = logistics_player
    if hour_bin != 'All':
        match_conditions['hour_bin'] = hour_bin
    
    if match_conditions:
        pipeline.append({'$match': match_conditions})
    
    pipeline.append({
        '$group': {
            '_id': None,
            'total_orders': {'$sum': 1},
            'successful_orders': {
                '$sum': {'$cond': [{'$eq': ['$order_status', 'success']}, 1, 0]}
            },
            'unique_locations': {
                '$addToSet': {
                    '$concat': [
                        {'$toString': '$pickup_lat'},
                        ',',
                        {'$toString': '$pickup_lon'}
                    ]
                }
            }
        }
    })
    
    pipeline.append({
        '$project': {
            'total_orders': 1,
            'successful_orders': 1,
            'success_rate': {
                '$multiply': [
                    {'$divide': ['$successful_orders', '$total_orders']},
                    100
                ]
            },
            'total_restaurants': {'$size': '$unique_locations'}
        }
    })
    
    results = list(collection.aggregate(pipeline))
    
    if results:
        result = results[0]
        return {
            'total_orders': result['total_orders'],
            'successful_orders': result['successful_orders'],
            'success_rate': round(result['success_rate'], 1),
            'total_restaurants': result['total_restaurants']
        }
    
    return {'total_orders': 0, 'successful_orders': 0, 'success_rate': 0, 'total_restaurants': 0}

def get_filters():
    """Get unique filter values"""
    collection = get_db_collection()
    
    logistics_players = collection.distinct('logistics_player', {
        'logistics_player': {'$nin': [None, '', 'unknown']}
    })
    logistics_players = sorted([str(p) for p in logistics_players if p])
    
    hour_bins = sorted(collection.distinct('hour_bin'))
    
    return logistics_players, hour_bins