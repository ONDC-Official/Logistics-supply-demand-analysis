import redis
import json
from config import Config

# Initialize Redis connection
redis_client = redis.Redis(
    host=Config.REDIS_HOST,
    port=Config.REDIS_PORT,
    db=0,
    decode_responses=True
)

def set_cache(key, value, expire_seconds=None):
    """Set cache value (JSON serialized)"""
    redis_client.set(key, json.dumps(value), ex=expire_seconds)

def get_cache(key):
    """Get cached value (JSON deserialized)"""
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    return None
