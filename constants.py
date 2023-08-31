import redis
import os

ESRI_API_KEY = os.environ.get('ESRI_API_KEY')
redis_instance = redis.StrictRedis.from_url(
    os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
)
