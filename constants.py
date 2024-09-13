import redis
import os
import json

ESRI_API_KEY = os.environ.get('ESRI_API_KEY')
redis_instance = redis.StrictRedis.from_url(
    os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
)

all_variables = []
long_names = {}

with open('schema.json', "r") as fp:
    schema = json.load(fp)

for var in schema:
    all_variables.append(var['name'])
    long_names[var['name']] = var['description']

meta_variables = ['latitude', 'longitude', 'observation_date', 'observation_depth', 'lon360', 'platform_code', 'platform_type', 'country', 'platform_id', 'longitude360']

data_variables = [x for x in all_variables if (x not in meta_variables)]
