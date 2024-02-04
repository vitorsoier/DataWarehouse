import schedule
import time

from database.api import ApiCollector
from contracts.schema import ApiSchema
from google.cloud import storage

storage_client = storage.Client()

def ApiToBucket (schema, repeat, client, buckets_name):
    api = ApiCollector(schema).runPipeline(repeat, client, buckets_name)

schedule.every(1).minute.do(ApiToBucket, ApiSchema, 50, storage_client, 'vitorsoier-teste01-datawarehouse')

while True:
    schedule.run_pending()
    time.sleep(1)