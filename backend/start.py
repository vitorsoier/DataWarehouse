from database.api import ApiCollector
from contracts.schema import ApiSchema
from google.cloud import storage

storage_client = storage.Client()
api = ApiCollector(ApiSchema).runPipeline(
    5, storage_client, "vitorsoier-teste01-datawarehouse"
)

print(api)
