from database.api import ApiCollector
from contracts.schema import ApiSchema

api = ApiCollector(ApiSchema).runPipeline(3)

print(api)
