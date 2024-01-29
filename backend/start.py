from database.api import ApiCollector
from contracts.schema import ApiSchema

api = ApiCollector(ApiSchema).run_pipeline(3)

print(api)
