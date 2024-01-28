import requests

from contracts.schema import GenericSchema, ApiSchema
from typing import List

class ApiCollector:

    def __init__(self, schema):
        self.__schema = schema
        return
    
    def run_pipeline(self, param):
        response = self.getData(param)
        response = self.extractorData(response)
        return response 
    
    def getData(self, param):
        if param > 1:
            response = requests.get(f'http://127.0.0.1:8000/compra_produto/{param}').json()
        else:
            response = requests.get(f'http://127.0.0.1:8000/compra_produto').json()
        return response
    
    def extractorData(self, response):
        result : List[GenericSchema] = []
        for compra in response:
            dict_aux = {}
            for key, value in self.__schema.items():
                if type(compra.get(key)) == value:
                    dict_aux[key] = compra[key]
                else:
                    dict_aux[key] = None   
            result.append(dict_aux)     
        return result
    
    def transformDf(self):
        return