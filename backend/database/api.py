import requests
import pandas as pd
import logging 

from datetime import datetime
from contracts.schema import GenericSchema, ApiSchema
from typing import List
from io import BytesIO


class ApiCollector:

    def __init__(self, schema):
        self.__schema = schema
        self.__buffer = None

        logging.basicConfig(filename = "backend/logs.log", level=logging.INFO)
        self.logger = logging.getLogger(__class__.__name__)
        return
    
    def run_pipeline(self, param):
        response = self.getData(param)
        response = self.extractorData(response)
        response = self.transformDf(response)
        return response 
    
    def getData(self, param):
        try:
            if param > 1:
                response = requests.get(f'http://127.0.0.1:8000/compra_produto/{param}').json()
            else:
                response = requests.get(f'http://127.0.0.1:8000/compra_produto').json()
            self.logger.info(f"Obtencao de dados realizada com sucecsso " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S"))
        except:
            self.logger.warning(f"Obtencao de dados falhou " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S"))
        return response
    
    def extractorData(self, response):
        try:
            result : List[GenericSchema] = []
            for compra in response:
                dict_aux = {}
                for key, value in self.__schema.items():
                    if type(compra.get(key)) == value:
                        dict_aux[key] = compra[key]
                    else:
                        dict_aux[key] = None   
                result.append(dict_aux)  
            self.logger.info(f"Obtencao de dados realizada com sucecsso " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S")) 
        except:
            self.logger.warning(f"Extracao de dados falhou " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S"))
        return result
    
    def transformDf(self, response):
        try:
            result = pd.DataFrame(response)
            self.logger.info(f"DF criado com sucecsso " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S"))
        except:
            self.logger.warning(f"Criacao DF falhou " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S"))
        return result
    
    def transforToParquet(self, response):
        self.__buffer = BytesIO()
        try:
            response.to_parquet(self.__buffer)
            self.logger.info(f"parquet criado com sucecsso " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S"))
        except:
            print("Error: não foi possível transformar o DF em parquet")
            self.logger.warning(f"Criacao parquet falhou " + datetime.now().strftime("%Y-%h-%d-%H:%M:%S"))
            self.__buffer = None