import logging
import requests
import pandas as pd
from datetime import datetime
from contracts.schema import GenericSchema
from typing import List
from io import BytesIO


class ApiCollector:

    def __init__(self, schema):
        self.__schema = schema
        self.__buffer = None

        logging.basicConfig(
            filename="backend/logs.log",
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
        )

        self.logger = logging.getLogger(__class__.__name__)

    def runPipeline(self, param):
        response = self.getData(param)
        if response != None:
            response = self.extractorData(response)
            if response != None:
                response = self.transformDf(response)
            else:
                self.logger.warning(f"Processo falhou na etapa extractor_data")
        else:
            self.logger.warning(f"Processo falhou na etapa get_data")
        return response

    def getData(self, param):
        try:
            if param > 1:
                response = requests.get(
                    f"http://127.0.0.1:8000/compra_produto/{param}").json()
            else:
                response = requests.get("http://127.0.0.1:8000/compra_produto").json()
            self.logger.info(f"Obtencao de dados realizada com sucesso")
        except:
            self.logger.warning(f"Obtencao de dados falhou")
            response = None
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
            self.logger.info(f"Extracao de dados realizada com sucesso")
        except:
            self.logger.warning(f"Extracao de dados falhou")
            return None
        return result

    def transformDf(self, response):
        try:
            result = pd.DataFrame(response)
            self.logger.info(f"DF criado com sucesso")
        except:
            self.logger.warning(f"Criacao DF falhou")
            result = pd.DataFrame()
        return result

    def transformToParquet(self, response):
        self.__buffer = BytesIO()
        try:
            response.to_parquet(self.__buffer)
            self.logger.info(f"Parquet criado com sucesso: ")
        except Exception as e:
            self.logger.warning(f"Criacao parquet falhou: ")
            self.__buffer = None
