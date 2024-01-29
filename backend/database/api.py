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
            format="%(message)s %(asctime)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
        )
        self.logger = logging.getLogger(__class__.__name__)

    def run_pipeline(self, param):
        response = self.get_data(param)
        response = self.extractor_data(response)
        response = self.transform_df(response)
        return response

    def get_data(self, param):
        try:
            if param > 1:
                response = requests.get(
                    f"http://127.0.0.1:8000/compra_produto/{param}"
                ).json()
            else:
                response = requests.get("http://127.0.0.1:8000/compra_produto").json()
            self.logger.info(f"Obtencao de dados realizada com sucesso: ")
        except requests.RequestException as e:
            self.logger.warning(f"Obtencao de dados falhou: ")
            response = []
        return response

    def extractor_data(self, response):
        result = []
        try:
            for compra in response:
                dict_aux = {
                    key: compra[key] if isinstance(compra.get(key), value) else None
                    for key, value in self.__schema.items()
                }
                result.append(dict_aux)
            self.logger.info(f"Extracao de dados realizada com sucesso: ")
        except Exception as e:
            self.logger.warning(f"Extracao de dados falhou: ")
        return result

    def transform_df(self, response):
        try:
            result = pd.DataFrame(response)
            self.logger.info(f"DF criado com sucesso: ")
        except Exception as e:
            self.logger.warning(f"Criacao DF falhou: ")
            result = pd.DataFrame()
        return result

    def transform_to_parquet(self, response):
        self.__buffer = BytesIO()
        try:
            response.to_parquet(self.__buffer)
            self.logger.info(f"Parquet criado com sucesso: ")
        except Exception as e:
            self.logger.warning(f"Criacao parquet falhou: ")
            self.__buffer = None
