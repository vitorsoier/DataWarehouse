import requests

class ApiCollector:

    def __init__(self):
        return
    
    def start(self):
        return
    
    def getData(self, param):
        if param > 1:
            response = requests.get(f'http://127.0.0.1:8000/compra_produto/{param}').json()
        else:
            response = requests.get(f'http://127.0.0.1:8000/compra_produto').json()
        return response
    
    def extractorData(self):
        return
    
    def transformDf(self):
        return