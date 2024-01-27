import pandas as pd
import random

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from faker import Faker

app = FastAPI()
fake = Faker("pt-BR")

id_loja_online = 11
file_name = "products.csv"

with open("index.html", "r", encoding="utf-8") as file:
    content = file.read()

df = pd.read_csv(file_name)


@app.get("/", response_class=HTMLResponse)
async def inicial():
    return HTMLResponse(content=content)


@app.get("/compra_produto")
async def compra_produto():

    index = random.randint(0, len(df) - 1)
    return [
        {
            "Name": fake.name(),
            "Creditcard": fake.credit_card_provider(),
            "Product": df.nome[index],
            "ean": int(df.ean[index]),
            "price": df.price[index],
            "clientPosition": fake.administrative_unit(),
            "store": id_loja_online,
            "dateTime": fake.date_this_year(),
        }
    ]


@app.get("/compra_produto/{total_compras}")
async def compra_produto(total_compras: int):

    if total_compras < 1:
        return "error: O número de compras tem que ser maior que 1"

    compras = []
    for i in range(total_compras):
        index = random.randint(0, len(df) - 1)
        compra = {
            "Name": fake.name(),
            "Creditcard": fake.credit_card_provider(),
            "Product": df.nome[index],
            "ean": int(df.ean[index]),
            "price": df.price[index],
            "clientPosition": fake.administrative_unit(),
            "store": id_loja_online,
            "dateTime": fake.date_this_year(),
        }
        compras.append(compra)

    return compras
