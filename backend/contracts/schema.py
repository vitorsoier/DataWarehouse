from typing import Dict, Union

GenericSchema = Dict[str, Union[int, str, float]]

ApiSchema: GenericSchema = {
    "ean": int,
    "price": float,
    "store": int,
    "dateTime": str,
}
