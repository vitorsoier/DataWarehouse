from typing import Dict, Union

GenericSchema = Dict[str, Union[int, str, float]]

ApiScheam : GenericSchema = {
    "ean": int,
    "price": float,
    "store": int,
    "dateTime": str,
}
