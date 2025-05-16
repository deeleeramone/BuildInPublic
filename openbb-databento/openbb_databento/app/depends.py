from typing import Annotated
from pathlib import Path

from fastapi import Depends
from openbb_store.store import Store
from pandas import DataFrame


historical_data_path = Path(__file__).parent.parent / "data" / "cache"

CacheStore = Store(str(historical_data_path))

historical_df = CacheStore.get_store("continuous")

ESOptions = Annotated[dict, Depends(lambda: CacheStore.get_store("es_options"))]

HistoricalData = Annotated[
    DataFrame,
    Depends(
        lambda: historical_df,
    ),
]

TermStructures = Annotated[
    dict, Depends(lambda: CacheStore.get_store("term_structures"))
]
