from fastapi import APIRouter
from openbb_databento.app.depends import TermStructures

router = APIRouter(prefix="/term_structures")

TS_SYMBOLS = {
    "CL": "Crude Oil",
    "NG": "Natural Gas",
    "GC": "Gold",
    "ES": "E-Mini S&P 500",
    "NQ": "E-Mini Nasdaq 100",
    "RTY": "E-Mini Russell 2000",
    "YM": "E-Mini Dow Jones Industrial Average",
}


@router.get("/get_choices", openapi_extra={"widget_config": {"exclude": True}})
async def get_choices(store: TermStructures) -> list:
    """Get the list of available term structures."""
    return [{"label": v, "value": k} for k, v in TS_SYMBOLS.items()]


@router.get("/get_term_structure")
async def get_term_structure(
    store: TermStructures,
    symbol: str = "CL",
    date: str = "2025-05-01",
) -> list:
    """Get the term structure for a given symbol."""
    if symbol not in TS_SYMBOLS:
        raise ValueError(f"Invalid symbol: {symbol}")

    term_structure = store.get(symbol, {}).get(date, None)
    if term_structure is None:
        raise ValueError(f"No data found for symbol: {symbol}")

    return term_structure.to_dict(orient="records")
