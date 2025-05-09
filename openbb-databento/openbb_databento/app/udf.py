from fastapi import APIRouter, Query
from typing import Optional, Annotated
from openbb_databento.app.depends import HistoricalData


router = APIRouter(prefix="/udf")


UDF_CONFIG = {
    "supported_resolutions": ["D", "W", "M"],
    "supports_group_request": False,
    "supports_marks": False,
    "supports_search": True,
    "supports_timescale_marks": False,
    "supports_time": False,
    "exchanges": [{"name": "Globex", "value": "Globex"}],
    "symbols_types": [
        {"name": "Futures", "value": "future"},
    ],
}


@router.get("/config")
async def udf_config() -> dict:
    """Get UDF config."""
    return UDF_CONFIG


@router.get("/symbols")
async def symbols(
    data: HistoricalData,
    symbol: str,
) -> dict:
    """Get symbol info."""

    df = data.query("symbol == @symbol").copy()
    name = df.name.values[0] if not df.empty else symbol

    result = {
        "name": symbol,
        "ticker": symbol,
        "description": name,
        "type": "futures",
        "exchange": "Globex",
        "listed_exchange": "Globex",
        "timezone": "America/Chicago",
        "minmov": 1,
        "pricescale": 100,
        "has_intraday": False,
        "has_daily": True,
        "has_weekly_and_monthly": True,
        "supported_resolutions": ["D", "W", "M"],
        "currency_code": "USD",
        "original_currency_code": "USD",
        "volume_precision": 1,
    }

    return result


@router.get("/search")
async def search_symbols(
    data: HistoricalData,
    query: str = "",
    exchange: str = "",
    limit: int = 20,
):

    df = data.copy()
    df.drop_duplicates(subset=["name"], inplace=True)

    try:
        search_results = df[
            df.symbol.str.contains(query, case=False)
            | df.name.str.contains(query, case=False)
        ]

        results = [
            dict(
                symbol=item["symbol"],
                full_name=f"Globex:{item['symbol']}",
                description=item["name"],
                exchange="Globex",
                listed_exchange="Globex",
                ticker=item["symbol"],
                type="future",
            )
            for item in search_results.to_dict(orient="records")
        ]

        return results
    except Exception as e:
        print(f"Error in symbol search: {e}")
        return []


@router.get("/history")
async def history(
    data: HistoricalData,
    symbol: str = "ES.c.0",
    resolution: Optional[str] = None,
    from_time: Annotated[
        Optional[int],
        Query(
            alias="from",
        ),
    ] = None,
    to_time: Annotated[
        Optional[int],
        Query(
            alias="to",
        ),
    ] = None,
    countback: Optional[int] = None,
) -> dict:
    """Get OHLC bars."""
    from datetime import datetime
    from pandas import to_datetime
    from pytz import timezone

    df = data.query("symbol == @symbol").copy()

    if resolution in ("1W", "1M"):
        df.date = to_datetime(df.date)
        rule = "1W" if "1W" in resolution else "MS"
        df = df.reset_index(drop=True).set_index("date")
        df = (
            df.resample(rule)
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna()
            .reset_index()
        )
        df.date = to_datetime(df.date)

    def apply_ts(x):
        """Apply timestamp with offset for intraday data."""
        d = (
            datetime.fromisoformat(x if isinstance(x, str) else x.isoformat())
            .astimezone(timezone("America/Chicago"))
            .timestamp()
        )
        return int(d)

    df.loc[:, "timestamp"] = df.date.apply(apply_ts)

    MIN_TIMESTAMP = df.timestamp.min()
    MAX_TIMESTAMP = df.timestamp.max()

    if from_time is not None and to_time is not None and to_time < MIN_TIMESTAMP:
        return {"s": "no_data"}

    if from_time is not None and from_time > MAX_TIMESTAMP:
        return {"s": "no_data"}

    if df.empty or len(df.index) == 0:
        return {"s": "no_data"}

    # Filter by from_time and to_time if provided
    if from_time is not None:
        df = df[df.timestamp >= from_time]

    if to_time is not None:
        df = df[df.timestamp <= to_time]

    if df.empty or len(df.index) == 0:
        return {"s": "no_data"}

    return {
        "s": "ok",
        "t": df.timestamp.tolist(),
        "c": df.close.tolist(),
        "o": df.open.tolist(),
        "h": df.high.tolist(),
        "l": df.low.tolist(),
        "v": df.volume.tolist(),
    }
