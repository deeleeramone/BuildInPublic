"""Main application."""

import asyncio

from typing import Annotated, Literal, Optional

from fastapi import FastAPI, Query

from demo_risk.constants import DATASET_CHOICES
from demo_risk.utils import (
    FactorsDoc,
    get_portfolio_data,
)

DATASETS = {}
LOADED_PORTFOLIO = None
LOADED_FACTORS = None

app = FastAPI()


@app.get("/templates.json")
async def get_templates():
    """Get templates."""
    return [
        {
            "name": "Factors",
            "img": "",
            "img_dark": "",
            "img_light": "",
            "description": "Fama-French Research Portfolio and Factor Data",
            "allowCustomization": True,
            "tabs": {
                "": {
                    "id": "",
                    "name": "",
                    "layout": [
                        {
                            "i": "fama_french_info_custom_obb",
                            "x": 0,
                            "y": 0,
                            "w": 12,
                            "h": 31,
                        },
                        {
                            "i": "load_portfolios_custom_obb",
                            "x": 12,
                            "y": 0,
                            "w": 28,
                            "h": 16,
                            "state": {
                                "params": {
                                    "start_date": "1995-01-01",
                                    "end_date": "2025-12-31",
                                },
                                "chartView": {"enabled": False, "chartType": "line"},
                                "columnState": {
                                    "default_": {
                                        "columnPinning": {
                                            "leftColIds": ["Date"],
                                            "rightColIds": [],
                                        }
                                    }
                                },
                            },
                        },
                        {
                            "i": "load_factors_custom_obb",
                            "x": 12,
                            "y": 16,
                            "w": 25,
                            "h": 15,
                            "state": {
                                "params": {
                                    "start_date": "1995-01-01",
                                    "end_date": "2025-12-31",
                                },
                                "chartView": {"enabled": False, "chartType": "line"},
                                "columnState": {
                                    "default_": {
                                        "columnPinning": {
                                            "leftColIds": ["Date"],
                                            "rightColIds": [],
                                        }
                                    }
                                },
                            },
                        },
                    ],
                }
            },
            "groups": [
                {
                    "name": "Group 3",
                    "type": "param",
                    "paramName": "frequency",
                    "defaultValue": "Monthly",
                    "widgetIds": [
                        "load_portfolios_custom_obb",
                        "load_factors_custom_obb",
                    ],
                },
                {
                    "name": "Group 2",
                    "type": "param",
                    "paramName": "start_date",
                    "defaultValue": "1995-01-01",
                    "widgetIds": [
                        "load_portfolios_custom_obb",
                        "load_factors_custom_obb",
                    ],
                },
                {
                    "name": "Group 4",
                    "type": "param",
                    "paramName": "end_date",
                    "defaultValue": "2025-12-31",
                    "widgetIds": [
                        "load_portfolios_custom_obb",
                        "load_factors_custom_obb",
                    ],
                },
            ],
        }
    ]


@app.get(
    "/fama_french_info",
    openapi_extra={
        "widget_config": {"refetchInterval": False},
        "name": "F-F Datasets Info",
    },
)
async def get_fama_french_info() -> str:
    """Get Fama French info."""
    descriptions = ""

    if not DATASETS:
        await asyncio.sleep(2)

    if DATASETS.get("loaded_portfolio"):
        descrip = DATASETS["loaded_portfolio"].get("meta", {}).get("description", "")
        descriptions += (
            "\n\n"
            + f"### {LOADED_PORTFOLIO.replace('_', ' ')}"
            + "\n\n"
            + descrip
            + "\n\n"
            if descrip
            else ""
        )
    if DATASETS.get("loaded_factors"):
        descrip = DATASETS["loaded_factors"].get("meta", {}).get("description", "")
        descriptions += (
            "\n\n"
            + f"### {LOADED_FACTORS.replace('_', ' ')}"
            + "\n\n"
            + descrip
            + "\n\n"
            if descrip
            else ""
        )

    return descriptions + "\n\n" + FactorsDoc


@app.get(
    "/load_portfolios",
    openapi_extra={
        "widget_config": {"refecthInterval": False, "name": "F-F Portfolio Data"}
    },
)
async def get_load_portfolios(
    portfolio: Annotated[
        str,
        Query(
            description="Portfolio set to load.",
            json_schema_extra={
                "x-widget_config": {
                    "style": {"popupWidth": 450},
                    "options": [
                        d for d in DATASET_CHOICES if "Portfolio" in d.get("value")
                    ],
                },
            },
        ),
    ] = "Portfolios_Formed_on_ME",
    measure: Annotated[
        Literal["Value", "Equal", "Number Of Firms", "Firm Size"],
        Query(description="Data measurement"),
    ] = "Value",
    frequency: Annotated[
        Literal["Monthly", "Annual"],
        Query(
            description="Data frequency. Only valid when the dataset is not daily (in the name)."
        ),
    ] = "Monthly",
    start_date: Annotated[
        Optional[str],
        Query(
            description="Start date",
            json_schema_extra={"x-widget_config": {"value": "$currentDate-30y"}},
        ),
    ] = None,
    end_date: Annotated[
        Optional[str],
        Query(
            description="End date",
            json_schema_extra={"x-widget_config": {"value": "$currentDate"}},
        ),
    ] = None,
) -> list:
    """Get dataset."""
    global DATASETS, LOADED_PORTFOLIO

    if not portfolio:
        return "Please select a portfolio."
    frequency = None if "Daily" in portfolio else frequency.lower()
    measure = measure.replace(" ", "_").lower()
    try:
        data, meta = get_portfolio_data(portfolio, frequency, measure)

        df = data[0]
        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]
        DATASETS["loaded_portfolio"] = {"meta": meta[0], "data": df}
        LOADED_PORTFOLIO = portfolio
        return df.reset_index().to_dict(orient="records") if data else []
    except Exception:
        return []


@app.get(
    "/load_factors",
    openapi_extra={
        "widget_config": {"refetchInterval": False, "name": "F-F Factors Data"}
    },
)
async def get_load_factors(
    factor: Annotated[
        str,
        Query(
            description="Factor",
            json_schema_extra={
                "x-widget_config": {
                    "style": {"popupWidth": 450},
                    "options": [
                        d
                        for d in DATASET_CHOICES
                        if "Factors" in d.get("value") or "Factor" in d.get("value")
                    ],
                },
            },
        ),
    ] = "F-F_Research_Data_5_Factors_2x3",
    frequency: Annotated[
        Literal["Monthly", "Annual"],
        Query(
            description="Data frequency. Only valid when the dataset is not daily or weekly (in the name)."
        ),
    ] = "Monthly",
    start_date: Annotated[
        Optional[str],
        Query(
            description="Start date",
            json_schema_extra={"x-widget_config": {"value": "$currentDate-30y"}},
        ),
    ] = None,
    end_date: Annotated[
        Optional[str],
        Query(
            description="End date",
            json_schema_extra={"x-widget_config": {"value": "$currentDate"}},
        ),
    ] = None,
) -> list:
    """Get dataset."""
    global DATASETS, LOADED_FACTORS

    if not factor:
        return "Please select a factor."

    if "daily" in factor or "weekly" in factor:
        frequency = None

    LOADED_FACTORS = factor
    try:
        dfs, meta = get_portfolio_data(factor, frequency)
        df = dfs[0]
        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]
        DATASETS["loaded_factors"] = {"meta": meta[0], "data": df}
        return df.reset_index().to_dict(orient="records")
    except Exception:
        return []


def main():
    """Run the app."""
    import subprocess

    subprocess.run(
        [
            "openbb-api",
            "--app",
            __file__,
            "--port",
            "6020",
        ]
    )


if __name__ == "__main__":
    main()
