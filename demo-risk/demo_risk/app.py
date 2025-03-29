"""Main application."""

import asyncio

from typing import Annotated, Literal, Optional

from fastapi import FastAPI, Query
from openbb_core.provider.abstract.data import Data
from pandas import DataFrame, DateOffset, Timestamp
from pydantic import Field, model_validator, model_serializer

from demo_risk.constants import DATASET_CHOICES, FACTOR_REGION_MAP, REGIONS_MAP
from demo_risk.depends import PortfolioData
from demo_risk.correlation_matrix import correlation_matrix
from demo_risk.utils import (
    FactorsDoc,
    get_portfolio_data,
    perform_ols,
)

DATASETS = {}
LOADED_PORTFOLIO = None
LOADED_FACTORS = None

app = FastAPI()


class PriceHistory(Data):
    """Historical Price Performance."""

    date: str = Field(
        description="Closing date of the stock.",
        title="Date",
        json_schema_extra={
            "x-widget_config": {
                "cellDataType": "date",
                "chartDataType": "time",
            },
        },
        alias="Date",
    )
    close: float = Field(
        description="Closing value.",
        title="Close",
        json_schema_extra={
            "x-widget_config": {
                "chartDataType": "series",
            }
        },
    )


class OLSData(Data):
    """OLS Data."""

    period: str = Field(
        None,
        description="The time period represented by the statistics.",
    )
    factor: str = Field(None, description="The factor in the factors dataset.")
    coefficient: Optional[float] = Field(
        None,
        description="The coefficient of the factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none"}},
    )
    p_value: Optional[float] = Field(
        None,
        description="The p-value of the factor in the OLS model.",
        title="P-Value",
        json_schema_extra={"x-widget_config": {"formatterFn": "none"}},
    )
    lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the factor in the OLS model.",
        title="Lower CI",
        json_schema_extra={"x-widget_config": {"formatterFn": "none"}},
    )
    upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the factor in the OLS model.",
        title="Upper CI",
        json_schema_extra={"x-widget_config": {"formatterFn": "none"}},
    )


class HoldingsData(Data):
    """Portfolio Holdings Data."""

    symbol: str = Field(None, description="The ticker of the stock.", alias="Symbol")
    name: str = Field(None, description="The name of the stock.", alias="Name")
    country: str = Field(
        None,
        description="The country of the stock.",
        alias="Country",
    )
    sector: str = Field(
        None,
        description="The sector of the stock.",
        alias="Sector",
    )
    industry: str = Field(
        None,
        description="The industry of the stock.",
        alias="Industry",
    )
    weight: Optional[float] = Field(
        None,
        description="The weight of the stock in the portfolio.",
        json_schema_extra={"x-unit_measurement": "percent"},
        alias="Weight",
    )


class PortfolioFactors(Data):
    """Portfolio Factors Data."""

    period: str = Field(
        None,
        description="The time period represented by the statistics.",
    )
    const_coeff: dict = Field(
        None,
        description="The constant coefficient in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "Constant",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    const_p_value: Optional[float] = Field(
        None,
        description="The p-value of the constant coefficient in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    const_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the constant coefficient in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    const_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the constant coefficient in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    mkt_rf_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the market factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "Mkt-RF",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    mkt_rf_p_value: Optional[float] = Field(
        None,
        description="The p-value of the market factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    mkt_rf_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the market factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    smb_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the SMB factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "SMB",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    smb_p_value: Optional[float] = Field(
        None,
        description="The p-value of the SMB factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    smb_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the SMB factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    smb_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the SMB factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    hml_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the HML factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "HML",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    hml_p_value: Optional[float] = Field(
        None,
        description="The p-value of the HML factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    hml_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the HML factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    hml_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the HML factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    rmw_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the RMW factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "RMW",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    rmw_p_value: Optional[float] = Field(
        None,
        description="The p-value of the RMW factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "formatterFn": "none",
                "hide": True,
            }
        },
    )
    rmw_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the RMW factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    rmw_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the RMW factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    cma_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the CMA factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "CMA",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    cma_p_value: Optional[float] = Field(
        None,
        description="The p-value of the CMA factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    cma_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the CMA factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    cma_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the CMA factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    mom_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the momentum factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "Mom",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    mom_p_value: Optional[float] = Field(
        None,
        description="The p-value of the momentum factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    mom_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the momentum factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    mom_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the momentum factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    wml_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the WML factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "WML",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    wml_p_value: Optional[float] = Field(
        None,
        description="The p-value of the WML factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    wml_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the WML factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    wml_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the WML factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    st_rev_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the short-term reversal factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "ST Reversal",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{P-Value}\n- {ConfidenceBands}",
                    }
                },
            }
        },
    )
    st_rev_p_value: Optional[float] = Field(
        None,
        description="The p-value of the short-term reversal factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    st_rev_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the short-term reversal factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    st_rev_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the short-term reversal factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    lt_rev_coeff: Optional[dict] = Field(
        None,
        description="The coefficient of the long-term reversal factor in the OLS model.",
        json_schema_extra={
            "x-widget_config": {
                "headerName": "LT Reversal",
                "formatterFn": "none",
                "renderFn": "hoverCard",
                "renderFnParams": {
                    "hoverCardData": {
                        "cellField": "value",
                        "title": "P-Values and Confidence Intervals.",
                        "markdown": "{description}\n- {additionalInfo}",
                    }
                },
            }
        },
    )
    lt_rev_p_value: Optional[float] = Field(
        None,
        description="The p-value of the long-term reversal factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    lt_rev_lower_ci: Optional[float] = Field(
        None,
        description="The lower confidence interval of the long-term reversal factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )
    lt_rev_upper_ci: Optional[float] = Field(
        None,
        description="The upper confidence interval of the long-term reversal factor in the OLS model.",
        json_schema_extra={"x-widget_config": {"formatterFn": "none", "hide": True}},
    )

    @model_validator(mode="before")
    def model_validate(cls, values):
        """Validate the model."""
        output = {}
        for k, v in values.items():
            if not v:
                continue
            output[k] = v

        return output

    @model_serializer()
    def model_serialize(self, return_type=dict):
        """Serialize the model."""
        output = {}
        for k in self.model_fields:
            if not getattr(self, k, None):
                continue
            output[k] = getattr(self, k)

        return output


@app.get("/templates.json", openapi_extra={"widget_config": {"exclude": True}})
async def get_templates():
    """Get templates."""
    return [
        {
            "name": "Factors",
            "img": "",
            "img_dark": "",
            "img_light": "",
            "description": "factors",
            "allowCustomization": True,
            "tabs": {
                "reference-data": {
                    "id": "reference-data",
                    "name": "Reference Data",
                    "layout": [
                        {
                            "i": "fama_french_info_custom_obb",
                            "x": 0,
                            "y": 2,
                            "w": 12,
                            "h": 25,
                        },
                        {
                            "i": "load_factors_custom_obb",
                            "x": 12,
                            "y": 13,
                            "w": 28,
                            "h": 14,
                            "state": {
                                "params": {
                                    "frequency": "monthly",
                                    "start_date": "2021-01-01",
                                    "end_date": "2025-03-27",
                                },
                                "chartView": {"enabled": False, "chartType": "line"},
                                "columnState": {
                                    "default_undefined": {
                                        "focusedCell": {
                                            "colId": "Mkt-RF",
                                            "rowIndex": 0,
                                            "rowPinned": None,
                                        }
                                    }
                                },
                            },
                        },
                        {
                            "i": "load_portfolios_custom_obb",
                            "x": 12,
                            "y": 2,
                            "w": 28,
                            "h": 11,
                            "state": {
                                "params": {
                                    "portfolio": "Portfolios_Formed_on_OP",
                                    "start_date": "2021-01-01",
                                    "end_date": "2025-03-27",
                                },
                                "chartView": {"enabled": False, "chartType": "line"},
                            },
                        },
                    ],
                },
                "portfolio-price--performance": {
                    "id": "portfolio-price--performance",
                    "name": "Portfolio Price & Performance",
                    "layout": [
                        {
                            "i": "portfolio_unit_price_custom_obb",
                            "x": 0,
                            "y": 2,
                            "w": 40,
                            "h": 24,
                            "state": {
                                "params": {"portfolio": "Client 2", "returns": "True"},
                                "chartView": {"enabled": True, "chartType": "line"},
                                "columnState": {
                                    "default_undefined": {
                                        "focusedCell": {
                                            "colId": "close",
                                            "rowIndex": 0,
                                            "rowPinned": None,
                                        },
                                        "rangeSelection": {
                                            "cellRanges": [
                                                {
                                                    "startRow": {
                                                        "rowIndex": 0,
                                                        "rowPinned": None,
                                                    },
                                                    "endRow": {
                                                        "rowIndex": 0,
                                                        "rowPinned": None,
                                                    },
                                                    "colIds": ["close"],
                                                    "startColId": "close",
                                                }
                                            ]
                                        },
                                    }
                                },
                            },
                        }
                    ],
                },
                "portfolio-region-and-sector-exposure": {
                    "id": "portfolio-region-and-sector-exposure",
                    "name": "Portfolio Region and Sector Exposure",
                    "layout": [
                        {
                            "i": "portfolio_sectors_custom_obb",
                            "x": 0,
                            "y": 13,
                            "w": 19,
                            "h": 14,
                            "state": {
                                "chartView": {"enabled": True, "chartType": "pie"}
                            },
                        },
                        {
                            "i": "portfolio_countries_custom_obb",
                            "x": 0,
                            "y": 2,
                            "w": 19,
                            "h": 11,
                            "state": {
                                "chartView": {"enabled": True, "chartType": "pie"}
                            },
                        },
                        {
                            "i": "portfolio_industries_custom_obb",
                            "x": 19,
                            "y": 2,
                            "w": 21,
                            "h": 25,
                            "state": {
                                "params": {"portfolio": "Client 3"},
                                "chartView": {"enabled": True, "chartType": "pie"},
                            },
                        },
                    ],
                },
                "portfolio-holdings": {
                    "id": "portfolio-holdings",
                    "name": "Portfolio Holdings",
                    "layout": [
                        {
                            "i": "portfolio_holdings_custom_obb",
                            "x": 0,
                            "y": 2,
                            "w": 40,
                            "h": 25,
                            "state": {
                                "params": {"portfolio": "Client 2"},
                                "chartView": {"enabled": True, "chartType": "bar"},
                            },
                        }
                    ],
                },
                "portfolio-holdings-correlations": {
                    "id": "portfolio-holdings-correlations",
                    "name": "Portfolio Holdings Correlations",
                    "layout": [
                        {
                            "i": "holdings_correlation_custom_obb",
                            "x": 0,
                            "y": 2,
                            "w": 40,
                            "h": 26,
                            "state": {"params": {"portfolio": "Client 2"}},
                        }
                    ],
                },
                "portfolio-factor-correlations": {
                    "id": "portfolio-factor-correlations",
                    "name": "Portfolio Factor Correlations",
                    "layout": [
                        {
                            "i": "portfolio_factors_custom_obb",
                            "x": 0,
                            "y": 2,
                            "w": 30,
                            "h": 9,
                            "state": {
                                "chartView": {"enabled": False, "chartType": "line"},
                                "columnState": {
                                    "default_undefined": {
                                        "columnVisibility": {
                                            "hiddenColIds": [
                                                "const_p_value",
                                                "const_lower_ci",
                                                "const_upper_ci",
                                                "mkt_rf_p_value",
                                                "mkt_rf_lower_ci",
                                                "mkt_rf_upper_ci",
                                                "smb_p_value",
                                                "smb_lower_ci",
                                                "smb_upper_ci",
                                                "hml_p_value",
                                                "hml_lower_ci",
                                                "hml_upper_ci",
                                                "rmw_p_value",
                                                "rmw_lower_ci",
                                                "rmw_upper_ci",
                                                "cma_p_value",
                                                "cma_lower_ci",
                                                "cma_upper_ci",
                                            ]
                                        }
                                    }
                                },
                            },
                        }
                    ],
                },
            },
            "groups": [
                {
                    "name": "Group 3",
                    "type": "param",
                    "paramName": "frequency",
                    "defaultValue": "monthly",
                    "widgetIds": [
                        "load_factors_custom_obb",
                        "load_portfolios_custom_obb",
                    ],
                },
                {
                    "name": "Group 2",
                    "type": "param",
                    "paramName": "start_date",
                    "defaultValue": "2021-01-01",
                    "widgetIds": [
                        "load_factors_custom_obb",
                        "load_portfolios_custom_obb",
                    ],
                },
                {
                    "name": "Group 4",
                    "type": "param",
                    "paramName": "end_date",
                    "defaultValue": "2025-03-27",
                    "widgetIds": [
                        "load_factors_custom_obb",
                        "load_portfolios_custom_obb",
                    ],
                },
                {
                    "name": "Group 5",
                    "type": "param",
                    "paramName": "region",
                    "defaultValue": "america",
                    "widgetIds": [
                        "load_factors_custom_obb",
                        "load_portfolios_custom_obb",
                    ],
                },
                {
                    "name": "Group 6",
                    "type": "endpointParam",
                    "paramName": "factor",
                    "defaultValue": "america",
                    "widgetIds": [
                        "load_factors_custom_obb",
                        "load_portfolios_custom_obb",
                    ],
                },
                {
                    "name": "Group 7",
                    "type": "param",
                    "paramName": "portfolio",
                    "defaultValue": "Client 1",
                    "widgetIds": [
                        "portfolio_sectors_custom_obb",
                        "portfolio_countries_custom_obb",
                        "portfolio_industries_custom_obb",
                        "portfolio_holdings_custom_obb",
                        "portfolio_unit_price_custom_obb",
                        "holdings_correlation_custom_obb",
                        "portfolio_factors_custom_obb",
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
        "widget_config": {
            "refecthInterval": False,
            "name": "F-F Portfolio Data",
        }
    },
)
async def get_load_portfolios(
    region: Annotated[
        str,
        Query(
            description="Region",
            json_schema_extra={
                "x-widget_config": {
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {
                        "get_regions": True,
                        "is_portfolio": True,
                    },
                }
            },
        ),
    ] = "america",
    portfolio: Annotated[
        str,
        Query(
            description="Portfolio set to load.",
            json_schema_extra={
                "x-widget_config": {
                    "style": {"popupWidth": 300},
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {
                        "region": "$region",
                        "is_portfolio": True,
                    },
                }
            },
        ),
    ] = "Portfolios_Formed_on_ME",
    measure: Annotated[
        Literal["Value", "Equal", "Number Of Firms", "Firm Size"],
        Query(description="Data measurement"),
    ] = "Value",
    frequency: Annotated[
        str,
        Query(
            description="Data frequency. Only valid when the dataset is not daily (in the name).",
            json_schema_extra={
                "x-widget_config": {
                    "style": {"popupWidth": 300},
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {
                        "region": "$region",
                        "is_portfolio": True,
                        "portfolio": "$portfolio",
                    },
                }
            },
        ),
    ] = "monthly",
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
    measure = measure.replace(" ", "_").lower()

    parsed_portfolio = (
        [
            v.get("value")
            for v in DATASET_CHOICES
            if v.get("value", "").startswith(portfolio)
            and "daily" in v.get("value").lower()
        ]
        if frequency == "daily"
        else [
            v.get("value")
            for v in DATASET_CHOICES
            if v.get("value", "").startswith(portfolio)
        ]
    )

    if frequency == "daily":
        parsed_portfolio = [d for d in parsed_portfolio if "daily" in d.lower()]
        frequency = None

    try:
        data, meta = get_portfolio_data(parsed_portfolio[0], frequency, measure)
        df = data[0]

        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]
        df = df.astype(float)
        df = df.sort_index()
        DATASETS["loaded_portfolio"] = {"meta": meta[0], "data": df}
        LOADED_PORTFOLIO = portfolio

        return df.reset_index().to_dict(orient="records") if data else []
    except Exception:
        return []


@app.get(
    "/load_factors",
    openapi_extra={
        "widget_config": {
            "refetchInterval": False,
            "name": "F-F Factors Data",
        }
    },
)
async def get_load_factors(
    region: Annotated[
        str,
        Query(
            description="Region",
            json_schema_extra={
                "x-widget_config": {
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {
                        "get_regions": True,
                        "is_portfolio": True,
                    },
                }
            },
        ),
    ] = "america",
    factor: Annotated[
        str,
        Query(
            description="Factor",
            json_schema_extra={
                "x-widget_config": {
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {"region": "$region", "is_portfolio": False},
                },
            },
        ),
    ] = "5_Factors",
    frequency: Annotated[
        str,
        Query(
            description="Data frequency. Only valid when the dataset is not daily or weekly (in the name).",
            json_schema_extra={
                "x-widget_config": {
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {
                        "region": "$region",
                        "factor": "$factor",
                        "is_portfolio": False,
                    },
                },
            },
        ),
    ] = "Monthly",
    start_date: Annotated[
        Optional[str],
        Query(
            description="Start date",
            json_schema_extra={"x-widget_config": {"value": "$currentDate-5y"}},
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

    interval = (
        FACTOR_REGION_MAP.get(region, {})
        .get("intervals", {})
        .get(factor, {})
        .get(frequency)
    )

    factor = FACTOR_REGION_MAP.get(region, {}).get("factors", {}).get(factor, factor)

    factor = factor + interval if interval else factor
    frequency = (
        None if "daily" in factor.lower() or "weekly" in factor.lower() else frequency
    )

    LOADED_FACTORS = factor
    try:
        dfs, meta = get_portfolio_data(factor, frequency)
        df = dfs[0]

        if start_date:
            df = df[df.index >= start_date]

        if end_date:
            df = df[df.index <= end_date]

        df = df.astype(float)
        df = df.sort_index()
        DATASETS["loaded_factors"] = {"meta": meta[0], "data": df}

        return df.reset_index().to_dict(orient="records")
    except Exception:
        return []


@app.get("/get_data_choices", openapi_extra={"widget_config": {"exclude": True}})
async def get_data_choices():
    await asyncio.sleep(2)
    output = [
        {"label": "Client 1", "value": "Client 1"},
        {"label": "Client 2", "value": "Client 2"},
        {"label": "Client 3", "value": "Client 3"},
    ]

    if not LOADED_PORTFOLIO or not DATASETS.get("loaded_portfolio", {}):
        return output

    data = DATASETS.get("loaded_portfolio", {}).get("data")
    if data is not None:
        cols = [k for k in data.columns if k != "Date"]
        output.append([{"label": k, "value": k} for k in cols])

    return output


@app.get(
    "/get_factor_choices",
    openapi_extra={"widget_config": {"exclude": True}},
)
async def get_factor_choices(
    region: str = None,
    factor: str = None,
    is_portfolio: bool = False,
    get_regions: bool = False,
    portfolio: str = None,
):
    if get_regions is True and is_portfolio is True:
        return [
            {"label": k.replace("_", " ").title(), "value": k}
            for k in list(REGIONS_MAP)
        ]
    if region and not factor and not is_portfolio:
        factors = FACTOR_REGION_MAP.get(region, {}).get("factors", {})
        return [
            {
                "label": k.replace("_", " ")
                .title()
                .replace("Lt", "LT")
                .replace("St", "ST"),
                "value": k,
            }
            for k in list(factors)
        ]
    if region and factor and not is_portfolio:
        intervals = (
            FACTOR_REGION_MAP.get(region, {}).get("intervals", {}).get(factor, {})
        )
        intervals = [
            {"label": k.replace("_", " ").title(), "value": k} for k in list(intervals)
        ]
        return intervals

    if not is_portfolio:
        return [
            {"label": k.replace("_", " ").title(), "value": k}
            for k in list(FACTOR_REGION_MAP)
        ]

    mapped_region = REGIONS_MAP.get(region, "").replace("_", " ")
    if is_portfolio and region and not portfolio:

        portfolios = (
            [
                {
                    "label": (
                        d["label"].replace(mapped_region, "").strip()
                        if region != "america"
                        else d["label"]
                    ),
                    "value": d["value"],
                }
                for d in DATASET_CHOICES
                if "Portfolio" in d.get("value")
                and d["value"].startswith(REGIONS_MAP.get(region, region))
                and "daily" not in d.get("value", "").lower()
            ]
            if region != "america"
            else [
                d
                for d in DATASET_CHOICES
                if "Portfolio" in d.get("value")
                and all(
                    not d.get("value", "").startswith(reg)
                    for reg in REGIONS_MAP.values()
                    if reg
                )
                and "daily" not in d.get("value", "").lower()
            ]
        )

        if "ex_" not in region:
            portfolios = [d for d in portfolios if "ex_" not in d.get("value", "")]

        return portfolios

    if is_portfolio and region and portfolio:
        portfolios = (
            [
                d
                for d in DATASET_CHOICES
                if "Portfolio" in d.get("value")
                and d["value"].startswith(REGIONS_MAP.get(region, region))
                and portfolio in d.get("value", "")
            ]
            if region != "america"
            else [
                d
                for d in DATASET_CHOICES
                if "Portfolio" in d.get("value")
                and all(
                    not d.get("value", "").startswith(reg)
                    for reg in REGIONS_MAP.values()
                    if reg
                )
                and portfolio in d.get("value", "")
            ]
        )
        has_daily = False
        frequencies = ["monthly", "annual"]

        for d in portfolios:
            if "daily" in d.get("value", "").lower():
                has_daily = True
                break

        frequencies = ["daily"] + frequencies if has_daily else frequencies

        return [{"label": k.title(), "value": k} for k in frequencies]

    return [{"label": "No choices found. Try a new parameter.", "value": None}]


@app.get(
    "/portfolio_factors",
    openapi_extra={
        "widget_config": {
            "name": "Portfolio Factor Correlations",
            "data": {
                "table": {
                    "showAll": False,
                }
            },
        }
    },
)
async def portfolio_factors(
    store: PortfolioData,
    region: Annotated[
        str,
        Query(
            description="Region for the factor data.",
            json_schema_extra={
                "x-widget_config": {
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {
                        "get_regions": True,
                        "is_portfolio": True,
                    },
                }
            },
        ),
    ] = "america",
    factor: Annotated[
        str,
        Query(
            description="Factor set to use for analysis.",
            json_schema_extra={
                "x-widget_config": {
                    "type": "endpoint",
                    "optionsEndpoint": "/get_factor_choices",
                    "optionsParams": {"region": "$region", "is_portfolio": False},
                },
            },
        ),
    ] = "5_Factors",
    portfolio: Literal["Client 1", "Client 2", "Client 3"] = "Client 1",
) -> list[PortfolioFactors]:
    """Get dataset."""
    global DATASETS, LOADED_FACTORS
    frequency = "daily"
    if not factor:
        return "Please select a factor."

    try:
        # Get client portfolio data
        portfolio_data = store.get_store(f"portfolio_{portfolio[-1]}")
        returns_data = portfolio_data.get("portfolio_returns")

        # Make sure we have portfolio data
        if returns_data is None or returns_data.empty:
            return [{"Error": f"No data found for portfolio {portfolio}"}]

        # Load factor data first to understand its date range
        interval = (
            FACTOR_REGION_MAP.get(region, {})
            .get("intervals", {})
            .get(factor, {})
            .get(frequency)
        )
        factor_name = (
            FACTOR_REGION_MAP.get(region, {}).get("factors", {}).get(factor, factor)
        )
        factor_name = factor_name + interval if interval else factor_name
        factor_freq = (
            None
            if "daily" in factor_name.lower() or "weekly" in factor_name.lower()
            else frequency
        )

        LOADED_FACTORS = factor_name

        dfs, meta = get_portfolio_data(factor_name, factor_freq)
        factor_df = dfs[0]
        # Apply date filtering if specified

        # Convert to float and sort
        factor_df = factor_df.astype(float)
        factor_df = factor_df.sort_index()

        # Get the factor date range to match our portfolio resampling
        factor_min_date = factor_df.index.min()
        factor_max_date = factor_df.index.max()

        portfolio_df = returns_data.to_frame().rename(columns={"close": portfolio})
        portfolio_df.index = portfolio_df.index.astype(str)

        # Filter portfolio data to factor date range
        portfolio_df = portfolio_df[
            (portfolio_df.index >= factor_min_date)
            & (portfolio_df.index <= factor_max_date)
        ]

        portfolio_df = portfolio_df.sort_index()

        # Find the intersection of dates between the two dataframes
        common_dates = portfolio_df.index.intersection(factor_df.index)

        if len(common_dates) == 0:
            return [
                {"Error": "No matching dates found between portfolio and factor data."}
            ]

        # Use only the common dates
        portfolio_df = portfolio_df.loc[common_dates]
        factor_df = factor_df.loc[common_dates]

        factor_df.loc[:, portfolio] = portfolio_df[portfolio].values
        # Store the result
        DATASETS["loaded_factors"] = {"meta": meta[0], "data": factor_df}

        coefficients = []
        added_portfolios = portfolio.split(",") if "," in portfolio else [portfolio]
        timeframes = ["1 Month", "3 Month", "YTD", "1 Year", "3 Year", "Max"]
        if not added_portfolios:
            return [
                {"Error": "Please select target data from the loaded portfolio set."}
            ]
        # Return as records for API response
        factor_cols = [
            k
            for k in factor_df.columns
            if k not in ["Date", "RF"] and k not in added_portfolios
        ]

        # Get current date
        factor_df.index = factor_df.index.astype("datetime64[s]")
        max_date = factor_df.index.max()
        current_year = max_date.year

        for timeframe in timeframes:
            added_port = factor_df.copy()
            if portfolio in added_port.columns and "RF" in factor_df.columns:
                added_port.loc[:, f"{portfolio}"] = (
                    added_port[portfolio].values - added_port["RF"].values
                )

            # Apply period filtering
            if timeframe == "1 Month":
                start_date = max_date - DateOffset(months=1)
                added_port = added_port[added_port.index >= start_date]
            elif timeframe == "3 Month":
                start_date = max_date - DateOffset(months=3)
                added_port = added_port[added_port.index >= start_date]
            elif timeframe == "YTD":
                start_date = Timestamp(f"{current_year}-01-01")
                added_port = added_port[added_port.index >= start_date]
            elif timeframe == "1 Year":
                start_date = max_date - DateOffset(years=1)
                added_port = added_port[added_port.index >= start_date]
            elif timeframe == "3 Year":
                start_date = max_date - DateOffset(years=3)
                added_port = added_port[added_port.index >= start_date]

            model = await perform_ols(
                added_port,
                portfolio,
                factor_cols,
            )
            model = model.round(6)
            model.reset_index(inplace=True)
            model.rename(columns={"index": "factor"}, inplace=True)

            model.loc[:, "period"] = timeframe
            model = model[
                [
                    "period",
                    "factor",
                    "coefficient",
                    "p_value",
                    "lower_ci",
                    "upper_ci",
                ]
            ]
            records = model.convert_dtypes().to_dict(orient="records")
            coefficients.extend(records)

        output = []
        pivoted = DataFrame(coefficients).pivot_table(
            columns="factor",
            index="period",
            values=["coefficient", "p_value", "lower_ci", "upper_ci"],
        )
        coeffs = pivoted["coefficient"].reset_index()
        p_value = pivoted["p_value"].reset_index()
        conf_hi = pivoted["upper_ci"].reset_index()
        conf_low = pivoted["lower_ci"].reset_index()
        idn = 0
        for i in coeffs.index:
            idn += 1
            for col in coeffs.columns[1:].tolist():
                p = round(float(p_value.iloc[i][col]), 6)
                lo = round(float(conf_low.iloc[i][col]), 6)
                hi = round(float(conf_hi.iloc[i][col]), 6)
                coeff = round(float(coeffs.iloc[i][col]), 6)
                output.append(
                    {
                        "period": coeffs.iloc[i, 0],
                        "factor": col,
                        "p_value": p,
                        "lower_ci": lo,
                        "upper_ci": hi,
                        "coefficient": {
                            "value": coeff,
                            "P-Value": f"{p}",
                            "ConfidenceBands": f"{lo} - {hi}",
                        },
                    }
                )
        period_list = ["1 Month", "3 Month", "YTD", "1 Year", "3 Year", "Max"]
        output_df = DataFrame(output)
        periods = [d for d in period_list if d in output_df.period.tolist()]
        final_output = []
        for period in periods:
            row = {}
            row["period"] = period
            period_df = output_df[output_df.period == period]
            factors = period_df.factor.unique().tolist()
            for factor in factors:
                c = period_df[period_df.factor == factor].coefficient.iloc[0]
                p = period_df[period_df.factor == factor].p_value.iloc[0]
                l = period_df[period_df.factor == factor].lower_ci.iloc[0]
                u = period_df[period_df.factor == factor].upper_ci.iloc[0]
                row[f"{factor.lower().replace('-', '_')}_coeff"] = c
                row[f"{factor.lower().replace('-', '_')}_p_value"] = float(p)
                row[f"{factor.lower().replace('-', '_')}_lower_ci"] = float(l)
                row[f"{factor.lower().replace('-', '_')}_upper_ci"] = float(u)
            final_output.append(row)

        return final_output

    except Exception as e:
        print(f"Error loading factor data: {e}")
        return [{"Error": f"Failed to load factor data: {str(e)}"}]


@app.get(
    "/portfolio_holdings",
    openapi_extra={
        "widget_config": {"refetchInterval": False, "name": "Portfolio Holdings"}
    },
)
async def portfolio_holdings(
    store: PortfolioData,
    portfolio: Annotated[
        Literal["Client 1", "Client 2", "Client 3"],
        Query(
            description="Select the portfolio.",
        ),
    ] = "Client 1",
) -> list[HoldingsData]:
    """Get portfolio holdings."""
    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return []

    holdings = port_data.get("holdings")
    holdings.Weight = holdings.Weight * 100
    if holdings is not None:
        return holdings.to_dict(orient="records")
    return []


@app.get(
    "/portfolio_countries",
    openapi_extra={
        "widget_config": {
            "refetchInterval": False,
            "name": "Portfolio Exposure by Country",
        },
    },
)
async def portfolio_countries(
    store: PortfolioData,
    portfolio: Annotated[
        Literal["Client 1", "Client 2", "Client 3"],
        Query(
            description="Select the portfolio.",
        ),
    ] = "Client 1",
) -> list:
    """Get portfolio's exposure by country."""
    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return []

    holdings = port_data.get("country_exposure")
    if holdings is not None:
        return holdings.to_dict(orient="records")
    return []


@app.get(
    "/portfolio_sectors",
    openapi_extra={
        "widget_config": {
            "refetchInterval": False,
            "name": "Portfolio Exposure by Sector",
        },
    },
)
async def portfolio_sectors(
    store: PortfolioData,
    portfolio: Annotated[
        Literal["Client 1", "Client 2", "Client 3"],
        Query(
            description="Select the portfolio.",
        ),
    ] = "Client 1",
) -> list:
    """Get portfolio's exposure by sector."""
    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return []

    holdings = port_data.get("sector_exposure")
    if holdings is not None:
        return holdings.to_dict(orient="records")
    return []


@app.get(
    "/portfolio_industries",
    openapi_extra={
        "widget_config": {
            "refetchInterval": False,
            "name": "Portfolio Exposure by Industry",
        },
    },
)
async def portfolio_industries(
    store: PortfolioData,
    portfolio: Annotated[
        Literal["Client 1", "Client 2", "Client 3"],
        Query(
            description="Select the portfolio.",
        ),
    ] = "Client 1",
) -> list:
    """Get portfolio's exposure by industry."""
    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return []

    holdings = port_data.get("industry_exposure")
    if holdings is not None:
        return holdings.to_dict(orient="records")
    return []


@app.get(
    "/portfolio_unit_price",
    openapi_extra={
        "widget_config": {
            "refetchInterval": False,
            "name": "Portfolio Price Performance",
        },
    },
)
async def portfolio_unit_price(
    store: PortfolioData,
    portfolio: Annotated[
        Literal["Client 1", "Client 2", "Client 3"],
        Query(
            description="Select the portfolio.",
        ),
    ] = "Client 1",
    asset: Annotated[
        str,
        Query(
            description="Select the asset.",
            json_schema_extra={
                "x-widget_config": {
                    "type": "endpoint",
                    "optionsEndpoint": "/get_asset_choices",
                    "optionsParams": {"portfolio": "$portfolio"},
                    "style": {"popupWidth": 400},
                },
            },
        ),
    ] = "Portfolio Units",
    period: Annotated[
        Literal["1 Month", "3 Month", "YTD", "1 Year", "3 Year", "Max"],
        Query(
            description="Select the period.",
        ),
    ] = "1 Year",
    returns: Annotated[
        bool, Query(description="Show returns instead of unit price.")
    ] = False,
) -> list[PriceHistory]:
    """Get portfolio's unit price history."""
    import pandas as pd

    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return []

    holdings = (
        (
            port_data.get("portfolio_returns").to_frame()
            if returns is True
            else port_data.get("portfolio_price_data")
        )
        if asset == "Portfolio Units"
        else (
            port_data.get("underlying_returns").get(asset)
            if returns is True
            else port_data.get("underlying_price_data").get(asset)
        )
    )

    if holdings is None or holdings.empty:
        return []

    if asset != "Portfolio Units":
        holdings = holdings.to_frame().rename(columns={asset: "close"})

    # Ensure index is datetime type
    if not isinstance(holdings.index, pd.DatetimeIndex):
        holdings.index = pd.to_datetime(holdings.index)

    # Sort by date
    holdings = holdings.sort_index()

    # Get current date
    max_date = holdings.index.max()
    current_year = max_date.year

    # Apply period filtering
    if period == "1 Month":
        start_date = max_date - pd.DateOffset(months=1)
        holdings = holdings[holdings.index >= start_date]
    elif period == "3 Month":
        start_date = max_date - pd.DateOffset(months=3)
        holdings = holdings[holdings.index >= start_date]
    elif period == "YTD":
        start_date = pd.Timestamp(f"{current_year}-01-01")
        holdings = holdings[holdings.index >= start_date]
    elif period == "1 Year":
        start_date = max_date - pd.DateOffset(years=1)
        holdings = holdings[holdings.index >= start_date]
    elif period == "3 Year":
        start_date = max_date - pd.DateOffset(years=3)
        holdings = holdings[holdings.index >= start_date]
    # For "Max", we use all data

    # If showing returns, calculate cumulative returns
    if returns is True:
        holdings.close = holdings.close.cumsum()

    # Convert index to string for serialization
    holdings.index = holdings.index.astype(str)

    return holdings.reset_index().to_dict(orient="records")


@app.get(
    "/get_asset_choices",
    openapi_extra={
        "widget_config": {
            "exclude": True,
        },
    },
)
async def get_asset_choices(
    store: PortfolioData,
    portfolio: str = "Client 1",
) -> list:
    """Get asset choices for the portfolio."""
    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return [{"label": "No assets found", "value": None}]

    holdings = port_data.get("holdings")

    symbols = holdings.Symbol.tolist()
    names = holdings.Name.tolist()

    choices = []
    choices.append({"label": "Portfolio Units", "value": "Portfolio Units"})
    for sym, name in zip(symbols, names):
        choices.append({"label": name, "value": sym})

    return choices


@app.get(
    "/portfolio_underlying_returns",
    openapi_extra={
        "widget_config": {
            "refetchInterval": False,
            "name": "Returns of the portfolio's underlying assets",
        },
    },
)
async def portfolio_underlying_returns(
    store: PortfolioData,
    portfolio: Annotated[
        Literal["Client 1", "Client 2", "Client 3"],
        Query(
            description="Select the portfolio.",
        ),
    ] = "Client 1",
) -> list:
    """Get portfolio's returns by underlying asset."""
    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return []

    holdings = port_data.get("underlying_performance")
    if holdings is not None:
        return holdings.to_dict(orient="records")
    return []


@app.get(
    "/holdings_correlation",
    openapi_extra={
        "widget_config": {
            "refetchInterval": False,
            "name": "Portfolio Holdings Correlations",
            "type": "chart",
            "gridData": {"w": 40, "h": 20},
            "params": [{"paramName": "theme", "show": False}],
        },
    },
)
async def holdings_correlation(
    store: PortfolioData,
    portfolio: Annotated[
        Literal["Client 1", "Client 2", "Client 3"],
        Query(
            description="Select the portfolio.",
        ),
    ] = "Client 1",
    timeframe: Annotated[
        Literal["1 Month", "3 Month", "1 Year", "3 Year", "5 Year"],
        Query(
            description="Select the timeframe.",
        ),
    ] = "3 Month",
    theme: Annotated[
        str,
        Query(
            description="Select the theme.",
        ),
    ] = "dark",
) -> dict:
    """Get portfolio holdings."""
    import json

    port_data = store.get_store(f"portfolio_{portfolio[-1]}")
    if not port_data:
        return []

    data = port_data.get("underlying_returns")

    timedelta = (
        21
        if timeframe == "1 Month"
        else 63 if timeframe == "3 Month" else 252 if timeframe == "1 Year" else 252 * 3
    )
    data = data.iloc[-timedelta:]

    if data is not None:
        fig = correlation_matrix(
            data,
            chart=True,
            theme=theme,
        )
        fig_json = json.loads(fig.to_json())

    return fig_json


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
