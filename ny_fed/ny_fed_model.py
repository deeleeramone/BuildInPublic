"""Model for NY Fed Data"""

from typing import Optional
import pandas as pd
from rest_api import RestAPI

__docstyle__ = "numpy"

fed_api = RestAPI()


# Filterable DataFrame by description of Primary Dealer Statistics Time Series
PDS_DESCRIPTIONS = (
    pd.DataFrame.from_dict(fed_api.pds.list_descriptions())
    .reset_index()
    .rename(columns={"index": "Series ID", "description": "Description"})
    .set_index("Description")
)


def search_pds_descriptions(description: str = "") -> pd.DataFrame:
    """Search for PDS descriptions and return corresponding Series IDs. Results are case sensitive.

    Parameters
    ----------
    description : str = ""
        Keywords to search for. Results are case sensitive.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame with Series IDs and descriptions.

    Examples
    --------
    >>> results = search_pds_descriptions(description = "FAILS")

    >>> results = search_pds_descriptions(description = "ASSET-BACKED")
    """

    results = PDS_DESCRIPTIONS.filter(like=description, axis=0)
    return results


def get_pds_fails(
    ftd: Optional[bool] = False,
    ftr: Optional[bool] = False,
    asset_type: Optional[str] = "",
    weights: Optional[bool] = False,
    passthrough: Optional[bool] = False,
) -> pd.DataFrame:
    """Primary Dealer Statistics for Fails to Deliver and Fails to Receive.

    Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.
    All figures are in millions of USD.  The Series IDs used in this function are:

        PDFTD-CS: CORPORATE SECURITIES : DEALER FINANCING FAILS TO DELIVER

        PDFTD-FGEM: FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) : DEALER FINANCING FAILS TO DELIVER

        PDFTD-FGM: FEDERAL AGENCY AND GSE MBS : DEALER FINANCING FAILS TO DELIVER

        PDFTD-OM: OTHER MBS : DEALER FINANCING FAILS TO DELIVER

        PDFTD-UST: TREASURY INFLATION-PROTECTED SECURITIES (TIPS) : DEALER FINANCING FAILS TO DELIVER

        PDFTD-USTET: U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS) : DEALER FINANCING FAILS TO DELIVER

        PDFTR-CS: CORPORATE SECURITIES : DEALER FINANCING FAILS TO RECEIVE

        PDFTR-FGEM: FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) : DEALER FINANCING FAILS TO RECEIVE

        PDFTR-FGM: FEDERAL AGENCY AND GSE MBS : DEALER FINANCING FAILS TO RECEIVE

        PDFTR-OM: OTHER MBS : DEALER FINANCING FAILS TO RECEIVE

        PDFTR-UST: TREASURY INFLATION-PROTECTED SECURITIES (TIPS) : DEALER FINANCING FAILS TO RECEIVE

        PDFTR-USTET: U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS) : DEALER FINANCING FAILS TO RECEIVE

    For research on the topic, see: https://www.federalreserve.gov/econres/notes/feds-notes/the-systemic-nature-of-settlement-fails-20170703.html

    "Large and protracted settlement fails are believed to undermine the liquidity and well-functioning of securities markets.
    Near-100 percent pass-through of fails suggests a high degree of collateral re-hypothecation together with the inability or unwillingness to borrow or buy the needed securities.
    Dealer-specific pass-through from fails to receive to fails to deliver is around 90% to 100% across collateral types and time periods."

    Parameters
    ----------
    ftd: Optional[bool] = False
        Flag to return only delivery fails.
    ftr: Optional[bool] = False
        Flag to return only receive fails.
    asset_type: Optional[str] = ""
        Returns fails-to-deliver and fails-to-receive for the specified asset class. This parameter is overriden by setting, ftd or ftr, as true.
    weights: Optional[bool] = False
        Flag to return the percent value weighting for each asset class against the total.
        This parameter must be used in confunction with, ftd or ftr, and when activated, overrides the asset_type parameter.
    passthrough: Optional[bool] = False
        Flag to return the passthrough rate, the ratio of fails-to-deliver as a percentage of fails-to-receive.
        This parameter overrides the weights and asset_type parameters.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame with results.

    Examples
    --------
    >>> pds_fails = get_pds_fails()

    Deliver and Receive Fails can be separated by setting, ftd or ftr, as true.

    >>> pds_ftds = get_pds_fails(ftd = True)

    The proportional weighting of each asset class is returned by setting, weights = True .

    >>> pds_ftds = get_pds_fails(ftd = True, weights = True)

    The ratio of fails-to-deliver as a percentage of fails-to-receive is returned by setting, passthrough = True.

    >>> pds_ftds = get_pds_fails(passthrough = True)
    """

    ASSET_CLASSES = [
        "US Treasuries",
        "Agency Notes and Coupons",
        "MBS",
        "Corporate Securities",
    ]

    all_data = (
        fed_api.pds.all_timeseries()
        .set_index("As Of Date")
        .replace("*", "0")
        .sort_index()
    )

    ftd_cols = [
        "FTDs - Corporate Securities",
        "FTDs - Agency and GSE Securities (Ex-MBS)",
        "FTDs - Agency and GSE MBS",
        "FTDs - Other MBS",
        "FTDs - TIPS",
        "FTDs - Treasury Securities (Ex-TIPS)",
    ]

    ftr_cols = [
        "FTRs - Corporate Securities",
        "FTRs - Agency and GSE Securities (Ex-MBS)",
        "FTRs - Agency and GSE MBS",
        "FTRs - Other MBS",
        "FTRs - TIPS",
        "FTRs - Treasury Securities (Ex-TIPS)",
    ]

    weights_cols = [
        "Corporate Securities (%)",
        "Agency and GSE Securites (Ex-MBS) (%)",
        "Agency and GSE MBS (%)",
        "Other MBS (%)",
        "TIPS (%)",
        "Treasury Securities (Ex-TIPS) (%)",
    ]

    data = pd.DataFrame()
    data["FTDs - Corporate Securities"] = all_data.query("`Time Series` == 'PDFTD-CS'")[
        "Value (millions)"
    ].astype(float)
    data["FTRs - Corporate Securities"] = all_data.query("`Time Series` == 'PDFTR-CS'")[
        "Value (millions)"
    ].astype(float)
    data["FTDs - Agency and GSE Securities (Ex-MBS)"] = all_data.query(
        "`Time Series` == 'PDFTD-FGEM'"
    )["Value (millions)"].astype(float)
    data["FTRs - Agency and GSE Securities (Ex-MBS)"] = all_data.query(
        "`Time Series` == 'PDFTR-FGEM'"
    )["Value (millions)"].astype(float)
    data["FTDs - Agency and GSE MBS"] = all_data.query("`Time Series` == 'PDFTD-FGM'")[
        "Value (millions)"
    ].astype(float)
    data["FTRs - Agency and GSE MBS"] = all_data.query("`Time Series` == 'PDFTR-FGM'")[
        "Value (millions)"
    ].astype(float)
    data["FTDs - Other MBS"] = all_data.query("`Time Series` == 'PDFTD-OM'")[
        "Value (millions)"
    ].astype(float)
    data["FTRs - Other MBS"] = all_data.query("`Time Series` == 'PDFTR-OM'")[
        "Value (millions)"
    ].astype(float)
    data["FTDs - TIPS"] = all_data.query("`Time Series` == 'PDFTD-UST'")[
        "Value (millions)"
    ].astype(float)
    data["FTRs - TIPS"] = all_data.query("`Time Series` == 'PDFTR-UST'")[
        "Value (millions)"
    ].astype(float)
    data["FTDs - Treasury Securities (Ex-TIPS)"] = all_data.query(
        "`Time Series` == 'PDFTD-USTET'"
    )["Value (millions)"].astype(float)
    data["FTRs - Treasury Securities (Ex-TIPS)"] = all_data.query(
        "`Time Series` == 'PDFTR-USTET'"
    )["Value (millions)"].astype(float)

    if ftd and ftr:
        print("Error: Please choose only one of ftd or ftr.")
        return

    if passthrough:
        new_data = pd.DataFrame(columns=weights_cols)
        new_data["Corporate Securities (%)"] = round(
            (data["FTDs - Corporate Securities"] / data["FTRs - Corporate Securities"])
            * 100,
            ndigits=2,
        )
        new_data["Agency and GSE Securites (Ex-MBS) (%)"] = round(
            (
                data["FTDs - Agency and GSE Securities (Ex-MBS)"]
                / data["FTRs - Agency and GSE Securities (Ex-MBS)"]
            )
            * 100,
            ndigits=2,
        )
        new_data["Agency and GSE MBS (%)"] = round(
            (data["FTDs - Agency and GSE MBS"] / data["FTRs - Agency and GSE MBS"])
            * 100,
            ndigits=2,
        )
        new_data["Other MBS (%)"] = round(
            (data["FTDs - Other MBS"] / data["FTRs - Other MBS"]) * 100, ndigits=2
        )
        new_data["TIPS (%)"] = round(
            (data["FTDs - TIPS"] / data["FTRs - TIPS"]) * 100, ndigits=2
        )
        new_data["Treasury Securities (Ex-TIPS) (%)"] = round(
            (
                data["FTDs - Treasury Securities (Ex-TIPS)"]
                / data["FTRs - Treasury Securities (Ex-TIPS)"]
            )
            * 100,
            ndigits=2,
        )
        return new_data

    if ftd:
        data = data[ftd_cols].copy()
        data["FTDs - Sum Total"] = data.sum(axis=1)
        if weights:
            data = round(data.div(data["FTDs - Sum Total"], axis=0) * 100, ndigits=2)
            data = data.drop(columns=["FTDs - Sum Total"])
            data.columns = weights_cols
            return data

        return data

    if ftr:
        data = data[ftr_cols].copy()
        data["FTRs - Sum Total"] = data.sum(axis=1)
        if weights:
            data = round(data.div(data["FTRs - Sum Total"], axis=0) * 100, ndigits=2)
            data = data.drop(columns=["FTRs - Sum Total"])
            data.columns = weights_cols
            return data

        return data

    if asset_type and asset_type != "":
        if asset_type not in ASSET_CLASSES:
            print("Invalid choice. Choose from: ", ASSET_CLASSES)
            return

    if asset_type and asset_type == "Corporate Securities":
        data = data.iloc[:, 0:2]

    if asset_type and asset_type == "Agency Notes and Coupons":
        return data.iloc[:, 2:4]

    if asset_type and asset_type == "Agency MBS":
        return data.iloc[:, 4:8]

    if asset_type and asset_type == "US Treasuries":
        return data.iloc[:, 8:12]

    return data
