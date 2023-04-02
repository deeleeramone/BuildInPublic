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
    """Search for Primary Dealer Statistics descriptions. Results are case sensitive.

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

        PDFTR-CS: CORPORATE SECURITIES : DEALER FINANCING FAILS TO RECEIVE

        PDFTD-FGEM: FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) : DEALER FINANCING FAILS TO DELIVER

        PDFTR-FGEM: FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) : DEALER FINANCING FAILS TO RECEIVE

        PDFTD-FGM: FEDERAL AGENCY AND GSE MBS : DEALER FINANCING FAILS TO DELIVER

        PDFTR-FGM: FEDERAL AGENCY AND GSE MBS : DEALER FINANCING FAILS TO RECEIVE

        PDFTD-OM: OTHER MBS : DEALER FINANCING FAILS TO DELIVER

        PDFTR-OM: OTHER MBS : DEALER FINANCING FAILS TO RECEIVE

        PDFTD-UST: TREASURY INFLATION-PROTECTED SECURITIES (TIPS) : DEALER FINANCING FAILS TO DELIVER

        PDFTR-UST: TREASURY INFLATION-PROTECTED SECURITIES (TIPS) : DEALER FINANCING FAILS TO RECEIVE

        PDFTD-USTET: U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS) : DEALER FINANCING FAILS TO DELIVER

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


def get_dealer_position_abs() -> pd.DataFrame:
    """Gets a time series of Primary Dealer Positions, for Asset Backed Securities, by Type.

    "PDPOSABS-ALB" : "ASSET-BACKED SECURITIES: AUTOMOBILE LOAN-BACKED SECURITIES: DEALER POSITION - LONG. - ASSET-BACKED SECURITIES: AUTOMOBILE LOAN-BACKED SECURITIES: DEALER POSITION - SHORT."

    "PDPOSABS-CCB" : "ASSET-BACKED SECURITIES: CREDIT CARD-BACKED SECURITIES: DEALER POSITION - LONG. - ASSET-BACKED SECURITIES: CREDIT CARD-BACKED SECURITIES: DEALER POSITION - SHORT."

    "PDPOSABS-SLB" : "ASSET-BACKED SECURITIES: STUDENT LOAN-BACKED SECURITIES: DEALER POSITION - LONG. - ASSET-BACKED SECURITIES: STUDENT LOAN-BACKED SECURITIES IN SHORT POSITION."

    "PDPOSABS-OAB" : "ASSET-BACKED SECURITIES: OTHER ASSET-BACKED SECURITIES: DEALER POSITION - LONG. - ASSET-BACKED SECURITIES: OTHER ASSET-BACKED SECURITIES: DEALER POSITION - SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame of Dealer Positions for Asset Backed Securities by Type.

    Example
    -------
    >>> dealer_position_abs = get_dealer_position_abs()
    """

    dealer_position_abs = pd.DataFrame()

    automobiles = (
        (fed_api.pds.get_timeseries("PDPOSABS-ALB")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    credit_cards = (
        (fed_api.pds.get_timeseries("PDPOSABS-CCB")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    student_loans = (
        (fed_api.pds.get_timeseries("PDPOSABS-SLB")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    other_assets = (
        (fed_api.pds.get_timeseries("PDPOSABS-OAB")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    abs_cols = ["Automobiles", "Credit Cards", "Student Loans", "Other Assets"]

    dealer_position_abs = pd.concat(
        [automobiles, credit_cards, student_loans, other_assets], axis=1
    )

    dealer_position_abs.columns = abs_cols

    dealer_position_abs["Total ABS"] = (
        dealer_position_abs["Automobiles"]
        + dealer_position_abs["Credit Cards"]
        + dealer_position_abs["Student Loans"]
        + dealer_position_abs["Other Assets"]
    )

    return dealer_position_abs


def get_dealer_position_tips() -> pd.DataFrame:
    """Gets a time series of Primary Dealer Positions, for TIPS, by duration.

    "PDPOSTIPS-L2" : "U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN LESS THAN OR EQUAL TO 2 YEARS : DEALER POSITION - LONG. - U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN LESS THAN OR EQUAL TO 2 YEARS : DEALER POSITION - SHORT."

    "PDPOSTIPS-G2" : "U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN MORE THAN 2 YEARS BUT LESS THAN OR EQUAL TO 6 YEARS : DEALER POSITION - LONG. - U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN MORE THAN 2 YEARS BUT LESS THAN OR EQUAL TO 6 YEARS : DEALER POSITION - SHORT."

    "PDPOSTIPS-G6L11" : "U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN MORE THAN 6 YEARS BUT LESS THAN OR EQUAL TO 11 YEARS : DEALER POSITION - LONG. - U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN MORE THAN 6 YEARS BUT LESS THAN OR EQUAL TO 11 YEARS : DEALER POSITION - SHORT."

    "PDPOSTIPS-G11" : "U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN MORE THAN 11 YEARS : DEALER POSITION - LONG. - U.S. TREASURY INFLATION-PROTECTED SECURITIES (TIPS) DUE IN MORE THAN 11 YEARS : DEALER POSITION - SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame of Dealer Positions for TIPS by duration.

    Example
    -------
    >>> dealer_position_tips = get_dealer_position_tips()
    """

    dealer_position_tips = pd.DataFrame()

    tips_cols = [
        "<= 2 Years",
        "> 2 Years <= 6 Years",
        "> 6 Years <= 11 Years",
        "> 11 Years",
    ]

    two_years = (
        (fed_api.pds.get_timeseries("PDPOSTIPS-L2")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    six_years = (
        (fed_api.pds.get_timeseries("PDPOSTIPS-G2")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    eleven_years = (
        (fed_api.pds.get_timeseries("PDPOSTIPS-G6L11")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    greater_than_eleven = (
        (fed_api.pds.get_timeseries("PDPOSTIPS-G11")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    dealer_position_tips = pd.concat(
        [two_years, six_years, eleven_years, greater_than_eleven], axis=1
    )

    dealer_position_tips.columns = tips_cols

    dealer_position_tips["Total TIPS"] = (
        dealer_position_tips["<= 2 Years"]
        + dealer_position_tips["> 2 Years <= 6 Years"]
        + dealer_position_tips["> 6 Years <= 11 Years"]
        + dealer_position_tips["> 11 Years"]
    )

    return dealer_position_tips


def get_dealer_position_tbills() -> pd.Series:
    """
    Gets a time series of Primary Dealer Positions, for US Treasury Bills.

    "PDPOSGS-B" : "U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) BILLS: DEALER POSITION - LONG. - U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) BILLS : DEALER POSITION - SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.

    Returns
    -------
    pd.Series: Pandas Series of Dealer Position for US Treasury Bills.

    Example
    -------
    >>> dealer_position_tbills = get_dealer_position_tbills()
    """

    dealer_position_tbills = (
        (
            pd.DataFrame(
                data=fed_api.pds.get_timeseries("PDPOSGS-B"),
                columns=["asofdate", "value"],
            )
        )
        .set_index("asofdate")["value"]
        .astype(float)
    )

    return dealer_position_tbills.rename("T-Bills")


def get_dealer_position_coupons() -> pd.DataFrame:
    """
    Gets a time series of Primary Dealer Positions, for US Treasury Seucirities Coupons (ex-TIPS), by duration.

    "PDPOSGSC-L2": "U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN LESS THAN OR EQUAL TO 2 YEARS : DEALER POSITION - LONG. - U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN LESS THAN OR EQUAL TO 2 YEARS : DEALER POSITION - SHORT."

    "PDPOSGSC-G2L3": "U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 2 YEARS BUT LESS THAN OR EQUAL TO 3 YEARS : DEALER POSITION - LONG. - U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 2 YEARS BUT LESS THAN OR EQUAL TO 3 YEARS : DEALER POSITION - SHORT."

    "PDPOSGSC-G3L6" : "U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 3 YEARS BUT LESS THAN OR EQUAL TO 6 YEARS : DEALER POSITION - LONG. - U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 3 YEARS BUT LESS THAN OR EQUAL TO 6 YEARS : DEALER POSITION - SHORT."

    "PDPOSGSC-G6L7" : "U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 6 YEARS BUT LESS THAN OR EQUAL TO 7 YEARS : DEALER POSITION - LONG. - U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 6 YEARS BUT LESS THAN OR EQUAL TO 7 YEARS : DEALER POSITION - SHORT."

    "PDPOSGSC-G7L11" : "U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 7 YEARS BUT LESS THAN OR EQUAL TO 11 YEARS : DEALER POSITION - LONG. - U.S. TREASURY SECURITIES (EXCLUDING TREASURY INFLATION-PROTECTED SECURITIES (TIPS)) COUPONS DUE IN MORE THAN 7 YEARS BUT LESS THAN OR EQUAL TO 11 YEARS : DEALER POSITION - SHORT."

    "PDPOSGSC-G11L21" : "U.S. TREASURY SECURITIES (EXCLUDING TIPS): COUPONS DUE IN MORE THAN 11 YEARS BUT LESS THAN OR EQUAL TO 21 YEARS - OUTRIGHT POSITIONS LONG - U.S. TREASURY SECURITIES (EXCLUDING TIPS): COUPONS DUE IN MORE THAN 11 YEARS BUT LESS THAN OR EQUAL TO 21 YEARS - OUTRIGHT POSITIONS SHORT."

    "PDPOSGSC-G21" : "U.S. TREASURY SECURITIES (EXCLUDING TIPS): COUPONS DUE IN MORE THAN 21 YEARS - OUTRIGHT POSITIONS LONG - U.S. TREASURY SECURITIES (EXCLUDING TIPS): COUPONS DUE IN MORE THAN 21 YEARS - OUTRIGHT POSITIONS SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.
    Where values are 0, the time series has no data for the given duration.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame of Dealer Position for US Treasury Seucirities Coupons (ex-TIPS), by duration.

    Example
    -------
    >>> dealer_position_coupons = get_dealer_position_coupons()
    """

    coupons_cols = [
        "<= 2 Years",
        ">2 Years <= 3 Years",
        ">3 Years <= 6 Years",
        ">6 Years <= 7 Years",
        ">7 Years <= 11 Years",
        ">11 Years <= 21 Years",
        ">21 Years",
    ]

    dealer_position_coupons = pd.DataFrame()

    two_years = (
        (fed_api.pds.get_timeseries("PDPOSGSC-L2")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    three_years = (
        (fed_api.pds.get_timeseries("PDPOSGSC-G2L3")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    six_years = (
        (fed_api.pds.get_timeseries("PDPOSGSC-G3L6")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    seven_years = (
        (fed_api.pds.get_timeseries("PDPOSGSC-G6L7")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    eleven_years = (
        (fed_api.pds.get_timeseries("PDPOSGSC-G7L11")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    twenty_one_years = (
        (fed_api.pds.get_timeseries("PDPOSGSC-G11L21")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    greater_than_21 = (
        (fed_api.pds.get_timeseries("PDPOSGSC-G21")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    dealer_position_coupons = pd.concat(
        [
            two_years,
            three_years,
            six_years,
            seven_years,
            eleven_years,
            twenty_one_years,
            greater_than_21,
        ],
        axis=1,
    ).fillna(0)

    dealer_position_coupons.columns = coupons_cols

    dealer_position_coupons["Total Coupons"] = (
        dealer_position_coupons["<= 2 Years"]
        + dealer_position_coupons[">2 Years <= 3 Years"]
        + dealer_position_coupons[">3 Years <= 6 Years"]
        + dealer_position_coupons[">6 Years <= 7 Years"]
        + dealer_position_coupons[">7 Years <= 11 Years"]
        + dealer_position_coupons[">11 Years <= 21 Years"]
        + dealer_position_coupons[">21 Years"]
    )

    return dealer_position_coupons


def get_dealer_position_frn() -> pd.Series:
    """Gets a time series of Primary Dealer Positioning for Floating Rate Notes.

    "PDPOSGS-BFRN" : "FLOATING RATE NOTES: DEALER POSITION - LONG. - FLOATING RATE NOTES: DEALER POSITION - SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.

    Returns
    -------
    pd.Series: Pandas Series of Dealer Position for Floating Rate Notes.

    Example
    -------
    >>> dealer_position_frn = get_dealer_position_frn()
    """

    dealer_position_frn = (
        pd.DataFrame(fed_api.pds.get_timeseries("PDPOSGS-BFRN")[["asofdate", "value"]])
        .set_index("asofdate")
        .rename(columns={"value": "Floating Rate Notes"})
        .astype(float)
    )

    return dealer_position_frn


def get_dealer_position_discount_notes() -> pd.Series:
    """Gets a time series of Primary Dealer Positioning for Federal Agency Discount Notes (ex-MBS).

    "PDPOSFGS-DN" : "FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) - DISCOUNT NOTES: DEALER POSITION - LONG - FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) - DISCOUNT NOTES: DEALER POSITION - SHORT"

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.

    Returns
    -------
    pd.Series: Pandas Series of Dealer Position for Federal Agency Discount Notes (ex-MBS).

    Example
    -------
    >>> dealer_position_discount_notes = get_dealer_position_discount_notes()
    """

    dealer_position_discount_notes = (
        pd.DataFrame(fed_api.pds.get_timeseries("PDPOSFGS-DN")[["asofdate", "value"]])
        .set_index("asofdate")
        .rename(columns={"value": "Federal Agency Discount Notes (ex-MBS)"})
        .astype(float)
    )

    return dealer_position_discount_notes


def get_dealer_position_agency_coupons() -> pd.Series:
    """Gets a time series of Primary Dealer Positioning for Federal Agency Coupons (ex-MBS).

    "PDPOSFGS-C" : "FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) - COUPONS: DEALER POSITION - LONG - FEDERAL AGENCY AND GSE SECURITIES (EXCLUDING MBS) - COUPONS : DEALER POSITION - SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.

    Returns
    -------
    pd.Series: Pandas Series of Dealer Position for Federal Agency Coupons (ex-MBS).

    Example
    -------
    >>> dealer_position_agency_coupons = get_dealer_position_agency_coupons()
    """

    dealer_position_agency_coupons = (
        pd.DataFrame(fed_api.pds.get_timeseries("PDPOSFGS-C")[["asofdate", "value"]])
        .set_index("asofdate")
        .rename(columns={"value": "Federal Agency Coupons (ex-MBS)"})
        .astype(float)
    )

    return dealer_position_agency_coupons


def get_dealer_position_mbs() -> pd.DataFrame:
    """Gets a time series of Primary Dealer Positioning for Residential and Commercial Mortgage-Backed Securities

    "PDPOSMBSFGS-TBA" : "Mortgage-backed Securities: Federal Agency and GSE MBS: TBAs - Outright Positions, Long. - Mortgage-backed Securities: Federal Agency and GSE MBS: TBAs - Outright Positions, Short."

    "PDPOSMBSFGS-OR" : "MORTGAGE-BACKED SECURITIES - ALL OTHER FEDERAL AGENCY AND GSE RESIDENTIAL MBS: DEALER POSITION - LONG. - MORTGAGE-BACKED SECURITIES - ALL OTHER FEDERAL AGENCY AND GSE RESIDENTIAL MBS: DEALER POSITION - SHORT."

    "PDPOSMBSFGS-ST" : "Mortgage-backed Securities: Federal Agency and GSE MBS: Specified pools - Outright Positions, Long. - Mortgage-backed Securities: Federal Agency and GSE MBS: Specified pools - Outright Positions, Short."

    "PDPOSMBSFGS-C" : "MORTGAGE-BACKED SECURITIES - FEDERAL AGENCY AND GSE CMBS: DEALER POSITION - LONG. - MORTGAGE-BACKED SECURITIES - FEDERAL AGENCY AND GSE CMBS: DEALER POSITION - SHORT."

    "PDPOSMBSNA-R" : "MORTGAGE-BACKED SECURITIES - NON-AGENCY RESIDENTIAL MBS : DEALER POSITION - LONG. - MORTGAGE-BACKED SECURITIES - NON-AGENCY RESIDENTIAL MBS : DEALER POSITION - SHORT."

    "PDPOSMBSNA-O" : "MORTGAGE-BACKED SECURITIES - NON-AGENCY OTHER CMBS : DEALER POSITION - LONG. - MORTGAGE-BACKED SECURITIES - NON-AGENCY OTHER CMBS : DEALER POSITION - SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.
    Where values are 0, the time series has no data for the given duration.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame of Dealer Position for Residential and Commercial Mortgage-Backed Securities.

    Example
    -------
    >>> dealer_position_mbs = get_dealer_position_mbs()
    """

    dealer_position_mbs = pd.DataFrame()

    mbs_cols = [
        "Agency and GSE MBS Residential - Other",
        "Agency and GSE CMBS",
        "Non-Agency MBS Residential",
        "Non-Agency CMBS Other",
        "Agency and GSE MBS Residential - Outright Positions",
        "Agency and GSE MBS Residential - Specified Pools",
    ]

    agency_residential_outrights = (
        (fed_api.pds.get_timeseries("PDPOSMBSFGS-TBA")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    agency_residential_pools = (
        (fed_api.pds.get_timeseries("PDPOSMBSFGS-ST")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    agency_residential_other = (
        (fed_api.pds.get_timeseries("PDPOSMBSFGS-OR")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    non_agency_residential = (
        (fed_api.pds.get_timeseries("PDPOSMBSNA-R")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    agency_cmbs = (
        (fed_api.pds.get_timeseries("PDPOSMBSFGS-C")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    non_agency_cmbs = (
        (fed_api.pds.get_timeseries("PDPOSMBSNA-O")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    dealer_position_mbs = pd.concat(
        [
            agency_residential_other,
            non_agency_residential,
            agency_cmbs,
            non_agency_cmbs,
            agency_residential_outrights,
            agency_residential_pools,
        ],
        axis=1,
    ).fillna(0)

    dealer_position_mbs.columns = mbs_cols

    dealer_position_mbs["Total Residential MBS"] = (
        dealer_position_mbs["Agency and GSE MBS Residential - Other"]
        + dealer_position_mbs["Non-Agency MBS Residential"]
        + dealer_position_mbs["Agency and GSE MBS Residential - Outright Positions"]
        + dealer_position_mbs["Agency and GSE MBS Residential - Specified Pools"]
    )

    dealer_position_mbs["Total Commericial MBS"] = (
        dealer_position_mbs["Agency and GSE CMBS"]
        + dealer_position_mbs["Non-Agency CMBS Other"]
    )

    return dealer_position_mbs


def get_dealer_position_commercial_paper() -> pd.Series:
    """Gets a time series of Primary Dealer Positioning for Commercial Paper.

    "PDPOSCSCP" : "CORPORATE SECURITIES: COMMERCIAL PAPER: DEALER POSITION - LONG. - CORPORATE SECURITIES: COMMERCIAL PAPER: DEALER POSITION - SHORT"

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.

    Returns
    -------
    pd.Series: Pandas Series of Dealer Position for Commercial Paper.

    Example
    -------
    >>> dealer_position_commercial_paper = get_dealer_position_commercial_paper()
    """

    dealer_position_commercial_paper = (
        pd.DataFrame(fed_api.pds.get_timeseries("PDPOSCSCP")[["asofdate", "value"]])
        .set_index("asofdate")
        .rename(columns={"value": "Commercial Paper"})
        .astype(float)
    )

    return dealer_position_commercial_paper


def get_dealer_position_investment_grade() -> pd.DataFrame:
    """Gets a time series of Dealer Positioning for Investment Grade Corporate Bonds, by duration.

    "PDPOSCSBND-L13" : "CORPORATE SECURITIES: INVESTMENT GRADE BONDS, NOTES AND DEBENTURES DUE IN LESS THAN OR EQUAL TO 13 MONTHS: DEALER POSITION - LONG. - CORPORATE SECURITIES: INVESTMENT GRADE BONDS, NOTES AND DEBENTURES DUE IN LESS THAN OR EQUAL TO 13 MONTHS: DEALER POSITION - SHORT."

    "PDPOSCSBND-G13" : "CORPORATE SECURITIES: INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 13 MONTHS BUT LESS THAN OR EQUAL TO 5 YEARS: DEALER POSITION - LONG. - CORPORATE SECURITIES INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 13 MONTHS BUT LESS THAN OR EQUAL TO 5 YEARS: DEALER POSITION - SHORT."

    "PDPOSCSBND-G5L10" : "CORPORATE SECURITIES INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 5 YEARS BUT LESS THAN OR EQUAL TO 10 YEARS: DEALER POSITION - LONG. - CORPORATE SECURITIES INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 5 YEARS BUT LESS THAN OR EQUAL TO 10 YEARS: DEALER POSITION -SHORT."

    "PDPOSCSBND-G10" : "CORPORATE SECURITIES INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 10 YEARS: DEALER POSITION - LONG. - CORPORATE SECURITIES INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 10 YEARS: DEALER POSITION - SHORT."

    All figures are $USD in millions. Data are updated on Thursdays at approximately 4:15 p.m. with the previous week's statistics.
    Where values are 0, the time series has no data for the given duration.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame of Dealer Position for Investment Grade Corporate Bonds, by duration.

    Example
    -------
    >>> dealer_position_investment_grade = get_dealer_position_investment_grade()
    """

    dealer_position_investment_grade = pd.DataFrame()

    investment_grade_cols = [
        "<= 13 Months",
        "> 13 Months < 5 Years",
        "> 5 Years <= 10 Years",
        "> 10 Years",
    ]

    thirteen_months = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-L13")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    five_years = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-G13")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    ten_years = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-G5L10")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    greater_than_ten_years = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-G10")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    dealer_position_investment_grade = pd.concat(
        [thirteen_months, five_years, ten_years, greater_than_ten_years], axis=1
    ).fillna(0)

    dealer_position_investment_grade.columns = investment_grade_cols

    dealer_position_investment_grade["Total Investment Grade Corporate Bonds"] = (
        dealer_position_investment_grade["<= 13 Months"]
        + dealer_position_investment_grade["> 13 Months < 5 Years"]
        + dealer_position_investment_grade["> 5 Years <= 10 Years"]
        + dealer_position_investment_grade["> 10 Years"]
    )

    return dealer_position_investment_grade


def get_dealer_position_junk_grade() -> pd.DataFrame:
    """Gets a time series of Dealer Positioning for Junk Grade Corporate Bonds, by duration.

    "PDPOSCSBND-BELL13" : "CORPORATE SECURITIES BELOW INVESTMENT GRADE DUE IN LESS THAN OR EQUAL TO 13 MONTHS: DEALER POSITION - LONG. - CORPORATE SECURITIES BELOW INVESTMENT GRADE DUE IN LESS THAN OR EQUAL TO 13 MONTHS: DEALER POSITION - SHORT."

    "PDPOSCSBND-BELG13" : "CORPORATE SECURITIES BELOW INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 13 MONTHS BUT LESS THAN OR EQUAL TO 5 YEARS: DEALER POSITION -LONG. - CORPORATE SECURITIES BELOW INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 13 MONTHS BUT LESS THAN OR EQUAL TO 5 YEARS: DEALER POSITION -SHORT."

    "PDPOSCSBND-BELG5L10" : "CORPORATE SECURITIES BELOW INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 5 YEARS BUT LESS THAN OR EQUAL TO 10 YEARS: DEALER POSITION - LONG. - CORPORATE SECURITIES BELOW INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 5 YEARS BUT LESS THAN OR EQUAL TO 10 YEARS: DEALER POSITION -SHORT."

    "PDPOSCSBND-BELG10" : "CORPORATE SECURITIES BELOW INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 10 YEARS: DEALER POSITION - LONG. - CORPORATE SECURITIES BELOW INVESTMENT GRADE BONDS NOTES AND DEBENTURES DUE IN MORE THAN 10 YEARS: DEALER POSITION -SHORT."

    Returns
    -------
    pd.DataFrame: Pandas DataFrame of Dealer Position for Junk Grade Corporate Bonds, by duration.

    Example
    -------
    >>> dealer_position_junk_grade = get_dealer_position_junk_grade()
    """

    dealer_position_junk_grade = pd.DataFrame()

    junk_grade_cols = [
        "<= 13 Months",
        "> 13 Months < 5 Years",
        "> 5 Years <= 10 Years",
        "> 10 Years",
    ]

    thirteen_months = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-BELL13")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    five_years = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-BELG13")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    ten_years = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-BELG5L10")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    greater_than_ten_years = (
        (fed_api.pds.get_timeseries("PDPOSCSBND-BELG10")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    dealer_position_junk_grade = pd.concat(
        [thirteen_months, five_years, ten_years, greater_than_ten_years], axis=1
    ).fillna(0)

    dealer_position_junk_grade.columns = junk_grade_cols

    dealer_position_junk_grade["Total Below Investment Grade Corporate Bonds"] = (
        dealer_position_junk_grade["<= 13 Months"]
        + dealer_position_junk_grade["> 13 Months < 5 Years"]
        + dealer_position_junk_grade["> 5 Years <= 10 Years"]
        + dealer_position_junk_grade["> 10 Years"]
    )

    return dealer_position_junk_grade


def get_dealer_position_state_municipal() -> pd.DataFrame:
    """Gets a time series of Dealer Positioning for State and Municipal Obligations, by duration.

    "PDPOSSMGO-L13" : "STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN LESS THAN OR EQUAL TO 13 MONTHS: DEALER POSITION - LONG. - STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN LESS THAN OR EQUAL TO 13 MONTHS: DEALER POSITION - SHORT."

    "PDPOSSMGO-G13" : "STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN MORE THAN 13 MONTHS BUT LESS THAN OR EQUAL TO 5 YEARS: DEALER POSITION - LONG. - STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN MORE THAN 13 MONTHS BUT LESS THAN OR EQUAL TO 5 YEARS: DEALER POSITION - SHORT."

    "PDPOSSMGO-G5L10" : "STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN MORE THAN 5 YEARS BUT LESS THAN OR EQUAL TO 10 YEARS: DEALER POSITION - LONG. - STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN MORE THAN 5 YEARS BUT LESS THAN OR EQUAL TO 10 YEARS: DEALER POSITION - SHORT."

    "PDPOSSMGO-G10" : "STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN MORE THAN 10 YEARS: DEALER POSITION - LONG. - STATE AND MUNICIPAL GOVERNMENT OBLIGATIONS DUE IN MORE THAN 10 YEARS: DEALER POSITION - SHORT."

    Returns
    -------
    pd.DataFrame: Pandas DataFrame of Dealer Position for State and Municipal Obligations, by duration.

    Example
    -------
    >>> dealer_position_state_municipal = get_dealer_position_state_municipal()
    """

    dealer_position_state_municipal = pd.DataFrame()

    state_municipal_cols = [
        "<= 13 Months",
        "> 13 Months < 5 Years",
        "> 5 Years <= 10 Years",
        "> 10 Years",
    ]

    thirteen_months = (
        (fed_api.pds.get_timeseries("PDPOSSMGO-L13")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    five_years = (
        (fed_api.pds.get_timeseries("PDPOSSMGO-G13")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    ten_years = (
        (fed_api.pds.get_timeseries("PDPOSSMGO-G5L10")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    greater_than_ten_years = (
        (fed_api.pds.get_timeseries("PDPOSSMGO-G10")[["asofdate", "value"]])
        .set_index("asofdate")["value"]
        .astype(float)
    )

    dealer_position_state_municipal = pd.concat(
        [thirteen_months, five_years, ten_years, greater_than_ten_years], axis=1
    ).fillna(0)

    dealer_position_state_municipal.columns = state_municipal_cols

    dealer_position_state_municipal["Total State and Municipal Obligations"] = (
        dealer_position_state_municipal["<= 13 Months"]
        + dealer_position_state_municipal["> 13 Months < 5 Years"]
        + dealer_position_state_municipal["> 5 Years <= 10 Years"]
        + dealer_position_state_municipal["> 10 Years"]
    )

    return dealer_position_state_municipal
