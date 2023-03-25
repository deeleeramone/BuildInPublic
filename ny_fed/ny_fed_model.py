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
    .rename(columns={'index': 'Series ID', 'description': 'Description'})
    .set_index('Description')
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

    results = PDS_DESCRIPTIONS.filter(like = description, axis = 0)
    return results

def get_pds_fails(
    change: Optional[bool] = False     
) -> pd.DataFrame:
    """Gets a the Primary Dealer Statistics Time Series for Fails to Deliver and Fails to Receive.

    If change is True, it will return the change from the previous week. All values are in millions of USD.

        PDSI5F-TD: FAILS TO DELIVER
        PDSI5F-TR: FAILS TO RECEIVE 

        PDSI5F-TDC: FAILS TO DELIVER - Change From Previous Week
        PDSI5F-TRC: FAILS TO RECEIVE - Change From Previous Week 

    Parameters
    ----------
    change: Optional[bool] = False
        Flag to return change from previous week.

    Returns
    -------
    pd.DataFrame: Pandas DataFrame with values in millions of USD.

    Example
    -------
    >>> pds_fails = get_pds_fails()
    """

    if change:
        series_d = "PDSI5F-TDC"
        series_r = "PDSI5F-TRC"
    else:
        series_d = "PDSI5F-TD"
        series_r = "PDSI5F-TR"

    pds_fails = pd.DataFrame()

    pds_fails_d = (
        pd.DataFrame(
            fed_api.pds.get_timeseries(series_d)
        ).rename(columns = {'asofdate': 'As Of Date', 'value': 'Value'})
        .set_index('As Of Date')['Value'].replace('*', '0')
    )

    pds_fails_r = (
        pd.DataFrame(
            fed_api.pds.get_timeseries(series_r)
        )
        .rename(columns = {'asofdate': 'As Of Date', 'value': 'Value'})
        .set_index('As Of Date')['Value']
        .replace('*', '0')
    )

    pds_fails["Failures to Deliver (Millions of USD)"] = pds_fails_d.astype(float)
    pds_fails["Failures to Receive (Millions of USD)"] = pds_fails_r.astype(float)
    pds_fails.index = pd.DatetimeIndex(pds_fails.index, yearfirst = True)

    return pds_fails
