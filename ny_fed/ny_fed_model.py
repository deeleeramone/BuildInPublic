"""Model for NY Fed Data"""

from typing import Optional
import pandas as pd
from ny_fed.rest_api import RestAPI

__docstyle__ = "numpy"

fed_api = RestAPI()


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

    data = fed_api.pds.all_timeseries()
    pds_fails = pd.DataFrame()

    pds_fails_d = (
        pd.DataFrame(
            data
            .query("`Time Series` == @series_d")
        )
        .set_index('As Of Date')['Value (millions)']
        .replace('*', '0')
    )

    pds_fails_r = (
        pd.DataFrame(
            data
            .query("`Time Series` == @series_r")
        )
        .set_index('As Of Date')['Value (millions)']
        .replace('*', '0')
    )

    pds_fails["Deliver (millions)"] = pds_fails_d.astype(float)
    pds_fails["Receive (millions)"] = pds_fails_r.astype(float)
    pds_fails.index = pd.DatetimeIndex(pds_fails.index, yearfirst = True)

    return pds_fails
