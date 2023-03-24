"""Python Connector for NY Federal Reserve RESTful API"""

from typing import Optional
import pandas as pd
from datetime import datetime
from datetime import timedelta

__docformat__ = "numpy"

BASE_URL = "https://markets.newyorkfed.org/api"

OPERATION_STATUS = ["announcements", "results"]
DETAILS = ["summary", "details"]

GUIDE_SHEET_TYPES = ["si", "wi", "fs"]

AMBS_OPERATIONS = ["all", "purchases", "sales", "roll", "swap"]
AMBS_SECURITIES = {
    "None": "",
    "Basket":"Basket",
    "Coupon Swap":"Coupon%20Swap",
    "Dollar Roll":"Dollar%20Roll",
    "Specified Pool":"Specified%20Pool",
    "TBA":"TBA"
}

FXS_OPERATION_TYPES = ["all", "usdollar", "nonusdollar"]
FXS_DATE_TYPES = ["all", "trade", "maturity"]

REFERENCE_RATE_TYPES = ["rate", "volume"]
SECURED_RATE_TYPES = ["tgcr", "bgcr", "sofr", "sofrai"]
UNSECURED_RATE_TYPES = ["effr", "obfr"]

REPO_OPERATION_TYPES = ["all", "repo", "reverserepo"]
REPO_OPERATION_METHODS = ["all", "fixed", "single", "multiple"]
REPO_SECURITY_TYPES = ["mbs", "agency", "tsy", "srf"]
REPO_TERM_TYPES = ["overnight", "term"]
LENDING_OPERATION_TYPES = ["all", "seclending", "extensions"]
AGENCY_HOLDING_TYPES = {
    "all":"all",
    "agency debts":"agency%20debts",
    "mbs":"mbs",
    "cmbs":"cmbs"
}
TREASURY_HOLDING_TYPES = ["all", "bills", "notesbonds", "frn", "tips"]
TREASURY_OPERATION_TYPES = ["all", "purchases", "sales"]
TREASURY_STATUS_TYPES = ["announcements", "results", "operations"]
TREASURY_SECURITY_TYPE = ["agency", "treasury"]

def get_as_of_dates() -> list:
    """Gets a list of valid SOMA As Of Dates

    Returns
    -------
    list: List of valid As Of Dates.

    Example
    -------
    asof_dates = get_as_of_dates()
    """
    url = get_endpoints()["System Open Market Account Holdings"]["list_asof"]

    dates = pd.read_json(url)["soma"]["asOfDates"]

    return dates

def get_endpoints(
    start_date: Optional[str] = "",
    end_date: Optional[str] = "",
    date: Optional[str] = '2022-02-22',
    details: Optional[str] = "details",
    n_operations: Optional[int] = 90,
    operation_status: Optional[str] = "results",
    ambs_operation: Optional[str] = "all",
    ambs_security: Optional[str] = "",
    fxs_operation_type: Optional[str] = "all",
    fxs_date_type: Optional[str] = "",
    fxs_counterparties: Optional[str] = "",
    guide_sheet_types: Optional[str] = "si",
    is_previous: Optional[bool] = False,
    pd_seriesbreak: Optional[str] = "SBN2022",
    pd_timeseries: Optional[str] = "PDSOOS-ABSTOT",
    pd_asof_date: Optional[str] = "2023-03-01",
    rate_type: Optional[str] = "",
    secured_type: Optional[str] = "sofr",
    unsecured_type: Optional[str] = "effr",
    repo_security_type: Optional[str] = "all",
    repo_operation_type: Optional[str] = "all",
    repo_operation_method: Optional[str] = "all",
    repo_term: Optional[str] = "",
    lending_operation: Optional[str] = "all",
    cusips: Optional[str] = "",
    description: Optional[str] = "",
    agency_holding_type: Optional[str] = "all",
    treasury_holding_type: Optional[str] = "all",
    treasury_operation: Optional[str] = "all",
    treasury_status: Optional[str] = "results",
    treasury_security_type: Optional[str] = "",
) -> dict:
    """This function is for generating the URL to the endpoint, it is meant for internal processing."""
    is_latest: str = "latest"
    
    if ambs_security != "":
        ambs_security = AMBS_SECURITIES[ambs_security]
    
    if is_previous:
        is_latest = "previous" if is_previous else "latest"

    END_POINTS = {
        "Agency Mortgage-Backed Securities Operations": {
            "latest": 
                BASE_URL
                +"/ambs/"
                f"{ambs_operation}""/"
                f"{operation_status}""/"
                f"{details}"
                "/latest.json",
            "previous": 
                BASE_URL
                +"/ambs/"
                f"{ambs_operation}""/"
                f"{operation_status}""/"
                f"{details}"
                "/previous.json",
            "lastTwoWeeks": 
                BASE_URL
                +"/ambs/"
                f"{ambs_operation}""/"
                f"{operation_status}""/"
                f"{details}"
                "/lastTwoWeeks.json",
            "last": 
                BASE_URL
                +"/ambs/"
                f"{ambs_operation}""/"
                f"{operation_status}""/"
                f"{details}"
                "/last/"
                f"{n_operations}"
                ".json",
            "search": 
                BASE_URL
                +"/ambs/"
                f"{ambs_operation}""/"
                f"{operation_status}""/"
                f"{details}"
                "/search.json?"
                "securities="f"{ambs_security}"
                "&desc="f"{description}"
                "&cusip="f"{cusips}"
                "&startDate="f"{start_date}"
                "&endDate="f"{end_date}"
        },
        "Central Bank Liquidty Swaps Operations": {
            "latest":
                BASE_URL
                +"/fxs/"
                f"{fxs_operation_type}"
                "/latest.json",
            "last":
                BASE_URL
                +"/fxs/"
                f"{fxs_operation_type}"
                "/last/"
                f"{n_operations}"
                ".json",
            "search":
                BASE_URL
                +"/fxs/"
                f"{fxs_operation_type}"
                "/search.json"
                "?startDate="f"{start_date}"
                "&endDate="f"{end_date}"
                "&dateType="f"{fxs_date_type}"
                "&counterparties="f"{fxs_counterparties}",
            "counterparties":
                BASE_URL
                +"/fxs/list/counterparties.json"            
        },
        "Guide Sheets": 
            BASE_URL
            +"/guidesheets/"
            f"{guide_sheet_types}""/"
            f"{is_latest}"
            ".json",
        "Primary Dealer Statistics": {
            "latest":
                BASE_URL
                +"/pd/latest/"
                f"{pd_seriesbreak}"
                ".json",
            "all_timeseries":
                BASE_URL
                +"/pd/get/all/timeseries.csv",
            "list_descriptions":
                BASE_URL
                +"/pd/list/timeseries.json",
            "list_asof":
                BASE_URL
                +"/pd/list/asof.json",
            "list_seriesbreaks":
                BASE_URL
                +"/pd/list/seriesbreaks.json",
            "get_asof":
                BASE_URL
                +"/pd/get/asof/"
                f"{pd_asof_date}"
                ".json",
            "get_timeseries":
                BASE_URL
                +"/pd/get/"
                f"{pd_timeseries}"
                ".json",
            "get_timeseries_seriesbreak":
                BASE_URL
                +"/pd/get/"
                f"{pd_seriesbreak}"
                "/timeseries/"
                f"{pd_timeseries}"
                ".json",
        },
        "Primary Dealer Market Share": {
            "quarterly":
                BASE_URL
                +"/marketshare/qtrly/latest.xlsx",
            "ytd":
                BASE_URL
                +"/marketshare/ytd/latest.xlsx",
        },
        "Reference Rates": {
            "latest":
                BASE_URL
                +"/rates/all/latest.json",
            "search":
                BASE_URL
                +"/rates/all/search.json?"
                "startDate="f"{start_date}"
                "&endDate="f"{end_date}"
                "&type="f"{rate_type}",
            "latest_secured":
                BASE_URL
                +"/rates/secured/all/latest.json",
            "latest_unsecured":
                BASE_URL
                +"/rates/unsecured/all/latest.json",
            "last_secured":
                BASE_URL
                +"/rates/secured/"
                f"{secured_type}"
                "/last/"
                f"{n_operations}"
                ".json",
            "last_unsecured":
                BASE_URL
                +"/rates/unsecured/"
                f"{unsecured_type}"
                "/last/"
                f"{n_operations}"
                ".json"
        },
        "Repo and Reverse Repo Operations": {
            "latest":
                BASE_URL
                +"/rp/"
                f"{repo_operation_type}"
                "/"f"{repo_operation_method}"
                "/"f"{operation_status}"
                "/latest.json",
            "lastTwoWeeks":
                BASE_URL
                +"/rp/"
                f"{repo_operation_type}"
                "/"f"{repo_operation_method}"
                "/"f"{operation_status}"
                "/lastTwoWeeks.json",
            "last":
                BASE_URL
                +"/rp/"
                f"{repo_operation_type}"
                "/"f"{repo_operation_method}"
                "/"f"{operation_status}"
                "/last/"
                f"{n_operations}"".json",
            "search":
                BASE_URL
                +"/rp/results/search.json?"
                "startDate="f"{start_date}"
                "&endDate="f"{end_date}"
                "&operationTypes="f"{repo_operation_type}"
                "&method="f"{repo_operation_method}"
                "&securityType="f"{repo_security_type}"
                "&term="f"{repo_term}",
            "propositions":
                BASE_URL
                +"/rp/reverserepo/propositions/search.json?"
                "startDate="f"{start_date}"
                "&endDate="f"{end_date}"
        },
        "Securities Lending Operations": {
            "latest":
                BASE_URL
                +"/seclending/"
                f"{lending_operation}"
                "/results/"
                f"{details}"
                "/latest.json",
            "lastTwoWeeks":
                BASE_URL
                +"/seclending/"
                f"{lending_operation}"
                "/results/"
                f"{details}"
                "/lastTwoWeeks.json",
            "last":
                BASE_URL
                +"/seclending/"
                f"{lending_operation}"
                "/results/"
                f"{details}"
                "/last/"
                f"{n_operations}"
                ".json",
            "search":
                BASE_URL
                +"/seclending/"
                f"{lending_operation}"
                "/results/"
                f"{details}"
                "/search.json"
                "?startDate="f"{start_date}"
                "&endDate="f"{end_date}"
                "&cusips="f"{cusips}"
                "&descriptions="f"{description}"
        },
        "System Open Market Account Holdings": {
            "summary":
                BASE_URL
                +"/soma/summary.json",
            "release_log":
                BASE_URL
                +"/soma/agency/get/release_log.json",
            "list_asof":
                BASE_URL
                +"/soma/asofdates/list.json",
            "get_asof":
                BASE_URL
                +"/soma/agency/get/asof/"
                f"{date}"".json",
            "get_cusip":
                BASE_URL
                +"/soma/agency/get/cusip/"
                f"{cusips}"".json",
            "get_holding_type":
                BASE_URL
                +"/soma/agency/get/"
                f"{agency_holding_type}"
                "/asof/"
                f"{date}"
                ".json",
            "agency_debts":
                BASE_URL
                +"/soma/agency/wam/agency%20debts/asof/"
                f"{date}"
                ".json",
            "list_release_dates":
                BASE_URL
                +"/soma/tsy/get/release_log.json",
            "get_treasury_asof":
                BASE_URL
                +"/soma/tsy/get/asof/"
                f"{date}"
                ".json",
            "get_treasury_cusip":
                BASE_URL
                +"/soma/tsy/get/cusip/"
                f"{cusips}"
                ".json",
            "get_treasury_holding_type":
                BASE_URL
                +"/soma/tsy/get/"
                f"{treasury_holding_type}"
                "/asof/"
                f"{date}"
                ".json",
            "get_treasury_debts":
                BASE_URL
                +"/soma/tsy/wam/"
                f"{treasury_holding_type}"
                "/asof/"
                f"{date}"
                ".json",
            "get_treasury_monthly":
                BASE_URL
                +"/soma/tsy/get/monthly.json",
        },
        "Treasury Securities Operations": {
            "current":
                BASE_URL
                +"/tsy/"
                f"{treasury_operation}"
                "/"f"{treasury_status}"
                "/"f"{details}"
                "/latest.json",
            "lastTwoWeeks":
                BASE_URL
                +"/tsy/"
                f"{treasury_operation}"
                "/results/"
                f"{details}"
                "/lastTwoWeeks.json",
            "last":
                BASE_URL
                +"/tsy/"
                f"{treasury_operation}"
                "/results/"
                f"{details}"
                "/last/"
                f"{n_operations}"
                ".json",
            "search":
                BASE_URL
                +"/tsy/"
                f"{treasury_operation}"
                "/results/"
                f"{details}"
                "/search.json?"
                "startDate="f"{start_date}"
                "&endDate="f"{end_date}"
                "&securityType="f"{treasury_security_type}"
                "&cusip="f"{cusips}"
                "&desc="f"{description}"
        }
    }
    return END_POINTS


def guide_sheets(
    guide_sheet_type: Optional[str] = "si",
    previous: Optional[bool] = False,
) -> dict:
    """Returns the latest, or previous, Guide Sheet. FR 2004SI, FR 2004WI, FR 2004F-Series.
    
    Parameters
    ----------
    guide_sheet_type: Optional[str] = "si"
        The guide sheet type. Choices are: ["si", "wi", "fs"]
    previous: Optional[bool] = False
        Whether to return the previous guide sheet.

    Returns
    -------
    dict: Dictionary of results.
    
    Example
    -------
    >>> guide_sheet = guide_sheets(guide_sheet_type = "wi", previous = True)
    """

    if guide_sheet_type not in GUIDE_SHEET_TYPES:
        print("Invalid choice. Choose from: ", GUIDE_SHEET_TYPES)
        return
    
    url = get_endpoints(
        guide_sheet_types = guide_sheet_type,
        is_previous = previous
    )["Guide Sheets"]

    guide_sheet = pd.read_json(url)['guidesheet'][f"{guide_sheet_type}"]
    
    if guide_sheet is not None:
        return guide_sheet
    
    else:
        print("No results returned.")
        return 


class AgencyMBSOperations:
    """Class for Agency Mortgage-Backed Securities Operations

    Attributes
    ----------
    ambs.latest: Function for the latest AMBS operation announcements or results for the current day. 
            Returns: Dictionary

    ambs.lastTwoWeeks: Function for the latest AMBS operation announcements or results for the last two weeks.
            Returns: Dictionary

    ambs.last: Function for the latest AMBS operation announcements or results for the last n operations.
            Returns: Dictionary

    ambs.search: Function to search for AMBS operation announcements or results within the last two years.
            Returns: Dictionary

    Examples
    --------
    >>> mbs = AgencyMBSOperations()
    
    >>> df = mbs.lastTwoWeeks(include = 'details')
    """
    def __init__(self) -> None:
        self.latest
        self.lastTwoWeeks
        self.last
        self.search


    def latest(
        self,
        operation: Optional[str] = "all",
        status: Optional[str] = "results",
        include: Optional[str] = "summary",
        previous: Optional[bool] = False
    ) -> dict:
        """Returns the latest AMBS operation announcements or results for the current day.

        Parameters
        ----------
        operation: Optional[str] = "all"
            The AMBS operation. Choices are: ["all", "purchases", "sales", "roll", "swap"]
        status: Optional[str] = "results"
            The AMBS operation status. Choices are: ["results", "announcements"]
        include: Optional[str] = "summary"
            The level of details to get. Choices are: ["details", "summary"]
        previous: Optional[bool] = False
            Whether to get the previous day's AMBS operation announcements or results.

        Returns
        -------
        dict: Dictionary with the results or announcements of the current day.

        Example
        -------
        >>> mbs = AgencyMBSOperations()
        >>> df = mbs.get_latest()

        """

        if operation not in AMBS_OPERATIONS:
            print("The operation is not supported. Choices are: ", AMBS_OPERATIONS)
            return

        if status not in OPERATION_STATUS:
            print("Invalid status. Choices are: ", OPERATION_STATUS)
            return

        if include not in DETAILS:
            print("Invalid choice. Choose from: ", DETAILS)
            return

        if previous:
            url = get_endpoints(
            ambs_operation = operation,
            operation_status = status,
            details = include
        )["Agency Mortgage-Backed Securities Operations"]["previous"]

        url = get_endpoints(
            ambs_operation = operation,
            operation_status = status,
            details = include
        )["Agency Mortgage-Backed Securities Operations"]["latest"]

        ambs_latest = pd.read_json(url)

        if ambs_latest['ambs']['auctions'] == []:
            print("No operations for the day.")
            return

        return ambs_latest


    def lastTwoWeeks(
        self,
        operation: Optional[str] = "all",
        status: Optional[str] = "results",
        include: Optional[str] = "summary"
    ) -> dict:
        """Returns the latest AMBS operation announcements or results for the last two weeks.

        Parameters
        ----------
        operation: Optional[str] = "all"
            The AMBS operation. Choices are: ["all", "purchases", "sales", "roll", "swap"]
        status: Optional[str] = "results"
            The AMBS operation status. Choices are: ["results", "announcements"]
        include: Optional[str] = "summary"
            The level of details to get. Choices are: ["details", "summary"]

        Returns
        -------
        dict: Dictionary of the results or announcements for the last two weeks.

        Example
        -------
        >>> mbs = AgencyMBSOperations()
        >>> df = mbs.get_lastTwoWeeks(operation = "swap")
        """
        if operation not in AMBS_OPERATIONS:
            print("The operation is not supported. Choices are: ", AMBS_OPERATIONS)
            return

        if status not in OPERATION_STATUS:
            print("Invalid status. Choices are: ", OPERATION_STATUS)
            return

        if include not in DETAILS:
            print("Invalid choice. Choose from: ", DETAILS)
            return

        url = get_endpoints(
            ambs_operation = operation,
            operation_status = status,
            details = include
        )["Agency Mortgage-Backed Securities Operations"]["lastTwoWeeks"]

        ambs_lastTwoWeeks = pd.read_json(url)
        
        if ambs_lastTwoWeeks['ambs']['auctions'] == []:
            print("No operations within the last two weeks.")
            return

        return ambs_lastTwoWeeks['ambs']['auctions']


    def last(
        self,
        operation: Optional[str] = "all",
        include: Optional[str] = "summary",
        operations: Optional[int] = 90
    ) -> dict:
        """Returns the last N number of AMBS operations results.

        Parameters
        ----------
        operation: Optional[str] = "all"
            The AMBS operation. Choices are: ["all", "purchases", "sales", "roll", "swap"]
        include: Optional[str] = "summary"
            The level of details to get. Choices are: ["details", "summary"]
        operations: Optional[int] = 90
            The number of AMBS operations results to get.

        Returns
        -------
        dict: Dictionary of the last N number of AMBS operations results.

        Example
        -------
        >>> mbs = AgencyMBSOperations()
        >>> df = mbs.get_last(operations = 180)
        """

        if operation not in AMBS_OPERATIONS:
            print("The operation is not supported. Choices are: ", AMBS_OPERATIONS)
            return

        if include not in DETAILS:
            print("Invalid choice. Choose from: ", DETAILS)
            return

        if operations < 1:
            print("The number of operations must be a postive integer above 0.")
            return

        url = get_endpoints(
            ambs_operation = operation,
            details = include,
            n_operations = operations
        )["Agency Mortgage-Backed Securities Operations"]["last"]

        ambs_last = pd.read_json(url)

        if ambs_last['ambs']['auctions'] == []:
            print("No operations within the last two weeks.")
            return

        return ambs_last['ambs']['auctions']

    def search(
        self,
        operation: Optional[str] = "all",
        status: Optional[str] = "results",
        include: Optional[str] = "summary",
        start: Optional[str] = (
            (datetime.now() + timedelta(days = -730))
            .strftime("%Y-%m-%d")
        ),
        end: Optional[str] = datetime.now().strftime("%Y-%m-%d"),
        securities: Optional[str] = "None",
        cusip: Optional[str] = "",
        desc: Optional[str] = "",
    ) -> dict:
        """Returns AMBS operations results from, up to, the previous two years.

        Parameters
        ----------
        operation: Optional[str] = "all"
            The AMBS operation. Choices are: ["all", "purchases", "sales", "roll", "swap"]
        status: Optional[str] = "results"
            The AMBS operation status. Choices are: ["results", "announcements"]
        include: Optional[str] = "summary"
            The level of details of information to get. Choices are: ["details", "summary"]
        start: Optional[str] = "2020-01-01"
            The end date (inclusive) up until which to search. Must be within the last two years. Format: YYYY-MM-DD
        end: Optional[str] = ""
            The start date (inclusive) from which to search. Format: YYYY-MM-DD
        securities: Optional[str] = ""
            The type of securities to search. Choices are:
            ["None", "Basket", "Coupon Swap", "Dollar Roll", "Specified Pool", "TBA"]
        desc: Optional[str] = ""
            Only return operations which include the given Security Description. Partial identifiers are accepted.        
        Returns
        -------
        dict: Dictionary of AMBS operations results.

        Example
        -------
        >>> mbs = AgencyMBSOperations()
        >>> df = mbs.search(securities = "Specified Pool")
        """
        if operation not in AMBS_OPERATIONS:
            print("The operation is not supported. Choices are: ", AMBS_OPERATIONS)
            return

        if status not in OPERATION_STATUS:
            print("Invalid status. Choices are: ", OPERATION_STATUS)
            return

        if include not in DETAILS:
            print("Invalid choice. Choose from: ", DETAILS)
            return

        if securities not in AMBS_SECURITIES.keys():
            print("Invalid choice. Choose from: ", AMBS_SECURITIES.keys())
            return

        if start < (datetime.now() + timedelta(days = -730)).strftime("%Y-%m-%d"):
            print("Date must be within the last two years. Reverting to two years ago.")
            start = (datetime.now() + timedelta(days = -730)).strftime("%Y-%m-%d")

        url = get_endpoints(
            ambs_operation = operation,
            operation_status = status,
            details = include,
            start_date = start,
            end_date = end,
            ambs_security = securities,
            cusips = cusip,
            description = desc           
        )["Agency Mortgage-Backed Securities Operations"]["search"]

        ambs_search = pd.read_json(url)

        if ambs_search['ambs']['auctions'] == []:
            print("No results found.")
            return

        return ambs_search['ambs']['auctions']


class CentralBankSwaps:
    """Class for central bank liquidity swaps operations.

    Attributes
    ----------
    swaps.latest: Function for returning central bank liquidity swaps for the current day.
            Returns: Dictionary
    swaps.last: Function for returning the last N number of liquidity swaps operations results.
            Returns: Dictionary
    swaps.counterparties: List of counterparties of liquidity swaps operations.
            Returns: List
    swaps.search: Function for searching for liquidity swaps operations.
            Returns: Dictionary
    
    Example
    -------
    >>> swaps = CentralBankSwaps()
    
    >>> df = swaps.last(operations = 180)
    """

    def __init__(self) -> None:
        self.latest
        self.last
        self.counterparties
        self.search


    def latest(
        self,
        operation: Optional[str] = "all",
    ) -> dict:
        """Returns the latest liquidity swaps operation results posted on current day.

        Parameters
        ----------
        operation: Optional[str] = "all"
            The operation type to search for. Choices are: ["all", "usdollar", "nonusdollar"]
    
        Returns
        -------
        dict: Dictionary of central bank liquidity swaps operation results posted on the current day.

        Example
        -------
        >>> df = latest()
        """

        if operation not in FXS_OPERATION_TYPES:
            print("The operation is not supported. Choices are: ", FXS_OPERATION_TYPES)
            return

        url = get_endpoints(fxs_operation_type = operation)['Central Bank Liquidty Swaps Operations']['latest']

        swaps_latest = pd.read_json(url)['fxSwaps']['operations']

        if swaps_latest == []:
            print("No operations for the day.")
            return

        return swaps_latest

    def last(
        self,
        operation: Optional[str] = "usdollar",
        operations: Optional[int] = 90,
    ) -> dict:
        """Returns the last N number of liquidity swaps operations results.

        Parameters
        ----------
        operation: Optional[str] = "usdollar"
            The operation type to search for. Choices are: ["usdollar", "nonusdollar"]
        operations: Optional[int] = 90
            The last N amount of trades to return.

        Returns
        -------
        dict: Dictionary of the last N number of liquidity swaps operations results.

        Examples
        --------
        >>> df = last(operation = "nonusdollar", operations = 180)

        >>> df = last()        
        """

        if operation != "usdollar" and operation != "nonusdollar":
            print("The operation is not supported. Choices are: ", ["usdollar", "nonusdollar"])
            return

        if operations < 1:
            print("The number of operations must be a postive integer above 0.")
            return

        url = get_endpoints(fxs_operation_type = operation)['Central Bank Liquidty Swaps Operations']['last']        

        swaps_last = pd.read_json(url)['fxSwaps']['operations']

        return swaps_last


    def counterparties(self) -> list:
        """List of counterparties of liquidity swaps operations.

        Returns
        -------
        list: List of counterparties of liquidity swaps operations.

        Example
        -------
        >>> counterparties = counterparties()        
        """

        url = get_endpoints()['Central Bank Liquidty Swaps Operations']['counterparties']

        swaps_counterparties = pd.read_json(url)["fxSwaps"]["counterparties"]

        return swaps_counterparties


    def search(
        self,
        operation: Optional[str] = "all",
        start: Optional[str] = "2000-01-01",
        end: Optional[str] = "",
        date_type: Optional[str] = "",
        counterparty: Optional[str] = "",

    ) -> dict:
        """Returns liquidity swaps operation results.

        Parameters
        ----------
        operation: Optional[str] = "all"
            The operation type to search for. Choices are: ["all", "usdollar", "nonusdollar"]
        start: Optional[str] = "2000-01-01"
            The start date (inclusive) from which to search, depending on date type. Format: YYYY-MM-DD
        end: Optional[str] = ""
            The end date (inclusive) up until to search, depending on date type. Format: YYYY-MM-DD
        date_type: Optional[str] = ""
            The date type to search for within the start and end. Choices are: ["all", "trade", "maturity"]
        counterparty: Optional[str] = ""
            A comma-separated list of counterparty names to search for. Partial names are accepted.
        
        Returns
        -------
        dict: Dictionary of liquidity swaps operation results.
        
        Example
        -------
        >>> df = search(operation = "nonusdollar", counterparty = 'eu')
        """

        if operation not in FXS_OPERATION_TYPES:
            print("The operation is not supported. Choices are: ", FXS_OPERATION_TYPES)
            return

        if date_type not in FXS_DATE_TYPES and date_type != "":
            print("The date type is not supported. Choices are: ", FXS_DATE_TYPES)
            return

        if date_type == "all":
            date_type = ""

        url = get_endpoints(
            fxs_operation_type = operation,
            start_date = start,
            end_date = end,
            fxs_date_type = date_type,
            fxs_counterparties = counterparty
        )['Central Bank Liquidty Swaps Operations']['search']

        swaps_search = pd.read_json(url)['fxSwaps']['operations']

        if swaps_search == []:
            print(
                "No results found. Expand the date range, or try one of counterparties from the list: ",
                self.counterparties()
            )
            return

        return swaps_search


class PrimaryDealerStatistics:
    """Class for Primary Dealer Statistics.

    Attributes
    ----------
    pds.all_timeseries: Function for returning all survey results.
            Returns: pd.DataFrame
    pds.list_descriptions: Function for returning descriptions for all timeseries.
            Returns: Dict
    pds.list_asof_dates: Function for returning asof dates with respective series breaks.
            Returns: Dict
    pds.list_seriesbreaks: Function for returning Series Breaks including Label, Start and End Date.
            Returns: Dict
    pds.list_timeseries: Function for returing all timeseries' key IDs.
            Returns: List
    pds.get_latest: Function for returning the latest Survey results for each timeseries and Series Break.
            Returns: Dict
    pds.get_asof: Function for returning survey results from a single date.
            Returns: Dict
    pds.get_timeseries: Function for returning the timeseries for a specific key ID.
            Returns: pd.DataFrame

    Examples
    --------
    >>> pds = PrimaryDealerStatistics()
    
    >>> surveys = pds.all_timeseries()
    """

    def __init__(self) -> None:
        self.all_timeseries
        self.list_descriptions
        self.list_asof_dates
        self.list_seriesbreaks
        self.list_timeseries
        self.get_latest
        self.get_asof
        self.get_timeseries


    def all_timeseries(self) -> pd.DataFrame:
        """Returns all survey results.

        Returns
        -------
        pd.DataFrame: DataFrame of all survey results.

        Example
        -------
        >>> surveys = all_timeseries()
        """

        url = get_endpoints()["Primary Dealer Statistics"]["all_timeseries"]

        timeseries = pd.read_csv(url)

        if not timeseries is None:
            return timeseries

        else:
            print("There was an error. Please try again later.")
            return


    def list_descriptions(self) -> dict:
        """Returns Description of timeseries/keyids.

        Returns
        -------
        dict: Dictionary of timeseries/keyids.

        Example
        -------
        >>> descriptions = list_descriptions()
        """

        url = get_endpoints()["Primary Dealer Statistics"]["list_descriptions"]

        descriptions = pd.read_json(url)['pd']['timeseries']

        if not descriptions is None:
            return pd.DataFrame(descriptions)[['keyid','description']].set_index('keyid').to_dict()

        else:
            print("There was an error with the request. Please try again later.")
            return


    def list_asof_dates(self) -> dict:
        """Returns all As Of Dates with respective Series Break.

        Returns
        -------
        dict: Dictionary of As Of Dates with respective Series Break.

        Example
        -------
        >>> asof_dates = list_asof_dates()
        """

        url = get_endpoints()["Primary Dealer Statistics"]["list_asof"]

        asof_dates = pd.read_json(url)
        if not asof_dates is None:
            return asof_dates["pd"]["asofdates"]

        else:
            print("There was an error with the request. Please try again later.")
            return


    def list_seriesbreaks(self) -> dict:
        """Returns Series Breaks including Label, Start and End Date.
        
        Returns
        -------
        dict: Dictionary of Series Breaks including Label, Start and End Date.
        
        Example
        -------
        >>> seriesbreaks = list_seriesbreaks()
        """
        
        url = get_endpoints()["Primary Dealer Statistics"]["list_seriesbreaks"]
        
        seriesbreaks = pd.read_json(url)["pd"]["seriesbreaks"]
        if not seriesbreaks is None:
            return seriesbreaks
        
        else:
            print("There was an error with the request. Please try again later.")
            return


    def get_latest(self, seriesbreak: Optional[str] = "SBN2022") -> dict:
        """
        Returns the latest Survey results for each timeseries and Series Break.

        Parameters
        ----------
        seriesbreak: Optional[str] = "SBN2022"
            The series break to return results from. Choices are: ["SBP2001", "SBP2013", "SBN2013", "SBN2015", "SBN2022"]
        Returns
        -------
        dict: Dictionary of latest survey results for each timeseries.

        Example
        -------
        >>> latest = get_latest()
        """
        
        choices = ["SBP2001", "SBP2013", "SBN2013", "SBN2015", "SBN2022"]
        
        if seriesbreak not in choices:
            print("Invalid Series Break. Choose from: ", choices)
            return
        
        url = get_endpoints()["Primary Dealer Statistics"]["latest"]
        
        latest = pd.read_json(url)["pd"]["timeseries"]
        
        if not latest is None:
            return latest
        else:
            print("There was an error with the request. Please try again later.")
            return


    def get_asof(
        self,
        asof_date: Optional[str] = ""
    ) -> dict:
        """Returns single date Survey results.
        
        Parameters
        ----------
        asof_date: Optional[str] = ""
            The single date to reuturn survey results from. Default is the latest date.

        Returns
        -------
        dict: Dictionary of survey results for the single date.

        Example
        -------
        >>> asof = get_asof()
        """
        
        if asof_date =="":
            asof_date = self.list_asof_dates()[0]["asof"]
        
        url = get_endpoints(pd_asof_date = asof_date)["Primary Dealer Statistics"]["get_asof"]
        
        asof = pd.read_json(url)['pd']['timeseries']

        if asof == []:
            print("Invalid Date. To print all dates, use: list_asof_dates()")
            return
        
        return asof


    def list_timeseries(self) -> list:
        """Returns a list of all timeseries' key IDs.
        
        Returns
        -------
        list: List of all timeseries' key IDs.
        
        Example
        -------
        TIMESERIES = list_timeseries()
        """

        return (pd.DataFrame.from_records(self.list_descriptions())['keyid'].to_list())


    def get_timeseries(
        self,
        key_id: str = "PDSOOS-ABSTOT",
        seriesbreak: Optional[str] = ""
    ) -> pd.DataFrame:
        """Returns a timeseries of a specific key ID.

        Parameters
        ----------
        key_id: str = "PDSOOS-ABSTOT"
            The single timeseries to return data for.
        seriesbreak: Optional[str] = ""
            The series break to return results from. Choices are: ["SBP2001", "SBP2013", "SBN2013", "SBN2015", "SBN2022"]

        Returns
        -------
        pd.DataFrame: Pandas DataFrame with the timeseries of a specific key ID.

        Example
        -------
        >>> timeseries = get_asof()
        """

        SERIESBREAKS = ["SBP2001", "SBP2013", "SBN2013", "SBN2015", "SBN2022"]

        if key_id not in self.list_timeseries():
            print("Invalid key ID. To print all valid key IDs, use: list_timeseries()")
            return
            
        if seriesbreak != "":
            if seriesbreak not in SERIESBREAKS:
                print("Invalid Series Break. Choose from: ", SERIESBREAKS)
                return

            url = get_endpoints(
                pd_timeseries = key_id,
                pd_seriesbreak = seriesbreak
            )["Primary Dealer Statistics"]["get_timeseries_seriesbreak"]

        else:
            url = get_endpoints(
                pd_timeseries = key_id
            )["Primary Dealer Statistics"]["get_timeseries"]

        timeseries = pd.read_json(url)['pd']['timeseries']

        if timeseries == []:
            print("Invalid key ID. To print all valid key IDs, use: list_timeseries()")
            return

        return pd.DataFrame.from_records(timeseries)


class ReferenceRates:
    """Class for Reference Rates

    Attributes
    ----------
    rates.get_latest: Function for returning the latest secured and unsecured rates.
            Returns: pd.DataFrame
    rates.search: Function for returning secured and/or unsecured rates and/or volume by date range.
            Returns: pd.DataFrame
    rates.get_secured: Function for returning the latest secured rates.
            Returns: pd.DataFrame
    rates.get_unsecured: Function for returning the latest unsecured rates.
            Returns: pd.DataFrame
    rates.last: Function for returning the last N number of rates for a single type.
            Returns: pd.DataFrame

    Examples
    --------
    >>> rates = ReferenceRates()

    >>> latest = rates.get_latest()
    """
    def __init__(self) -> None:

        self.get_latest


    def get_latest(self) -> pd.DataFrame:
        """Returns the latest secured and unsecured rates.

        Returns
        -------
        pd.DataFrame: Pandas DataFrame with the latest secured and unsecured rates.

        Example
        -------
        >>> latest = get_latest()
        """

        url = get_endpoints()["Reference Rates"]["latest"]

        latest = pd.DataFrame.from_records(pd.read_json(url)['refRates'])

        return latest


    def search(
        self,
        start: Optional[str] = "",
        end: Optional[str] = "",
        data_type: Optional[str] = "",
    ) -> pd.DataFrame:
        """Returns secured and/or unsecured rates and/or volume by date range.

        Parameters
        ----------
        start: Optional[str] = ""
            The start date (inclusive) to return data from. Default to the current date. Format: YYYY-MM-DD.
        end: Optional[str] = ""
            The end date (inclusive) up until which to search. Format: YYYY-MM-DD.
        data_type: Optional[str] = ""
            The report type to return. Choices are: ["rate", "volume", ""]

        Returns
        -------
        pd.DataFrame: Pandas DataFrame with the secured and/or unsecured rates and/or volume.

        Example
        -------
        >>> results = search(start = '2020-03-01', end = '2020-04-01')
        """

        if data_type != "rate" and data_type != "volume" and data_type != "":
            print("Invalid choice. Choose from: rate, volume, or ''") 
            return

        url = get_endpoints(
            start_date = start,
            end_date = end,
            rate_type = data_type
        )["Reference Rates"]["search"]

        results = pd.read_json(url)['refRates']

        if len(results) == 0:
            print("No data found. Try expanding the date range.")
            return

        return pd.DataFrame.from_records(results)


    def get_secured(self) -> pd.DataFrame:
        """Returns the latest secured rates.

        Returns
        -------
        pd.DataFrame: Pandas DataFrame with the latest secured rates.

        Example
        -------
        secured = get_secured()
        """

        url = get_endpoints()["Reference Rates"]["latest_secured"]
        
        secured = pd.DataFrame.from_records(pd.read_json(url)['refRates'])

        return secured

    def get_unsecured(self) -> pd.DataFrame:
        """Returns the latest unsecured rates.

        Returns
        -------
        pd.DataFrame: Pandas DataFrame with the latest unsecured rates.

        Example
        -------
        unsecured = get_unsecured()
        """

        url = get_endpoints()["Reference Rates"]["latest_unsecured"]

        unsecured = pd.DataFrame.from_records(pd.read_json(url)['refRates'])

        return unsecured


    def get_last(
        self,
        ratetype: Optional[str] = "sofr",
        number: Optional[int] = 0,
    ) -> pd.DataFrame:
        
        TYPES = SECURED_RATE_TYPES+UNSECURED_RATE_TYPES
        
        if number < 0:
            print("Invalid choice. Number must be positive.")
            return
        
        if ratetype not in TYPES:
            print("Invalid Choice. Choose from: ", TYPES)
            return
        
        if ratetype in SECURED_RATE_TYPES:
            url = get_endpoints(
                secured_type = ratetype,
                n_operations = number,
            )["Reference Rates"]["last_secured"]
        
        elif ratetype in UNSECURED_RATE_TYPES:    
            url = get_endpoints(
                unsecured_type = ratetype,
                n_operations = number,
            )["Reference Rates"]["last_unsecured"]

        last = pd.DataFrame.from_records(pd.read_json(url)['refRates'])

        return last


class RepoOperations:
    """Class for Repo and Reverse Repo Operations

    Attributes
    ----------
    repo.get_latest: Function for returning the latest Repo and/or Reverse Repo operations Announcements or Results for the current day.
            Returns: Dict
    repo.search: Function for searching for Repo and/or Reverse Repo operations.
            Returns: Dict
    repo.get_propositions: Function for returning propositions for a given date range.
            Returns: Dict

    Examples
    --------
    >>> repo = RepoOperations()
    
    >>> latest = repo.get_latest(lastTwoWeeks = True)
    
    >>> repo_df = repo.get_latest(operation_type = 'repo', last_N = 90)
    """

    def __init__(self) -> None:

        self.get_latest

    def get_latest(
        self,
        operation_type: Optional[str] = "all",
        method: Optional[str] = "all",
        status: Optional[str] = "results",
        lastTwoWeeks: Optional[bool] = False,
        last_N: Optional[int] = None,
    ) -> dict:
        """Returns the latest Repo and/or Reverse Repo operations Announcements or Results for the current day.

        Parameters
        ----------
        operation_type: Optional[str] = "all"
            The type of operation to return. Choices are: ["all", "repo", "reverserepo"]
        method: Optional[str] = "all"
            The operation method. Choices are: ["all", "fixed", "single", "multiple"]
        status: Optional[str] = "results"
            The operation status. Choices are: ["results", "announcements"]
        lastTwoWeeks: Optional[bool] = False
            Whether to return the last two weeks of operations. This overrides values entered for, last_N. Default to False.
        last_N: Optional[int] = None
            The number of previous operations to return. Default to None.

        Returns
        -------
        dict: Dictionary with the latest Repo and/or Reverse Repo operations Announcements or Results for the current day.

        Example
        -------
        >>> latest = get_latest()

        >>> latest = get_latest(last_N = 90)
        """

        if status not in OPERATION_STATUS:
            print("Invalid choice. Choose from: results, announcements")
            return

        if operation_type not in REPO_OPERATION_TYPES:
            print("Invalid choice. Choose from: ", REPO_OPERATION_TYPES)
            return

        if method not in REPO_OPERATION_METHODS:
            print("Invalid choice. Choose from: ", REPO_OPERATION_METHODS)
            return

        if lastTwoWeeks:
            url = get_endpoints(
                repo_operation_type = operation_type,
                repo_operation_method = method,
                operation_status = status
            )["Repo and Reverse Repo Operations"]["lastTwoWeeks"]

        elif last_N:
            url = get_endpoints(
            repo_operation_type = operation_type,
            repo_operation_method = method,
            operation_status = status,
            n_operations = last_N
        )["Repo and Reverse Repo Operations"]["last"]

        else:
            url = get_endpoints(
                repo_operation_type = operation_type,
                repo_operation_method = method,
                operation_status = status
            )["Repo and Reverse Repo Operations"]["latest"]

        latest = pd.read_json(url)["repo"]["operations"]

        if latest == []:
            print("No data found. Try, lastTwoWeeks, or, last_N operations.")
            return

        return latest


    def search(
        self,
        start: Optional[str] = "2000-01-01",
        end: Optional[str] = "",
        operation_type: Optional[str] = "",
        method: Optional[str] = "",
        security_type: Optional[str] = "",
        term: Optional[str] = "",
    ) -> dict:
        """Search for repo and/or reverse repo operations.

        Parameters
        ----------
        start: Optional[str] = "2000-01-01"
            The start date (inclusive) of the search. Default to "2000-01-01".
        end: Optional[str] = ""
            The end date (inclusive) to search up to. Default to "".
        operation_type: Optional[str] = ""
            The operation type. Choices are: ["all", "repo", "reverserepo"]
        method: Optional[str] = ""
            The operation method by which to filter. Choices are: ["all", "fixed", "single", "multiple"]
        security_type: Optional[str] = ""
            The security type (tranche) by which to filter. For specific types, only operations which include that type will be returned.
            Choices are: ["all", "agency", "mbs", "tsy", "srf"]
        term: Optional[str] = ""
            The term of the operation. Choices are: ["overnight", "term"]

        Returns
        -------
        dict: Dictionary of results.

        Examples
        --------
        >>> results = search()

        >>> results = search(operation_type = "repo", security_type = "mbs")

        >>> results = search(start = '2019-09-01', end = '2019-10-01')
        """

        if term not in REPO_TERM_TYPES and term != "":
            print("Invalid choice for term. Choose from: ", REPO_TERM_TYPES)
            return

        elif security_type not in REPO_SECURITY_TYPES and security_type != "":
            print("Invalid choice for security_type. Choose from: ", REPO_SECURITY_TYPES)
            return

        elif operation_type not in REPO_OPERATION_TYPES and operation_type != "":
            print("Invalid choice for operation_type. Choose from: ", REPO_OPERATION_TYPES)
            return

        elif method not in REPO_OPERATION_METHODS and method != "":
            print("Invalid method choice. Choose from: ", REPO_OPERATION_METHODS)
            return

        else:
            url = get_endpoints(
                start_date = start,
                end_date = end,
                repo_operation_type = operation_type,
                repo_operation_method = method,
                repo_security_type = security_type,
                repo_term = term
            )["Repo and Reverse Repo Operations"]["search"]

            results = pd.read_json(url)['repo']['operations']

            if results == []:
                print("No data found. Try expanding the parameters.")
                return

            return results


    def get_propositions(
        self,
        start: Optional[str] = "",
        end: Optional[str] = "",
    ) -> dict:
        """Returns Propositions for Reverse Repo operations.

        Parameters
        ----------
        start: Optional[str] = ""
            The start date (inclusive) of the search.
        end: Optional[str] = ""
            The end date (inclusive) of the search.

        Returns
        -------
        dict: Dictionary of results.

        Examples
        --------
        >>> results = get_propositions()

        >>> results = get_propositions(start = '2019-09-01', end = '2019-10-01')
        """

        url = get_endpoints(
            start_date = start,
            end_date = end,
        )["Repo and Reverse Repo Operations"]["propositions"]

        propositions = pd.read_json(url)["repo"]["operations"]

        if propositions == []:
            print("No data found. Try expanding the date range.")
            return

        return propositions


class SecuritiesLending:
    """Class for securities lending and extensions.

    Attributes
    ----------
    lending.get_latest: Function for getting the latest securities lending operations.
            Returns: Dict
    lending.search: Function for searching for securities lending operations.
            Returns: Dict

    Examples
    --------
    >>> lending = SecuritiesLending()
    
    >>> latest = lending.get_latest()
    
    >>> latest = lending.get_latest(operation_type = 'extensions', lastTwoWeeks = True, include = 'summary')
    
    >>> results = lending.search(include = 'details', cusip = '912796ZD4')
    """

    def __init__(self) -> None:

        self.get_latest

    def get_latest(
        self,
        operation_type: Optional[str] = "all",
        include: Optional[str] = "details",
        lastTwoWeeks: Optional[bool] = False,
        last_N: Optional[int] = None
    ) -> dict:
        """Gets the latest securities lending operations.

        Parameters
        ----------
        lastTwoWeeks: Optional[bool] = False
            Whether to get the last two weeks of operations. This overrides values entered for, last_N. Default to False.
        last_N: Optional[int] = None
            The number of previous operations to return. Default to None.

        Returns
        -------
        dict: Dictionary of results.

        Examples
        --------
        >>> latest = get_latest(last_N = 1)
        
        >>> latest = get_latest(operation_type = 'extensions', lastTwoWeeks = True, include = 'summary')
        """
        
        if operation_type not in LENDING_OPERATION_TYPES:
            print("Invalid choice. Choose from: ", LENDING_OPERATION_TYPES)
            return

        if lastTwoWeeks:
            url = get_endpoints(
                lending_operation = operation_type,
                details = include
            )["Securities Lending Operations"]["lastTwoWeeks"]

        elif last_N:
            url = get_endpoints(
                n_operations=last_N,
                lending_operation=operation_type,
                details = include
            )["Securities Lending Operations"]["last"]

        else:
            url = get_endpoints(
                lending_operation=operation_type,
                details = include
            )["Securities Lending Operations"]["latest"]

        latest = pd.read_json(url)["seclending"]["operations"]

        if latest == []:
            print("No data found. Try, lastTwoWeeks, or, last_N operations.")
            return

        return latest

    def search(
        self,
        operation_type: Optional[str] = "all",
        include: Optional[str] = "details",
        start: Optional[str] = (datetime.now()-timedelta(days = 364)).strftime("%Y-%m-%d"),
        end: Optional[str] = "",
        cusip: Optional[str] = "",
        desc: Optional[str] = "",
    ) -> dict:
        """Search for securities lending operations.
        
        Parameters
        ----------
        operation_type: Optional[str] = "all"
            The operation type. Choices are: ["all", "seclending", "extensions"]
        include: Optional[str] = "details"
            The level of detail to return. Choices are: ["summary", "details"]
        start: Optional[str] = (datetime.now()-timedelta(days = 364)).strftime("%Y-%m-%d")
            The start date (inclusive) to search from. Must be one year or less. Defaults to one year ago.
        end: Optional[str] = ""
            The end date (inclusive) to search up to. Defaults to now.
        cusip: Optional[str] = ""
            The CUSIP of the security to search for.
        desc: Optional[str] = ""
            The description field of the security to search for.

        Returns
        dict: Dictionary of results.

        Examples
        --------
        >>> results = search()

        >>> results = lending.search(include = 'details', cusip = '912796ZD4')
        """
        if operation_type not in LENDING_OPERATION_TYPES:
            print("Invalid choice. Choose from: ", LENDING_OPERATION_TYPES)
            return

        if include not in DETAILS:
            print("Invalid choice. Choose from: ", DETAILS)
            return

        if start < (datetime.now() + timedelta(days = -365)).strftime("%Y-%m-%d"):
            print("Start date must be less than 1 year in the past.")
            return

        url = get_endpoints(
            lending_operation = operation_type,
            details = include,
            start_date = start,
            end_date = end,
            cusips = cusip,
            description = desc
        )["Securities Lending Operations"]["search"]

        results = pd.read_json(url)["seclending"]["operations"]

        if results == []:
            print("No results found. Try expanding the parameters.")
            return

        return results


class SOMAHoldings:
    """Class for System Open Market Account Holdings.

    Attributes
    ----------
    soma.get_release_log: Function for getting the last three months of Angency Release and As Of Dates.
            Returns: Dict
    soma.get_agency_holdings: Function for getting the latest agency holdings, or as of a single date.
            Returns: Dict
    soma.get_treasury_holdings: Function for getting the latest Treasury holdings, or as of a single date.
            Returns: Dict
    soma.summary: Function for getting the summary Of SOMA holdings for each As of Date and holding type.
            Returns: pd.DataFrame
    Examples
    --------
    >>> soma = SOMAHoldings()

    >>> logs = soma.get_release_log()
    
    >>> mbs = soma.get_agency_holdings(holding_type = "mbs")
    
    >>> monthly_holdings = soma.get_treasury_holdings(monthly = True)
    """

    def __init__(self) -> None:

        self.get_summary
        self.get_release_log
        self.get_agency_holdings
        self.get_treasury_holdings


    def get_release_log(
        self,
        treasury: Optional[bool] = False,
    ) -> dict:
        """Returns the last three months Agency Release and As Of Dates.

        Returns
        -------
        dict: Dictionary of of the last three months of Angency Release and As Of Dates.

        Example
        -------
        >>> release_log = get_release_log(treasury = True)
        """

        if treasury:
            url = get_endpoints()["System Open Market Account Holdings"]["list_release_dates"]
        else:
            url = get_endpoints()["System Open Market Account Holdings"]["release_log"]

        release_log = pd.read_json(url)["soma"]["dates"]

        if release_log == []:
            print("No data found. Try again later.")
            return

        return release_log


    def get_summary(self
    ) -> pd.DataFrame:
        """Returns Summary Of SOMA holdings for each As of Date and holding type.
        
        Returns
        -------
        pd.DataFrame: Summary Of SOMA holdings for each As of Date and holding type.

        Example
        -------
        soma = get_summary()        
        """
        url = get_endpoints()["System Open Market Account Holdings"]["summary"]
        
        summary = pd.read_json(url)["soma"]
        summary = pd.DataFrame.from_records(summary).transpose()[0]
        summary = pd.DataFrame.from_records(summary)

        return summary

    def get_agency_holdings(
        self,
        asof_date: Optional[str] = "",
        cusip: Optional[str] =  "",
        holding_type: Optional[str] = "",
        wam: Optional[bool] = False
    ) -> dict:
        """Gets the latest agency holdings, or as of a single date.

        Parameters
        ----------
        asof_date: Optional[str] = get_as_of_dates()[0]
            The As Of Date to get data for. Defaults to the latest.    
        cusip: Optional[str] = ""
            The CUSIP of the security to search for. Overrides other arguments.
        holding_type: Optional[str] = ""
            The holding type for which to retrieve. Choices are: ['all', 'agency debts', 'mbs', 'cmbs'] 
        wam: Optional[bool] = False
            Whether to return a single date weighted average maturity for Agency debt. Defaults to False.
        Returns
        -------
        dict: Dictionary of results.

        Examples
        --------
        >>> holdings = get_agency_holdings(holding_type = "cmbs")
        
        >>> df = get_agency_holdings(cusip = "3138LMCK7")
        
        >>> wam = get_agency_holdings(wam = True)
        """
        if asof_date == "":
            asof_date = get_as_of_dates()[0]

        try:
            if cusip and cusip != "":
                url = get_endpoints(cusips = cusip)["System Open Market Account Holdings"]["get_cusip"]

            if asof_date:
                if wam:
                    url = get_endpoints(
                        date = asof_date,
                    )["System Open Market Account Holdings"]["agency_debts"]

                    return (pd.read_json(url)["soma"])

                else:
                    url = get_endpoints(date = asof_date)["System Open Market Account Holdings"]["get_asof"]

            if holding_type != "":
                if holding_type not in AGENCY_HOLDING_TYPES.keys():
                    print("Invalid choice. Choose from: ['all', 'agency debts', 'mbs', 'cmbs']")
                    return
                url = get_endpoints(
                    agency_holding_type = AGENCY_HOLDING_TYPES[holding_type],
                    date = asof_date
                )["System Open Market Account Holdings"]["get_holding_type"]

            holdings = pd.read_json(url)["soma"]["holdings"]

            if holdings == []:
                print("No data found. Try adjusting the date. Print all valid asof_dates with: get_as_of_dates().")
                return

            return holdings

        except Exception:
            print("There was an error with the parameters entered. Check the CUSIP and try again.")


    def get_treasury_holdings(
        self,
        asof_date: Optional[str] = "",
        cusip: Optional[str] =  "",
        holding_type: Optional[str] = "",
        wam: Optional[bool] = False,
        monthly: Optional[bool] = False
    ) -> dict:
        """Gets the latest Treasury holdings, or as of a single date.

        Parameters
        ----------
        asof_date: Optional[str] = get_as_of_dates()[0]
            The As Of Date to get data for. Defaults to the latest.    
        cusip: Optional[str] = ""
            The CUSIP of the security to search for. Overrides other arguments.
        holding_type: Optional[str] = ""
            The holding type for which to retrieve. Choices are: ['all', 'bills', 'notesbonds', 'frn', 'tips'] 
        wam: Optional[bool] = False
            Whether to return a single date weighted average maturity for Agency debt. Defaults to False.
        monthly: Optional[bool] = False
            If true, returns historical data for all securities at a monthly interval. This behaviour overrides.
        Returns
        -------
        dict: Dictionary of results.

        Examples
        --------
        >>> holdings = get_treasury_holdings(holding_type = "tips")

        >>> df = get_treasury_holdings(cusip = "912810FH6")

        >>> wam = get_treasury_holdings(wam = True)
        
        >>> monthly = get_treasury_holdings(monthly = True, holding_type = "bills")
        """
        if asof_date == "":
            asof_date = get_as_of_dates()[0]

        try:

            if cusip != "":
                url = get_endpoints(cusips = cusip)["System Open Market Account Holdings"]["get_treasury_cusip"]

            elif monthly:
                url = get_endpoints()['System Open Market Account Holdings']['get_treasury_monthly']

            elif asof_date:
                if wam:
                    url = get_endpoints(
                        date = asof_date,
                    )["System Open Market Account Holdings"]["get_treasury_debts"]

                    return (pd.read_json(url)["soma"])

                else:
                    url = get_endpoints(date = asof_date)["System Open Market Account Holdings"]["get_treasury_asof"]

            elif holding_type != "":
                if holding_type not in TREASURY_HOLDING_TYPES:
                    print("Invalid choice. Choose from: ", TREASURY_HOLDING_TYPES)
                    return
                url = get_endpoints(
                    treasury_holding_type = holding_type,
                    date = asof_date
                )["System Open Market Account Holdings"]["get_treasury_holding_type"]

            holdings = pd.read_json(url)["soma"]["holdings"]

            if holdings == []:
                print("No data found. Try adjusting the date. Print all valid asof_dates with: get_as_of_dates().")
                return

            return holdings

        except Exception:
            print("There was an error with the parameters entered. Check the CUSIP and try again.")


class TreasurySecurityOperations:
    """Class for outrights and prices paid.

    Attributes
    ----------
    treasury.get_latest: Function for returning the latest Treasury operation Announcements or Results for the current day, previous two weeks, or last N Operations.
            Returns: dict
    treasury.search: Function for searching Treasury operations for a given security type, start date, end date, or CUSIP.
            Returns: dict

    Examples
    --------
    >>> tso = TreasurySecurityOperations()

    >>> treasuries = tso.get_latest(lastTwoWeeks = True)
    
    >>> results = tso.search(cusip = "912")
    """

    def __init__(self) -> None:
        
        self.get_latest
        self.search


    def get_latest(
        self,
        operation_type: Optional[str] = "all",
        status: Optional[str] = "results",
        include: Optional[str] = "details",
        lastTwoWeeks: Optional[bool] = False,
        last_N: Optional[int] = None,
    ) -> dict:
        """Returns the latest Treasury operation Announcements or Results for the current day, previous two weeks, or last N Operations.

        Parameters
        ----------
        operation_type: Optional[str] = "all"
            The type of Treasury operation. Choices are: ["all", "purchases", "sales"]
        status: Optional[str] = "results"
            The status of the  operation. Choices are: ["announcements", "results", "operations"]
        include: Optional[str] = "details"
            The level of detail to return. Choices are: ["details", "summary"]
        lastTwoWeeks: Optional[bool] = False
            Whether to return the last two weeks of operations. This behaviour overrides. Defaults to False. 
        last_N: Optional[int] = None
            The number of last N operations to return. Defaults to None.

        Returns
        -------
        dict: Dictionary of results.

        Examples
        --------
        >>> treasuries = get_latest(lastTwoWeeks = True)

        >>> purchases = get_latest(last_N = 90, operation_type = "purchases")
        """

        if lastTwoWeeks == True:
            url = get_endpoints(treasury_status = status)["Treasury Securities Operations"]["lastTwoWeeks"]
            return (pd.read_json(url)["treasury"]["auctions"])

        if operation_type not in TREASURY_OPERATION_TYPES:
            print("Invalid choice. Choose from: ", TREASURY_OPERATION_TYPES)
            return

        if status not in TREASURY_STATUS_TYPES:
            print("Invalid choice. Choose from: ", TREASURY_STATUS_TYPES)
            return

        if last_N:
            url = get_endpoints(
                treasury_operation = operation_type,
                details = include,
                n_operations = last_N
            )["Treasury Securities Operations"]["last"]
            return (pd.read_json(url)["treasury"]["auctions"])

        url = get_endpoints(
            treasury_operation = operation_type,
            treasury_status = status,
            details = include
        )["Treasury Securities Operations"]["current"]

        latest = pd.read_json(url)["treasury"]["auctions"]

        if latest == []:
            print("No results found for the current day.")
            return

        return latest


    def search(
        self,
        operation_type: Optional[str] = "all",
        include: Optional[str] = "details",
        security_type: Optional[str] = "all",
        start: Optional[str] = "",
        end: Optional[str] = "",
        cusip: Optional[str] = "",
        desc: Optional[str] = "",

    ) -> dict:
        """Search Treasury operations for a given security type, start date, end date, or CUSIP.

        Parameters
        ----------
        operation_type: Optional[str] = "all"
            The operation type to search. Choices are: ["all", "purchases", "sales"]
        include: Optional[str] = "details"
            The level of detail to return. Choices are: ["details", "summary"]
        security_type: Optional[str] = "all"
            Filter by security type. Choices are: ["agency", "treasury"]
        start: Optional[str] = ""
            The start date to search from. Maximum of one year ago. Format: YYYY-MM-DD
        end: Optional[str] = ""
            The end date to search up to. Format: YYYY-MM-DD
        cusip: Optional[str] = ""
            Only return operations which include the given CUSIP. Partial identifiers are accepted.
        desc: Optional[str] = ""
            Only return operations which include the given Security Description. Partial identifiers are accepted.

        Returns
        -------
        dict: Dictionary of results.

        Examples
        --------
        >>> results = search(operation_type = "purchases", include = "summary")
        
        >>> results = search(cusip= '912')
        """

        if start == "":
            start = (datetime.now()-timedelta(days=364)).strftime("%Y-%m-%d")

        if start < (datetime.now()-timedelta(days = 364)).strftime("%Y-%m-%d"):
            print("Start date must be within the last year. Defaulting to one year ago.")
            start = (datetime.now()-timedelta(days=364)).strftime("%Y-%m-%d")

        if operation_type not in TREASURY_OPERATION_TYPES:
            print("Invalid choice. Choose from: ", TREASURY_OPERATION_TYPES)
            return

        if include not in DETAILS:
            print("Invalid choice. Choose from: ", DETAILS)
            return

        url = get_endpoints(
            treasury_operation = operation_type,
            details = include,
            treasury_security_type = security_type,
            cusips = cusip,
            start_date = start,
            end_date = end,
            description = desc
        )["Treasury Securities Operations"]["search"]

        results = pd.read_json(url)["treasury"]["auctions"]
        if results == []:
            print("No results found. Try expanding the search parameters.")
            return

        return results


class RestAPI:
    """Class for interacting with the NY Federal Reserve REST API.

    Attributes
    ----------
    guide_sheets: Function for retrieving guide sheets.
    ambs: Class for Agency MBS Operations.
    swaps: Class for Central Bank Swaps.
    pds: Class for Primary Dealer Statistics.
    rates: Class for Reference Rates.
    lending: Class for Securities Lending Operations.
    soma: Class for System Open Market Account Holdings.
    treasury: Class for Treasury Security Operations.
    """
    def __init__(self) -> None:
        self.guide_sheets = guide_sheets
        self.ambs = AgencyMBSOperations()
        self.swaps = CentralBankSwaps()
        self.pds = PrimaryDealerStatistics()
        self.rates = ReferenceRates()
        self.repo = RepoOperations()
        self.lending = SecuritiesLending()
        self.soma = SOMAHoldings()
        self.treasury = TreasurySecurityOperations()
