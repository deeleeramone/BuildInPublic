# A Python Interface for the NY Federal Reserve RESTful API and RSS Feeds

All data available from the RESTful API (https://markets.newyorkfed.org/static/docs/markets-api.html) is wrapped as a Python client.

## How to Use

Import it:

```console
from ny_fed.rest_api import RestAPI
fed = RestAPI()
```

Consult the docstrings:

```console
fed?

Type:        RestAPI
String form: <ny_fed.rest_api.RestAPI object at 0x102ab37f0>
File:        ~/GitHub/BuildInPublic/ny_fed/rest_api.py
Docstring:  
Class for interacting with the REST API.

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
```

Docstrings for details on each class outline the contents.

```console
fed.soma?

Type:        SOMAHoldings
String form: <ny_fed.rest_api.SOMAHoldings object at 0x102c56ad0>
File:        ~/GitHub/BuildInPublic/ny_fed/rest_api.py
Docstring:  
Class for System Open Market Account Holdings.

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
```

Most data is returned as JSON format.

```
fed.soma.get_agency_holdings(holding_type = "mbs")[0]

{'asOfDate': '2023-03-15',
 'cusip': '3132DWAW3',
 'securityDescription': 'UMBS MORTPASS 2% 01/51',
 'term': '30yr',
 'currentFaceValue': '29783152229.72',
 'isAggregated': '',
 'securityType': 'MBS'}
```

Some fields, such as `currentFaceValue` may need to be converted as a float.
