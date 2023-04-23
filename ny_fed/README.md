# A Python Wrapper for the NY Federal Reserve

## Primary Dealer Statistics

This module (WIP) takes the raw data from the RESTful API and parses it into Pandas DataFrames.
Individual time series can be returned, or grouped like the website.  All values are in millions of US dollars.

https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics


### How to Use

Import it:

```python
from primary_dealer_statistics import search_pds_descriptions,get_pds_series,get_dealer_financing,DealerPositioning,Fails
```

Initialize the classes:

```python
dp = DealerPositioning()
fails = Fails()
```

Search the descriptions:

```python

help(search_pds_descriptions)

Signature: search_pds_descriptions(description: str = '') -> pandas.core.frame.DataFrame
Docstring:
Search for Primary Dealer Statistics descriptions. Results are case sensitive.

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
```

```python
search_pds_descriptions(description = "ASSET-BACKED").head(1)
```

| Description                                                                                                                                | Series ID   |
|:-------------------------------------------------------------------------------------------------------------------------------------------|:------------|
| Total - ASSET-BACKED SECURITIES: DEALER TRANSACTIONS WITH INTER-DEALER BROKERS. + ASSET-BACKED SECURITIES: DEALER TRANSACTIONS WITH OTHER. | PDABTOT     |


Lookup individual series:

```python
get_pds_series('PDABTOT').tail(5)
```

| As Of Date   |   PDABTOT |
|:-------------|----------:|
| 2023-03-15   |      1818 |
| 2023-03-22   |      1476 |
| 2023-03-29   |      2765 |
| 2023-04-05   |      2193 |
| 2023-04-12   |      2097 |

![Screenshot 2023-04-23 at 1 46 50 PM](https://user-images.githubusercontent.com/85772166/233865435-0c5a7d5a-aab4-49e9-b44e-bb30773515c5.png)

```python
dp.tips().tail(5)
```

| As Of Date   |   <= 2 Years |   > 2 Years <= 6 Years |   > 6 Years <= 11 Years |   > 11 Years |   Total TIPS |
|:-------------|-------------:|-----------------------:|------------------------:|-------------:|-------------:|
| 2023-03-15   |        11320 |                   6485 |                    1348 |          530 |        19683 |
| 2023-03-22   |        10752 |                   6571 |                    1328 |          289 |        18940 |
| 2023-03-29   |        10852 |                   6823 |                    1511 |          310 |        19496 |
| 2023-04-05   |        12827 |                   7630 |                    1215 |          667 |        22339 |
| 2023-04-12   |        11397 |                   7433 |                     570 |          290 |        19690 |

```python
fails.mbs_fails(mbs_class = 'c').tail(5)
```

| As Of Date   |   <2.5% |   2.5% |   3.0% |   3.5% |   4.0% |   4.5% |   5.0% |   5.5% |   6.0% |   >6.0% |
|:-------------|--------:|-------:|-------:|-------:|-------:|-------:|-------:|-------:|-------:|--------:|
| 2022-11-21   |   23468 |  49853 |  61728 |  43598 |  73162 | 121508 | 122199 |  92571 |  28146 |    3957 |
| 2022-12-20   |   20403 |  47651 |  45153 |  33914 |  64548 |  81273 | 101005 |  96802 |  64589 |   25892 |
| 2023-01-23   |   21386 |  44707 |  45006 |  34015 |  59571 |  77182 |  93820 |  83301 |  71046 |   32967 |
| 2023-02-21   |   27093 |  49499 |  44586 |  36300 |  60985 |  89913 | 145910 | 102226 |  59099 |   19205 |
| 2023-03-21   |   23438 |  42656 |  48508 |  33781 |  46644 |  79600 | 119420 |  95710 |  63744 |   18605 |


## RESTful API

All data available from the RESTful API (https://markets.newyorkfed.org/static/docs/markets-api.html) is accessible via Python functions.

### How to Use

Import it:

```python
from ny_fed.rest_api import RestAPI
fed = RestAPI()
```

Consult the docstrings:

```python
fed?
```

```console
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

Docstrings contain details for the contents of each sub-class.

```python
fed.soma?
```

```console
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

```python
fed.soma.get_agency_holdings(holding_type = "mbs")[0]
```

```console
{'asOfDate': '2023-03-15',
 'cusip': '3132DWAW3',
 'securityDescription': 'UMBS MORTPASS 2% 01/51',
 'term': '30yr',
 'currentFaceValue': '29783152229.72',
 'isAggregated': '',
 'securityType': 'MBS'}
```

Some fields, such as `currentFaceValue`, may need to be converted as a float.

```python
import pandas as pd

df = pd.DataFrame.from_records(
    fed.soma.get_agency_holdings(holding_type = "mbs")
)
df["currentFaceValue"] = df.currentFaceValue.astype(float)
df["currentFaceValue"].sum()
```

```console
2600298927914.92
```
