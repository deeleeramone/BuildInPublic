import re
from io import BytesIO
from functools import lru_cache
import zipfile
from openbb_core.provider.utils.helpers import get_requests_session
from demo_risk.constants import (
    COUNTRY_PORTFOLIO_FILES,
    COUNTRY_PORTFOLIOS_URLS,
    URL_MAP,
    INTERNATIONAL_INDEX_PORTFOLIO_FILES,
    INTERNATIONAL_INDEX_PORTFOLIOS_URLS,
)
from pandas import DataFrame, MultiIndex, to_datetime


FactorsDoc = """

Source
------

https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html

All returns are in U.S. dollars, include dividends and capital gains, and are not continuously compounded.

Market is the return on a region's value-weight market portfolio minus the U.S. one month T-bill rate.

The Fama/French 5 factors (2x3) are constructed using the 6 value-weight portfolios formed on size and book-to-market, the 6 value-weight portfolios formed on size and operating profitability, and the 6 value-weight portfolios formed on size and investment.

To construct the SMB, HML, RMW, and CMA factors, we sort stocks in a region into two market cap and three respective book-to-market equity (B/M), operating profitability (OP), and investment (INV) groups at the end of each June.

Big stocks are those in the top 90% of June market cap for the region, and small stocks are those in the bottom 10%. The B/M, OP, and INV breakpoints for a region are the 30th and 70th percentiles of respective ratios for the big stocks of the region.

Rm–Rf for July of year t to June of t+1 include all stocks for which we have market equity data for June of t. SMB, HML, RMW, and CMA for July of year t to June of t+1 include all stocks for which we have market equity data for December of t-1 and June of t, (positive) book equity data for t-1 (for SMB, HML, and RMW), non-missing revenues and at least one of the following: cost of goods sold, selling, general and administrative expenses, or interest expense for t-1 (for SMB and RMW), and total assets data for t-2 and t-1 (for SMB and CMA).

The momentum and short term reversal portfolios are reconstituted monthly and the other research portfolios are reconstituted annually. We reconstruct the full history of returns each month when we update the portfolios.

Size and Book-to-Market Portfolios
----------------------------------

- Small Value
- Small Neutral
- Small Growth
- Big Value
- Big Neutral
- Big Growth

BE < 0; bottom 30%, middle 40%, top 30%; quintiles; deciles.
Firms with negative book equity are in only the BE < 0 portfolio.

Size and Operating Profitability Portfolios
-------------------------------------------

- Small Robust
- Small Neutral
- Small Weak
- Big Robust
- Big Neutral
- Big Weak

Operating Profitability bottom 30%, middle 40%, top 30%; quintiles; deciles.

Size and Investment Portfolios
------------------------------

- Small Conservative
- Small Neutral
- Small Aggressive
- Big Conservative
- Big Neutral
- Big Aggressive

ME < 0 (not used); bottom 30%, middle 40%, top 30%; quintiles; deciles.
Investment bottom 30%, middle 40%, top 30%; quintiles; deciles.


Factors
-------

SMB: Small Minus Big

SMB (Small Minus Big) is the average return on the nine small stock portfolios minus the average return on the nine big stock portfolios

HML: High Minus Low

HML (High Minus Low) is the average return on the two value portfolios minus the average return on the two growth portfolios

RMW: Robust Minus Weak
RMW (Robust Minus Weak) is the average return on the two robust operating profitability portfolios minus the average return on the two weak operating profitability portfolio

CMA: Conservative Minus Aggressive

CMA (Conservative Minus Aggressive) is the average return on the two conservative investment portfolios minus the average return on the two aggressive investment portfolios

Definitions
-----------

ME : Market Equity

Market equity (size) is price times shares outstanding. Price and shares outstanding are from CRSP.

BE : Book Equity

Book equity is constructed from Compustat data or collected from the Moody’s Industrial, Financial, and Utilities manuals. BE is the book value of stockholders’ equity, plus balance sheet deferred taxes and investment tax credit (if available), minus the book value of preferred stock. Depending on availability, we use the redemption, liquidation, or par value (in that order) to estimate the book value of preferred stock. Stockholders’ equity is the value reported by Moody’s or Compustat, if it is available. If not, we measure stockholders’ equity as the book value of common equity plus the par value of preferred stock, or the book value of assets minus total liabilities (in that order). See Davis, Fama, and French, 2000, “Characteristics, Covariances, and Average Returns: 1929-1997,” Journal of Finance, for more details.

BE/ME : Book-to-Market

The book-to-market ratio used to form portfolios in June of year t is book equity for the fiscal year ending in calendar year t-1, divided by market equity at the end of December of t-1.

OP : Operating Profitability

The operating profitability ratio used to form portfolios in June of year t is annual revenues minus cost of goods sold, interest expense, and selling, general, and administrative expense divided by the sum of book equity and minority interest for the last fiscal year ending in t-1.

INV : Investment

The investment ratio used to form portfolios in June of year t is the change in total assets from the fiscal year ending in year t-2 to the fiscal year ending in t-1, divided by t-2 total assets.

E/P : Earnings/Price

Earnings is total earnings before extraordinary items, from Compustat. The earnings/price ratio used to form portfolios in June of year t is earnings for the fiscal year ending in calendar year t-1, divided by market equity at the end of December of t-1.

CF/P : Cashflow/Price

Cashflow is total earnings before extraordinary items, plus equity’s share of depreciation, plus deferred taxes (if available), from Compustat. Equity’s share is defined as market equity divided by assets minus book equity plus market equity. The cashflow/price ratio used to form portfolios in June of year t is the cashflow for the fiscal year ending in calendar year t-1, divided by market equity at the end of December of t-1.

D/P : Dividend Yield

The dividend yield used to form portfolios in June of year t is the total dividends paid from July of t-1 to June of t per dollar of equity in June of t. The dividend yield is computed using the with and without dividend returns from CRSP, as described in Fama and French, 1988, “Dividend yields and expected stock returns,” Journal of Financial Economics 25.
"""

InternationalPortofliosDoc = """

Countries
---------

We form value and growth portfolios in each country using four valuation ratios:

- book-to-market (B/M)
- earnings-price (E/P)
- cash earnings to price (CE/P)
- dividend yield (D/P)

We form the portfolios at the end of December each year by sorting on one of the four ratios and
then compute value-weighted returns for the following 12 months.

The value portfolios (High) contain firms in the top 30% of a ratio and the growth portfolios (Low) contain firms in the bottom 30%.

There are two sets of portfolios.

In one, firms are included only if we have data on all four ratios.

In the other, a firm is included in a sort variable's portfolios if we have data for that variable.

The market return (Mkt) for the first set is the value weighted average of the returns for only firms with all four ratios.

The market return for the second set includes all firms with book-to-market data, and Firms is the number of firms with B/M data.

- Australia
- Austria
- Belgium
- Canada
- Denmark
- Finland
- France
- Germany
- Hong Kong
- Ireland
- Italy
- Japan
- Malaysia
- Netherlands
- New Zealand
- Norway
- Singapore
- Spain
- Sweden
- Switzerland
- United Kingdom

International Index Portfolios
------------------------------

The returns on the index portfolios are constructed by averaging the returns on the country portfolios.

Each country is added to the index portfolios when the return data for the country begin; the country start dates can be inferred from the country return files.

We weight countries in the index portfolios in proportion to their EAFE + Canada weights.

- UK
- Scandinavia
- Europe
- Europe ex-UK
- Asia Pacific
- All

"""


@lru_cache
def download_file(dataset):
    if dataset not in list(URL_MAP):
        raise ValueError(
            f"Dataset {dataset} not found in available datasets: {list(URL_MAP)}"
        )
    url = URL_MAP[dataset]
    with get_requests_session() as session:
        response = session.get(url)
        response.raise_for_status()

    data = ""
    with zipfile.ZipFile(BytesIO(response.content)) as f:
        data = f.open(f.namelist()[0]).read()
        try:
            data = data.decode("utf-8")
        except UnicodeDecodeError:
            data = data.decode("latin-1")

    return data


def apply_date(x):
    date = str(x).replace(" ", "")
    if len(date) == 6:
        date = date[:4] + "-" + date[4:]
        date = to_datetime(date, format="%Y-%m")
        date = date.date().strftime("%Y-%m-%d")
    elif len(date) == 8:
        date = date[:4] + "-" + date[4:6] + "-" + date[6:]
    elif len(date) == 4:
        date = date + "-12-31"
    return date


def read_csv_file(data: str):
    """Parse the raw data from a .csv file into a list of dictionaries representing tables."""
    import re

    lines = data.splitlines()
    tables = []
    general_description = []

    # Extract general description from the top of the file
    description_end_idx = 0
    for idx, line in enumerate(lines):
        if line.strip().startswith(",") or re.match(r"^\s*\d{4,6}", line):
            description_end_idx = idx
            break
        if line.strip():
            general_description.append(line.strip())

    # Extract initial table metadata from the last line of general description
    table_metadata = ""
    if general_description:
        if (
            "Monthly" in general_description[-1]
            or "Annual" in general_description[-1]
            or "Returns" in general_description[-1]
        ):
            table_metadata = general_description[-1]
            general_description = general_description[:-1]

    general_desc_text = "\n".join(general_description)

    # Process tables in the file
    i = description_end_idx

    while i < len(lines):
        # Skip empty lines
        while i < len(lines) and not lines[i].strip():
            i += 1

        if i >= len(lines):
            break

        # Check if this is a table header (starts with comma)
        if lines[i].strip().startswith(","):
            # Look for metadata line before this header
            metadata = table_metadata  # Default to initial metadata
            j = i - 1
            while j >= description_end_idx:
                if lines[j].strip():
                    metadata = lines[j].strip()
                    break
                j -= 1

            # Parse headers
            header_line = lines[i].strip()
            headers = ["Date"] + header_line.split(",")[1:]

            # Move past header row
            i += 1

            # Collect data rows
            data_rows = []

            # Look for data rows until we hit the next header or end of file
            while i < len(lines):
                line = lines[i].strip()
                if not line:
                    break
                values = line.split(",")
                if values:
                    data_rows.append([d.strip() for d in values])

                i += 1

            # Add table if it has data
            if data_rows:
                tables.append(
                    {
                        "meta": metadata,
                        "headers": headers,
                        "rows": data_rows,
                        "is_annual": "Annual" in metadata,
                    }
                )

        # Check for standalone metadata
        elif "--" in lines[i] and any(
            d in lines[i] for d in ["Daily", "Monthly", "Annual", "Weekly"]
        ):
            # Update metadata for next table
            table_metadata = lines[i].strip()
            i += 1
        else:
            # Skip other lines
            i += 1

    return tables, general_desc_text


def process_csv_tables(tables, general_description=""):
    """Convert parsed table dictionaries from CSV files to pandas DataFrames."""
    dataframes = []
    metadata = []

    for table_idx, table in enumerate(tables):
        # Skip empty tables
        if not table["rows"]:
            continue

        # Create DataFrame from rows
        rows_data = table.get("rows", [])
        headers = table["headers"]

        # Check if we have enough headers
        max_cols = max(len(row) for row in rows_data)
        if len(headers) < max_cols:
            headers.extend([f"Column_{i}" for i in range(len(headers), max_cols)])

        # Create dataframe with proper dimensions and headers
        df = DataFrame(rows_data)

        if df.empty or df.shape[1] == 0:
            continue

        # Set column names
        df.columns = headers[: df.shape[1]]

        # Convert Date column to datetime
        try:
            # Convert YYYYMM format to datetime
            df["Date"] = df.Date.apply(apply_date)
        except Exception as e:
            print(f"Error parsing dates: {e}")
            df["Date"] = df["Date"].astype(str)

        # Set Date as index
        df = df.set_index("Date")
        df = df.sort_index()
        dataframes.append(df)

        # Get metadata from the table
        table_meta_desc = table["meta"].strip()

        # Determine frequency from the table's metadata
        frequency = "monthly"
        if "Annual" in table_meta_desc:
            frequency = "annual"

        # Create metadata entry
        table_meta = {
            "description": f"### {table_meta_desc}\n\n"
            + general_description.replace("\n", " ")
            + "\n\n",
            "frequency": frequency,
            "formations": headers[1:],
        }
        metadata.append(table_meta)

    return dataframes, metadata


def read_dat_file(data: str):
    """Parse the raw data from a .dat file into a list of dictionaries representing tables."""
    import re

    lines = data.splitlines()
    tables = []

    i = 0
    current_table = []
    while i < len(lines):
        # Check for table separator or new table metadata indicator
        if re.match(r"\s*,", lines[i]) or (
            i > 1
            and "Data" in lines[i]
            and "current_table" in locals()
            and current_table["rows"]
        ):
            # Add current table if it exists and has rows
            if "current_table" in locals() and current_table["rows"]:
                tables.append(current_table)

            # If this is a separator line, skip it
            if re.match(r"\s*,", lines[i]):
                i += 1
                continue

        # Start a new table
        current_table = {"meta": "", "spanners": "", "headers": [], "rows": []}
        meta_lines = []

        # Process metadata (which may span multiple lines)
        while i < len(lines):
            line = lines[i].strip()
            # Check if this line looks like the start of data or spanner rows
            if (
                "--" in line  # Spanner line
                or "Firms" in line.split()
                and any(c.isdigit() for c in lines[i + 1])
                if i + 1 < len(lines)
                else False  # Firms header
                or line in ["", " "]  # Empty separator
                or (line and line[0].isdigit() and len(line.split()) > 2)  # Data row
            ):
                break

            if line:  # Only add non-empty lines
                meta_lines.append(line)
            i += 1

        # Join all metadata lines
        current_table["meta"] = "\n".join(meta_lines)

        # Process spanners if we have a line with dashes
        if i < len(lines) and "--" in lines[i]:
            current_table["spanners"] = lines[i]
            i += 1
        else:
            # No spanners line found
            current_table["spanners"] = ""  # Empty spanners for tables like "Firms"

        # Process headers - handle special case for "Firms" tables
        if i < len(lines):
            header_line = lines[i].strip()
            # Check if this is a "Firms" table with its specific format
            if "Firms" in header_line.split():
                current_table["headers"] = ["Date"] + header_line.split()
                i += 1
            elif header_line and not header_line[0].isdigit():  # Normal header line
                current_table["headers"] = ["Date"] + header_line.split()
                i += 1
            else:  # This is already a data row - create default headers
                if "Firms" in current_table["meta"]:
                    # Default headers for Firms tables if header row is missing
                    current_table["headers"] = [
                        "Date",
                        "Firms",
                        "B/M",
                        "E/P",
                        "CE/P",
                        "Yld",
                    ]
                else:
                    # Skip this table - malformed
                    while i < len(lines) and not (
                        re.match(r"\s*,", lines[i]) or "Data" in lines[i]
                    ):
                        i += 1
                    continue

        # Process rows until next separator or next table start
        row_count = 0
        while i < len(lines) and not (
            re.match(r"\s*,", lines[i]) or "Data" in lines[i]
        ):
            # Skip copyright lines, empty lines, and other non-data lines
            if (
                lines[i].strip()
                and not lines[i].strip().startswith("Copyright")
                and "©" not in lines[i]
                and any(c.isdigit() for c in lines[i])
            ):  # Ensure line has at least one digit (likely a date)
                current_table["rows"].append(lines[i].split())
                row_count += 1
            i += 1

    # Add the last table if it has rows
    if "current_table" in locals() and current_table["rows"]:
        tables.append(current_table)

    return tables


@lru_cache
def download_international_portfolios(url):
    """Download the international index portfolios file."""
    with get_requests_session() as session:
        response = session.get(url)
        response.raise_for_status()

    return response


def get_international_portfolio_data(
    index: str = None, country: str = None, dividends: bool = True
):
    """Download and extract the international index or country portfolio data."""
    if not index and not country:
        raise ValueError("Please provide either an index or a country.")
    if index:
        if index not in list(INTERNATIONAL_INDEX_PORTFOLIO_FILES):
            raise ValueError(
                f"Index {index} not found in available indexes: {INTERNATIONAL_INDEX_PORTFOLIO_FILES}"
            )
        url = INTERNATIONAL_INDEX_PORTFOLIOS_URLS["dividends" if dividends else "ex"]
        index = INTERNATIONAL_INDEX_PORTFOLIO_FILES[index]
    if country:
        if country not in list(COUNTRY_PORTFOLIO_FILES):
            raise ValueError(
                f"Country {country} not found in available countries: {COUNTRY_PORTFOLIO_FILES}"
            )
        url = COUNTRY_PORTFOLIOS_URLS["dividends" if dividends else "ex"]
        index = COUNTRY_PORTFOLIO_FILES[country]

    response = download_international_portfolios(url)
    with zipfile.ZipFile(BytesIO(response.content)) as f:
        filenames = f.namelist()
        if index in filenames:
            try:
                data = f.open(index).read().decode("utf-8")
            except UnicodeDecodeError:
                data = f.open(index).read().decode("latin-1")
        else:
            raise ValueError(
                f"Index {index} not found in available indexes: {filenames}"
            )

        return data


def process_international_portfolio_data(tables: list, dividends: bool = True):
    """Convert parsed table dictionaries to pandas DataFrames with proper multi-index columns."""
    dataframes = []
    metadata = []

    for table_idx, table in enumerate(tables):
        # Extract spanner groups
        spanner_groups = table["spanners"].replace("-", "").split()

        # Create DataFrame from rows
        rows_data = table.get("rows", [])
        df = DataFrame(rows_data)

        if df.empty:
            continue

        # Set column names based on headers
        headers = table["headers"]
        if len(headers) == len(df.columns):
            df.columns = headers

        # Check if this is a special case table with "Firms" column
        has_firms_column = "Firms" in df.columns

        # Parse and set Date column
        try:
            df["Date"] = df["Date"].apply(apply_date)
        except Exception:
            df["Date"] = df["Date"].astype(str)

        # Set Date as index (or Date and Mkt if applicable)
        if "Mkt" in df.columns and not has_firms_column:
            df = df.set_index(["Date", "Mkt"])
        else:
            df = df.set_index("Date")

        # Create multi-index columns only for regular tables (not those with Firms)
        if not has_firms_column and spanner_groups and len(df.columns) > 0:
            # Create multi-index columns
            remaining_headers = list(df.columns)
            bottom_level = remaining_headers

            # Calculate columns per group
            cols_per_group = len(remaining_headers) // len(spanner_groups)

            # Create top level for multi-index
            top_level = []
            for group in spanner_groups:
                top_level.extend([group] * cols_per_group)

            # Handle Zero column specially
            if "Zero" in remaining_headers:
                zero_idx = remaining_headers.index("Zero")
                # Find Yld group
                for group in spanner_groups:
                    if group.lower() == "yld":
                        # Ensure top_level has enough elements
                        while len(top_level) <= zero_idx:
                            top_level.append("")
                        top_level[zero_idx] = group
                        break

            # Create the multi-index columns
            if len(top_level) == len(bottom_level):
                df.columns = MultiIndex.from_arrays([top_level, bottom_level])

        dataframes.append(df)

        # Format metadata for description
        meta_text = table["meta"].strip().replace("\n", " - ")
        if dividends is False:
            meta_text += " - Ex-Dividends"

        # Format metadata nicely, replacing multiple spaces with a single space
        meta_text = re.sub(r"\s{2,}", " ", meta_text)

        is_annual = (
            df.index[0][0].date().day == 31
            if isinstance(df.index, MultiIndex)
            else df.index[0].day == 31
        )

        table_meta = {
            "description": meta_text,
            "frequency": "annual" if is_annual else "monthly",
            "formations": (
                [d for d in df.columns.tolist() if d != "Firms"]
                if has_firms_column
                else spanner_groups
            ),
        }
        metadata.append(table_meta)

    return dataframes, metadata


def get_international_portfolio(
    index: str = None,
    country: str = None,
    dividends: bool = True,
    frequency: str = None,
    measure: str = "usd",
    all_data_items_required: bool = None,
) -> tuple:
    data = get_international_portfolio_data(index, country, dividends)
    tables = read_dat_file(data)
    dataframes, metadata = process_international_portfolio_data(tables, dividends)

    if measure and measure not in ["usd", "local", "ratios"]:
        raise ValueError(
            f"Measure {measure} not supported. Choose from 'usd', 'local', or 'ratios'."
        )

    if frequency == "monthly" and measure == "ratios":
        raise ValueError("Only annual frequency is available for 'ratios' measure.")

    if frequency:
        dfs = [
            df
            for df, meta in zip(dataframes, metadata)
            if meta["frequency"] == frequency
        ]
        dfs_meta = [meta for meta in metadata if meta["frequency"] == frequency]
    else:
        dfs = dataframes
        dfs_meta = metadata

    if measure == "local":
        dfs = [df for df, meta in zip(dfs, dfs_meta) if "Local" in meta["description"]]
        dfs_meta = [meta for meta in dfs_meta if "Local" in meta["description"]]
    elif measure == "usd":
        dfs = [
            df for df, meta in zip(dfs, dfs_meta) if "Local" not in meta["description"]
        ]
        dfs_meta = [meta for meta in dfs_meta if "Local" not in meta["description"]]
    elif measure == "ratios":
        dfs = [df for df, meta in zip(dfs, dfs_meta) if "Ratios" in meta["description"]]
        dfs_meta = [meta for meta in dfs_meta if "Ratios" in meta["description"]]

    if all_data_items_required is True:
        dfs = [
            df for df, meta in zip(dfs, dfs_meta) if "Required" in meta["description"]
        ]
        dfs_meta = [meta for meta in dfs_meta if "Required" in meta["description"]]
    elif all_data_items_required is False:
        dfs = [
            df
            for df, meta in zip(dfs, dfs_meta)
            if "Not Reqd" not in meta["description"]
        ]
        dfs_meta = [meta for meta in dfs_meta if "Not Reqd" not in meta["description"]]

    return dfs, dfs_meta


def get_portfolio_data(
    dataset: str, frequency: str = None, measure: str = None
) -> tuple:
    """Get the portfolio data for a given dataset."""
    if frequency and frequency.lower() not in ["monthly", "annual", "daily"]:
        raise ValueError(
            f"Frequency {frequency} not supported. Choose from 'monthly', 'annual', or 'daily'."
        )
    if measure and measure not in ["value", "equal", "number_of_firms", "firm_size"]:
        raise ValueError(
            f"Measure {measure} not supported. Choose from 'value', 'equal', 'number_of_firms', or 'firm_size'."
        )
    if measure in ["number_of_firms", "firm_size"] and frequency == "annual":
        raise ValueError(
            f"Measure '{measure}' is only available for monthly frequency."
        )
    if "Factor" in dataset:
        measure = None

    file = download_file(dataset)
    table, desc = read_csv_file(file)
    dfs, metadata = process_csv_tables(table, desc)

    if frequency:
        out_dfs = [
            df
            for df, meta in zip(dfs, metadata)
            if meta["frequency"].lower() == frequency.lower()
        ]
        out_metadata = [
            meta for meta in metadata if meta["frequency"].lower() == frequency.lower()
        ]
    else:
        out_dfs = dfs
        out_metadata = metadata

    if measure is not None:
        if measure in ["value", "equal"]:
            out_dfs = [
                df
                for df, meta in zip(out_dfs, out_metadata)
                if "--" in meta["description"]
                and measure.lower() in meta["description"].split(" -- ")[0].lower()
            ]
            out_metadata = [
                meta
                for meta in out_metadata
                if "--" in meta["description"]
                and measure.lower() in meta["description"].split(" -- ")[0].lower()
            ]
        elif measure == "number_of_firms":
            out_dfs = [
                df
                for df, meta in zip(out_dfs, out_metadata)
                if "Number of Firms" in meta["description"]
            ]
            out_metadata = [
                meta
                for meta in out_metadata
                if "Number of Firms" in meta["description"]
            ]
        elif measure == "firm_size":
            out_dfs = [
                df
                for df, meta in zip(out_dfs, out_metadata)
                if "Average Firm Size" in meta["description"]
            ]
            out_metadata = [
                meta
                for meta in out_metadata
                if "Average Firm Size" in meta["description"]
            ]

    return out_dfs, out_metadata
