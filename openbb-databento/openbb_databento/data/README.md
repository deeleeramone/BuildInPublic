## Databento Demonstration Application

This application is a demonstration integration of CME futures data, utilizing Databento's Python [client](https://github.com/databento/databento-python/tree/main).

### Authorization

A subscription to the CME Globex MDP 3.0 feed is required. Details of this dataset are found [here](https://databento.com/docs/venues-and-datasets/glbx-mdp3).

Add your Databento API key as an environment variable, DATABENTO_API_KEY, from the command line, or create/modify an `.env` file in `~/.openbb_platform/.env`. The key can also be added to the "credentials" dictionary in, `~/.openbb_platform/user_settings.json`, with a key, `databento_api_key`.

### Charting

Charting is powered by a TradingView UDF server, with daily, historical, continuous contracts 0-12 for select assets, spanning 2010-07-01 through 2025-05-01.

Weekly and monthly bars are aggregated in the backend and served to the widget.

Data is cached for performance.

### Streaming Trades

Trades of futures represent the front contract, and all assets share a single connection to the Databento Live client.

Subscribe/Unsubscribe events are handled by the backend via a pub/sub workflow, allowing unlimited instances of any asset(s) stream.

The backend is configured to connect on startup and open the trades stream, subscribing to a defined list of assets.

The master list is broken down into categories, each one having its own tab for display.

If an asset has not traded since the backend was started, it will display as 0 and update when a trade message is received.

