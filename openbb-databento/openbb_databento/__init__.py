"""OpenBB DataBento Provider Extension."""

from openbb_core.provider.abstract.provider import Provider

databento_provider = Provider(
    name="databento",
    description="Databento provides live and historical market data APIs for equities, options, futures, and FX.",
    website="https://databento.com/docs/quickstart/set-up",
    credentials=[
        "api_key",
    ],
    repr_name="Databento - Market data APIs for real-time and historical data.",
    instructions="""
You need an API key to request data from Databento. Sign up (https://databento.com/signup) and you will automatically receive an API key. Each API key is a 32-character string starting with db-, that can be found from the API keys page on your portal (https://databento.com/portal/keys).
""",
    fetcher_dict={},
)
