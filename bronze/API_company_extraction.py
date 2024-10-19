import finnhub
import pandas as pd

# Extract company profile from Finnhub API


def company_profile_table(symbol, api_key):
    # Initialize the Finnhub client
    finnhub_client = finnhub.Client(api_key=api_key)

    try:
        data = finnhub_client.company_profile2(symbol=symbol)
    except finnhub.FinnhubAPIException as e:
        print(f"FinnhubAPIException for symbol {symbol}: {e}")
        return pd.DataFrame()

    if not data:
        print(f"No company profile data found for symbol {symbol}.")
        return pd.DataFrame()

    rows = [
        {
            "name": data.get("name", ""),
            "ticker": data.get("ticker", ""),
            "currency": data.get("currency", ""),
            "country": data.get("country", ""),
            "exchange": data.get("exchange", ""),
            "industry": data.get("finnhubIndustry", ""),
            "weburl": data.get("weburl", ""),
        }
    ]

    return pd.DataFrame(rows)
