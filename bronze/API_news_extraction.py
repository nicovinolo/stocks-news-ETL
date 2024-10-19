import finnhub
import pandas as pd

# Extract news from finnhub API


def news_table(symbol, api_key, date):
    # Initialize the Finnhub client
    finnhub_client = finnhub.Client(api_key=api_key)

    try:
        data = finnhub_client.company_news(
            symbol=symbol, _from=f"{date}", to=f"{date}")
    except finnhub.FinnhubAPIException as e:
        print(f"FinnhubAPIException for symbol {symbol}: {e}")
        return pd.DataFrame()

    if not data:
        print(f"No news data found for symbol {symbol}.")
        return pd.DataFrame()

    rows = []
    for article in data:
        rows.append({
            "date": pd.to_datetime(article.get("datetime", ""), unit='s'),
            "company": article.get("related", ""),
            "source": article.get("source", ""),
            "headline": article.get("headline", ""),
            "summary": article.get("summary", ""),
            "url": article.get("url", "")
        })

    return pd.DataFrame(rows)
