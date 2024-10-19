import pandas as pd
from bronze.API_news_extraction import news_table
from bronze.API_company_extraction import company_profile_table
from utils.config import DIR_PATH
from airflow.exceptions import AirflowException

# Create two raw parquets


def parquet_creation(symbols, api_key, date):

    # News raw data
    news_raw = pd.DataFrame()
    for symbol in symbols:
        df_news = news_table(symbol, api_key, date)
        news_raw = pd.concat([news_raw, df_news], ignore_index=True)

    if news_raw.empty:
        raise AirflowException(
            "No news_raw data was retrieved for companies. DAG failed"
        )

    # Create parquet for news raw data
    news_file_path = (DIR_PATH + "/bronze/news/news_raw" + date + ".parquet")

    news_raw.to_parquet(news_file_path)

    # Company raw data
    company_raw = pd.DataFrame()
    for symbol in symbols:
        df_company = company_profile_table(symbol, api_key)
        company_raw = pd.concat([company_raw, df_company], ignore_index=True)

    if len(company_raw) != len(symbols):
        raise AirflowException(
            "There was a problem retrieving companies data. DAG failed"
        )

    # Create parquet for company raw data
    company_profile_path = (
        DIR_PATH +
        "/bronze/company/company_raw" +
        date +
        ".parquet")

    company_raw.to_parquet(company_profile_path)
