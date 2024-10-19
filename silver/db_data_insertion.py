from utils.config import DIR_PATH
from utils.config import REDSHIFT_SCHEMA
import pandas as pd


def insert_company_data_into_redshift(engine, date):
    company_file_path = (
        DIR_PATH +
        "/bronze/company/company_raw" +
        date +
        ".parquet")
    company_profile_df = pd.read_parquet(company_file_path)

    connection = engine.connect()

    for index, row in company_profile_df.iterrows():
        # Extracting company details from each row
        company_details = {
            "name": row['name'],
            "ticker": row['ticker'],
            "currency": row['currency'],
            "country": row['country'],
            "exchange": row['exchange'],
            "industry": row['industry'],
            "weburl": row['weburl']
        }

        # Check if the company already exists in the Redshift table based on
        # ticker
        query_check = f"""
            SELECT COUNT(*)
            FROM "{REDSHIFT_SCHEMA}".company_dimension
            WHERE ticker = '{company_details['ticker']}'
        """
        result = connection.execute(query_check).fetchone()

        if result[0] == 0:  # If the company does not exist
            # Insert the new company data into the Redshift table
            query_insert = f"""
                INSERT INTO "{REDSHIFT_SCHEMA}".company_dimension (name, ticker, currency, country, exchange, industry, weburl)
                VALUES ('{company_details['name']}', '{company_details['ticker']}', '{company_details['currency']}', '{company_details['country']}',
                        '{company_details['exchange']}', '{company_details['industry']}', '{company_details['weburl']}')
            """
            connection.execute(query_insert)
            print(
                f"Inserted company with ticker '{company_details['ticker']}' into company_dimension table.")
        else:
            print(
                f"Company with ticker '{company_details['ticker']}' already exists in the company_dimension table.")

    connection.close()


def insert_news_data_into_redshift(engine, date):

    news_df = pd.read_parquet(
        DIR_PATH +
        "/bronze/news/news_raw" +
        date +
        ".parquet")

    # Eliminate problematic characters
    news_df['summary'] = news_df['summary'].str.replace(
        r"[^a-zA-Z0-9\s]", ' ', regex=True)
    news_df['headline'] = news_df['headline'].str.replace(
        r"[^a-zA-Z0-9\s]", ' ', regex=True)
    news_df['date'] = pd.to_datetime(news_df['date'], unit='s').dt.date

    connection = engine.connect()
    print("Connection to Redshift successful!")

    # Convert the date to the appropriate format if it's not already
    formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')

    # Delete existing records for the specified date in news_raw_data
    delete_query = f"""
        DELETE FROM "{REDSHIFT_SCHEMA}".news_raw_data
        WHERE date = '{formatted_date}'
    """
    connection.execute(delete_query)
    print(f"Data for day {formatted_date} was deleted if existed")

    for index, row in news_df.iterrows():
        # Extracting news details from each row
        news_details = {
            "date": row['date'].strftime('%Y-%m-%d'),
            "company": row['company'],
            "source": row['source'],
            "headline": row['headline'][:2998],
            "summary": row['summary'][:5998],
            "url": row['url'][:598]
        }

        insert_query = f"""
            INSERT INTO "{REDSHIFT_SCHEMA}".news_raw_data (date, company, source, headline, summary, url)
            VALUES ('{news_details['date']}', '{news_details['company']}', '{news_details['source']}',
                    '{news_details['headline']}', '{news_details['summary']}', '{news_details['url']}')
        """

        connection.execute(insert_query)

    connection.close()
