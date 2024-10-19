from utils.config import REDSHIFT_SCHEMA
import pandas as pd


def populate_company_insights(engine, date):

    # Convert the date to the appropriate format if it's not already
    formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')

    query = f"""
    DELETE FROM "{REDSHIFT_SCHEMA}".company_insights
    WHERE snapshot_date = CAST('{formatted_date}' AS DATE);

    INSERT INTO "{REDSHIFT_SCHEMA}".company_insights (company, snapshot_date, total_news, unique_sources, earliest_news_date, latest_news_date, average_headline_length )
    SELECT
        b.name as company,
        CAST('{formatted_date}' AS DATE) AS snapshot_date,
        COUNT(a.news_id) AS total_news,
        COUNT(DISTINCT a.source_id) AS unique_sources,
        MIN(a.date) AS earliest_news_date,
        MAX(a.date) AS latest_news_date,
        AVG(LENGTH(a.headline)) AS average_headline_length
    FROM "{REDSHIFT_SCHEMA}".news_fact_table a
    INNER JOIN "{REDSHIFT_SCHEMA}".company_dimension b on (a.company_id = b.company_id)
    WHERE a.date <= CAST('{formatted_date}' AS DATE)
    GROUP BY b.name;

    """

    connection = engine.connect()

    connection.execute(query)

    connection.close()
    print("Redshift connection closed.")
