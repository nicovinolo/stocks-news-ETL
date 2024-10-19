from utils.config import REDSHIFT_SCHEMA
import pandas as pd


def table_tansformation(engine, date):

    # Convert the date to the appropriate format if it's not already
    formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')

    query = f"""
    INSERT INTO "{REDSHIFT_SCHEMA}".source_dimension (description)
    SELECT DISTINCT a.source as description
    FROM "{REDSHIFT_SCHEMA}".news_raw_data a
    WHERE a.date = CAST ('{formatted_date}' AS DATE)
    AND NOT EXISTS (
        SELECT 1
        FROM "{REDSHIFT_SCHEMA}".source_dimension b
        WHERE b.description = a.source
    );

    DELETE FROM "{REDSHIFT_SCHEMA}".news_fact_table
    WHERE date = CAST('{formatted_date}' AS DATE);

    INSERT INTO "{REDSHIFT_SCHEMA}".news_fact_table (date, company_id, source_id, headline, summary, url)
    SELECT
        a.date,
        b.company_id,
        c.source_id,
        a.headline,
        a.summary,
        a.url
    FROM "{REDSHIFT_SCHEMA}".news_raw_data a
    INNER JOIN "{REDSHIFT_SCHEMA}".company_dimension b on (a.company = b.ticker)
    INNER JOIN "{REDSHIFT_SCHEMA}".source_dimension c on (a.source = c.description)
    WHERE a.date = CAST('{formatted_date}' AS DATE);

    """

    connection = engine.connect()

    connection.execute(query)

    connection.close()
    print("Redshift connection closed.")
