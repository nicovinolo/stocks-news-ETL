from utils.config import REDSHIFT_SCHEMA


def create_redshift_tables(engine):

    query = f"""
    CREATE TABLE IF NOT EXISTS "{REDSHIFT_SCHEMA}".news_raw_data (
        news_id INT IDENTITY(1,1),
        date DATE,
        company VARCHAR (900),
        source VARCHAR (900),
        headline VARCHAR(3000),
        summary VARCHAR(6000),
        url VARCHAR(600),
        PRIMARY KEY(news_id)
    );

    CREATE TABLE IF NOT EXISTS "{REDSHIFT_SCHEMA}".source_dimension (
        source_id INT IDENTITY(1,1),
        description VARCHAR(900),
        PRIMARY KEY(source_id)
    );

    CREATE TABLE IF NOT EXISTS "{REDSHIFT_SCHEMA}".company_dimension (
        company_id INT IDENTITY(1,1),
        name VARCHAR (900),
        ticker VARCHAR(10),
        currency VARCHAR(10),
        country VARCHAR(200),
        exchange VARCHAR(200),
        industry VARCHAR(200),
        weburl VARCHAR(900),
        PRIMARY KEY(company_id)
    );

    CREATE TABLE IF NOT EXISTS "{REDSHIFT_SCHEMA}".news_fact_table (
        news_id INT IDENTITY(1,1),
        date DATE,
        company_id INT,
        source_id INT,
        headline VARCHAR(3000),
        summary VARCHAR(6000),
        url VARCHAR(600),
        PRIMARY KEY(news_id),
        FOREIGN KEY (company_id) REFERENCES "{REDSHIFT_SCHEMA}".company_dimension(company_id),
        FOREIGN KEY (source_id) REFERENCES "{REDSHIFT_SCHEMA}".source_dimension(source_id)
    );

    CREATE TABLE IF NOT EXISTS "{REDSHIFT_SCHEMA}".company_insights (
        company VARCHAR (900),
        snapshot_date DATE,
        total_news INT,
        unique_sources INT,
        earliest_news_date DATE,
        latest_news_date DATE,
        average_headline_length FLOAT,
        PRIMARY KEY (company, snapshot_date)
    );
    """

    connection = engine.connect()

    connection.execute(query)

    connection.close()
