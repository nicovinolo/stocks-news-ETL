from sqlalchemy import create_engine
from utils.config import REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DBNAME

def create_redshift_engine_from_env():

    # Create the connection string and engine
    connection_string = f"postgresql://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}"
    engine = create_engine(connection_string)
    
    return engine
