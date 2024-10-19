import os
from dotenv import load_dotenv

DIR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(DIR_PATH, '.env'))

# Get the Redshift parameters from environment variables
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DBNAME = os.getenv('REDSHIFT_DBNAME')
REDSHIFT_SCHEMA = os.getenv('REDSHIFT_SCHEMA')

#Get API key
api_key = os.getenv('API_KEY')

#List of companies
companies=['AAPL','AMZN']