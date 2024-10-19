import unittest
import pandas as pd
from unittest.mock import patch
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bronze.API_company_extraction import company_profile_table  

class TestCompanyProfileTableDataTypes(unittest.TestCase):

    @patch('finnhub.Client.company_profile2')
    def test_data_types(self, mock_company_profile):
        # Simulate API response for company profile with only the required fields
        mock_company_profile.return_value = {
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "currency": "USD",
            "country": "US",
            "exchange": "NASDAQ",
            "finnhubIndustry": "Technology",
            "weburl": "https://www.apple.com"
        }

        # Call the function with mocked API response
        result = company_profile_table(symbol="AAPL", api_key="fake_key")

        # Define expected types for the selected columns
        expected_types = {
            "name": str,
            "ticker": str,
            "currency": str,
            "country": str,
            "exchange": str,
            "industry": str,
            "weburl": str
        }

        # Check that the data types of the DataFrame columns match the expected types
        for column, expected_type in expected_types.items():
            for value in result[column]:
                self.assertIsInstance(value, expected_type, 
                                      f"Column '{column}' should be of type {expected_type}, but got {type(value)}.")

if __name__ == '__main__':
    unittest.main()
#lets try and test if liting is working