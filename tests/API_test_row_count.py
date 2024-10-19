import unittest
import pandas as pd
import sys
import os
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bronze.API_news_extraction import news_table 

class TestNewsTableRowCount(unittest.TestCase):

    @patch('finnhub.Client.company_news')
    def test_row_count_matches_api_response(self, mock_company_news):
        # Simulate API response with 3 news articles
        mock_company_news.return_value = [
            {
                'category': 'company',
                'datetime': 1728916933,
                'headline': "Why Apple Stock Was Added To 'Tactical Outperform' List Ahead Of Earnings",
                'id': 130444713,
                'image': 'https://media.zenfs.com/en/ibd.com/1bf1a80fbb9bd2c1ce40c3fe9381498f',
                'related': 'AAPL',
                'source': 'Yahoo',
                'summary': 'A Wall Street analyst added Apple stock his firm’s “tactical outperform” list ahead of the company’s fiscal fourth-quarter earnings report.',
                'url': 'https://finnhub.io/api/news?id=95c5951232b339759b68d91e4278656edff59c952f77daaf2afd1b0a6bbbccdb'
            },
            {
                'category': 'company',
                'datetime': 1728916920,
                'headline': 'Apple Stock Has Stalled. Why This Analyst Calls It a ‘Tactical Outperform.’',
                'id': 130444714,
                'image': 'https://s.yimg.com/ny/api/res/1.2/nzI0sBAiT37d4LW8X5dEJw--/YXBwaWQ9aGlnaGxhbmRlcjt3PTEyMDA7aD02MDA-/https://media.zenfs.com/en/Barrons.com/db762e823c94477a1f87ebc895ebdd0e',
                'related': 'AAPL',
                'source': 'Yahoo',
                'summary': "The iPhone maker's September-quarter earnings come at the end of this month. Evercore is calling for investors to position themselves for an upturn.",
                'url': 'https://finnhub.io/api/news?id=0bf16aeaa2c74eaec0d0fc85e5f058461b3c35169d69d6338cfdeace03461da7'
            },
            {
                'category': 'company',
                'datetime': 1728912767,
                'headline': 'Apple’s hearing aid software could change how we deal with hearing loss forever',
                'id': 130444715,
                'image': 'https://www.medicaldevice-network.com/wp-content/uploads/sites/23/2024/10/shutterstock_1565811877.jpg',
                'related': 'AAPL',
                'source': 'Yahoo',
                'summary': 'Did you hear the news? Apple’s AirPods Pro 2 line will transform into hearing aids with new software here’s why it matters.',
                'url': 'https://finnhub.io/api/news?id=0767bb861c59e986abe24116e9a15fa6ca04ca40c1fd363a69a9d296be14e6ac'
            }
        ]

        # Call the function with mocked API response
        result = news_table(symbol="AAPL", api_key="fake_key", date="2023-10-01")

        # Verify the number of rows matches the length of the API response
        self.assertEqual(len(result), 3, "The number of rows in the DataFrame should match the number of articles returned by the API.")

if __name__ == '__main__':
    unittest.main()

