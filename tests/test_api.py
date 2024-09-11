import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from api import DoRequest, SteamRequest, SteamSpyRequest, ParseSteamGame
import config

class TestAPI(unittest.TestCase):

    def setUp(self):
        # Set up any common test data or configurations
        pass

    def tearDown(self):
        # Clean up after each test
        pass

    @patch('api.requests.get')
    def test_do_request_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        url = 'https://example.com'
        result = DoRequest(url)

        self.assertEqual(result, mock_response)
        mock_get.assert_called_once_with(url=url, params=None, timeout=config.DEFAULT_TIMEOUT, allow_redirects=True, headers=None)

    @patch('api.requests.get')
    @patch('api.time.sleep')
    def test_do_request_retry(self, mock_sleep, mock_get):
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 500
        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_get.side_effect = [mock_response_fail, mock_response_success]

        url = 'https://example.com'
        result = DoRequest(url)

        self.assertEqual(result, mock_response_success)
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once()

    @patch('api.DoRequest')
    def test_steam_request_success(self, mock_do_request):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            '123': {
                'success': True,
                'data': {
                    'type': 'game',
                    'is_free': False,
                    'price_overview': {'final_formatted': '$9.99'},
                    'developers': ['Test Developer']
                }
            }
        }
        mock_do_request.return_value = mock_response

        result = SteamRequest('123', 5, 0, 0, 3)

        self.assertIsNotNone(result)
        self.assertEqual(result['type'], 'game')

    @patch('api.DoRequest')
    def test_steam_spy_request_success(self, mock_do_request):
        mock_response = MagicMock()
        mock_response.text = '{"developer": "Test Developer"}'
        mock_response.json.return_value = {"developer": "Test Developer"}
        mock_do_request.return_value = mock_response

        result = SteamSpyRequest('123', 5, 0, 0, 3)

        self.assertIsNotNone(result)
        self.assertEqual(result['developer'], 'Test Developer')

    def test_parse_steam_game(self):
        app_data = {
            'name': 'Test Game',
            'release_date': {'date': '2023-01-01', 'coming_soon': False},
            'required_age': 18,
            'is_free': False,
            'price_overview': {'final_formatted': '$19.99'},
            'dlc': ['1', '2'],
            'detailed_description': 'Test description',
            'about_the_game': 'About the game',
            'short_description': 'Short description',
            'platforms': {'windows': True, 'mac': False, 'linux': True},
            'metacritic': {'score': 85},
            'achievements': {'total': 50},
            'recommendations': {'total': 1000},
            'content_descriptors': {'notes': 'Test notes'},
            'supported_languages': 'English*, French, German*',
            'developers': ['Dev1', 'Dev2'],
            'publishers': ['Pub1'],
            'categories': [{'description': 'Single-player'}, {'description': 'Multi-player'}],
            'genres': [{'description': 'Action'}, {'description': 'Adventure'}],
        }

        result = ParseSteamGame(app_data)

        self.assertEqual(result['name'], 'Test Game')
        self.assertEqual(result['release_date'], '2023-01-01')
        self.assertEqual(result['required_age'], 18)
        self.assertEqual(result['price'], 19.99)
        self.assertEqual(result['dlc_count'], 2)
        self.assertEqual(result['windows'], True)
        self.assertEqual(result['mac'], False)
        self.assertEqual(result['linux'], True)
        self.assertEqual(result['metacritic_score'], 85)
        self.assertEqual(result['achievements'], 50)
        self.assertEqual(result['recommendations'], 1000)
        self.assertEqual(result['supported_languages'], ['English', 'French', 'German'])
        self.assertEqual(result['full_audio_languages'], ['English', 'German'])
        self.assertEqual(result['developers'], ['Dev1', 'Dev2'])
        self.assertEqual(result['publishers'], ['Pub1'])
        self.assertEqual(result['categories'], ['Single-player', 'Multi-player'])
        self.assertEqual(result['genres'], ['Action', 'Adventure'])

if __name__ == '__main__':
    unittest.main()
