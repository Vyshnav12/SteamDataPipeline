import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from steam_scraper import get_app_list, process_game, save_progress
import config

class TestSteamScraper(unittest.TestCase):

    def setUp(self):
        # Set up any necessary test fixtures
        self.bucket_name = 'test-bucket'
        self.args = MagicMock()
        self.args.sleep = 1
        self.args.retries = 3
        self.args.steamspy = True

    @patch('steam_scraper.load_from_s3')
    @patch('steam_scraper.save_to_s3')
    @patch('steam_scraper.DoRequest')
    def test_get_app_list(self, mock_do_request, mock_save_to_s3, mock_load_from_s3):
        # Test when app list is loaded from S3
        mock_load_from_s3.return_value = ['1', '2', '3']
        result = get_app_list(self.bucket_name, self.args)
        self.assertEqual(result, ['1', '2', '3'])
        mock_load_from_s3.assert_called_once_with(self.bucket_name, config.APPLIST_FILE)

        # Test when app list is not in S3 and needs to be fetched from Steam
        mock_load_from_s3.return_value = None
        mock_do_request.return_value.json.return_value = {
            'applist': {'apps': [{'appid': 1}, {'appid': 2}, {'appid': 3}]}
        }
        result = get_app_list(self.bucket_name, self.args)
        self.assertEqual(result, ['1', '2', '3'])
        mock_do_request.assert_called_once()
        mock_save_to_s3.assert_called_once()

    @patch('steam_scraper.SteamRequest')
    @patch('steam_scraper.SteamSpyRequest')
    @patch('steam_scraper.ParseSteamGame')
    def test_process_game(self, mock_parse_steam_game, mock_steamspy_request, mock_steam_request):
        # Test successful game processing
        mock_steam_request.return_value = {'some': 'data'}
        mock_parse_steam_game.return_value = {'release_date': '2022-01-01', 'name': 'Test Game'}
        mock_steamspy_request.return_value = {'userscore': 80}

        game, status = process_game('123', self.args, set(), set(), 0, 0)
        
        self.assertEqual(status, 'added')
        self.assertIsNotNone(game)
        self.assertEqual(game['name'], 'Test Game')
        self.assertEqual(game['user_score'], 80)

        # Test game not released
        mock_parse_steam_game.return_value = {'release_date': ''}
        game, status = process_game('123', self.args, set(), set(), 0, 0)
        self.assertEqual(status, 'not_released')
        self.assertIsNone(game)

        # Test game discarded
        mock_steam_request.return_value = None
        game, status = process_game('123', self.args, set(), set(), 0, 0)
        self.assertEqual(status, 'discarded')
        self.assertIsNone(game)

    @patch('steam_scraper.save_to_s3')
    def test_save_progress(self, mock_save_to_s3):
        args = MagicMock()
        args.autosave = 10
        notreleased_set = set(['1', '2', '3'])
        discarded_set = set(['4', '5', '6'])

        # Test when autosave condition is met
        save_progress(self.bucket_name, args, notreleased_set, discarded_set, 10, 10)
        self.assertEqual(mock_save_to_s3.call_count, 2)

        # Test when autosave condition is not met
        mock_save_to_s3.reset_mock()
        save_progress(self.bucket_name, args, notreleased_set, discarded_set, 5, 5)
        mock_save_to_s3.assert_not_called()

if __name__ == '__main__':
    unittest.main()
