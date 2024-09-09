import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import json
from datetime import datetime
from botocore.exceptions import ClientError

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from utils import (save_to_s3, load_from_s3, save_chunk_to_s3, merge_chunks,
                   SanitizeText, Log, ProgressLog, PriceToFloat,
                   load_metadata_index, save_metadata_index, update_metadata_index)
import config

class TestUtils(unittest.TestCase):

    def setUp(self):
        self.mock_s3_client = MagicMock()
        self.bucket_name = 'test-bucket'

    @patch('utils.s3_client')
    def test_save_to_s3(self, mock_s3):
        data = {'key': 'value'}
        save_to_s3(self.bucket_name, 'test.json', data)
        mock_s3.upload_fileobj.assert_called_once()

    @patch('utils.s3_client')
    def test_load_from_s3(self, mock_s3):
        # Test successful load
        mock_s3.download_fileobj = MagicMock()
        mock_s3.download_fileobj.side_effect = lambda bucket, key, file_obj: file_obj.write(json.dumps({'key': 'value'}).encode('utf-8'))
        result = load_from_s3(self.bucket_name, 'test.json')
        self.assertEqual(result, {'key': 'value'})

        # Test NoSuchKey exception
        mock_s3.download_fileobj.side_effect = ClientError({'Error': {'Code': 'NoSuchKey'}}, 'operation_name')
        result = load_from_s3(self.bucket_name, 'nonexistent.json')
        self.assertIsNone(result)

    @patch('utils.save_to_s3')
    def test_save_chunk_to_s3(self, mock_save):
        chunk = {'1': {'name': 'Game 1'}}
        manifest = {'chunks': []}
        result = save_chunk_to_s3(self.bucket_name, chunk, manifest)
        self.assertEqual(len(result['chunks']), 1)
        mock_save.assert_called_once()

    @patch('utils.load_from_s3')
    @patch('utils.save_to_s3')
    def test_merge_chunks(self, mock_save, mock_load):
        mock_load.side_effect = [
            {'chunks': ['chunk_1.json', 'chunk_2.json']},
            {'1': {'name': 'Game 1'}},
            {'2': {'name': 'Game 2'}}
        ]
        merge_chunks(self.bucket_name, 'output.json')
        mock_save.assert_called_once_with(self.bucket_name, 'output.json', {'1': {'name': 'Game 1'}, '2': {'name': 'Game 2'}})

    def test_SanitizeText(self):
        text = "<p>Test   text</p>"
        result = SanitizeText(text)
        self.assertEqual(result, "Test text")

    @patch('utils.logger')
    def test_Log(self, mock_logger):
        Log(config.INFO, "Test message")
        mock_logger.log.assert_called_once()

    @patch('utils.Log')
    def test_ProgressLog(self, mock_Log):
        start_time = datetime.now()
        ProgressLog("Test", 50, 100, start_time)
        mock_Log.assert_called_once()

    def test_PriceToFloat(self):
        self.assertEqual(PriceToFloat("$9.99"), 9.99)
        self.assertEqual(PriceToFloat("Free"), 0.0)

    @patch('utils.load_from_s3')
    def test_load_metadata_index(self, mock_load):
        mock_load.return_value = ['1', '2', '3']
        result = load_metadata_index(self.bucket_name)
        self.assertEqual(result, {'1', '2', '3'})

    @patch('utils.save_to_s3')
    def test_save_metadata_index(self, mock_save):
        metadata = {'1', '2', '3'}
        save_metadata_index(self.bucket_name, metadata)
        mock_save.assert_called_once()
        # Check that the correct bucket and file name were used
        self.assertEqual(mock_save.call_args[0][0], self.bucket_name)
        self.assertEqual(mock_save.call_args[0][1], config.METADATA_FILE)
        # Check that the saved data is a list containing the same elements as the metadata set
        saved_data = mock_save.call_args[0][2]
        self.assertIsInstance(saved_data, list)
        self.assertEqual(set(saved_data), metadata)

    def test_update_metadata_index(self):
        metadata = {'1', '2'}
        chunk = {'3': {}, '4': {}}
        result = update_metadata_index(metadata, chunk)
        self.assertEqual(result, {'1', '2', '3', '4'})

if __name__ == '__main__':
    unittest.main()
