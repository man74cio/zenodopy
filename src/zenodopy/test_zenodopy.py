import unittest
from unittest.mock import patch, MagicMock,PropertyMock
import zenodopy
from zenodopy import Client  # Import the Client class directly
import json

"""
# Test for zenodopy (MMancini branch)

# usage :
python -m unittest test_zenodopy.py
"""

class TestZenodoClient(unittest.TestCase):

    def setUp(self):
        self.client = zenodopy.Client(sandbox=True)

    def test_initialization(self):
        self.assertTrue(self.client.sandbox)
        self.assertEqual(self.client._endpoint, "https://sandbox.zenodo.org/api")
        self.assertEqual(self.client._doi_pattern, r'^10\.5072/zenodo\.\d+$')

    def test_token_reading(self):
        with patch('zenodopy.Client._read_from_config', new_callable=PropertyMock) as mock_read_config:
            mock_read_config.return_value = "test_token"
            client = Client(sandbox=True)
            self.assertEqual(client._token, "test_token")
            mock_read_config.assert_called_once()

    @patch('requests.get')
    def test_get_depositions(self, mock_get):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = [{"id": 1, "title": "Test Deposition"}]
        mock_get.return_value = mock_response

        result = self.client._get_depositions()
        self.assertEqual(result, [{"id": 1, "title": "Test Deposition"}])

    @patch('requests.get')
    def test_get_depositions_by_id(self, mock_get):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {"id": 1, "title": "Test Deposition"}
        mock_get.return_value = mock_response

        result = self.client._get_depositions_by_id(1)
        self.assertEqual(result, {"id": 1, "title": "Test Deposition"})

    @patch('requests.get')
    def test_get_bucket_by_id(self, mock_get):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {"links": {"bucket": "https://sandbox.zenodo.org/api/files/1234-abcd"}}
        mock_get.return_value = mock_response

        result = self.client._get_bucket_by_id(1)
        self.assertEqual(result, "https://sandbox.zenodo.org/api/files/1234-abcd")


    @patch('requests.post')
    @patch('requests.put')
    def test_create_project(self, mock_put, mock_post):
        # Mock the initial POST request to create the deposition
        mock_post_response = MagicMock()
        mock_post_response.ok = True
        mock_post_response.json.return_value = {
            "id": 1234,
            "links": {"bucket": "https://sandbox.zenodo.org/api/files/1234-abcd"},
        }
        mock_post.return_value = mock_post_response

        # Mock the PUT request to change metadata
        mock_put_response = MagicMock()
        mock_put_response.ok = True
        mock_put.return_value = mock_put_response

        # Call the method
        self.client.create_project("Test Project", "dataset", "Test Description")

        # Assertions
        mock_post.assert_called_once_with(
            f"{self.client._endpoint}/deposit/depositions",
            auth=self.client._bearer_auth,
            data=json.dumps({}),
            headers={'Content-Type': 'application/json'}
        )

        expected_metadata = {
            'metadata': {
                'title': "Test Project",
                'upload_type': "dataset",
                'description': "Test Description",
                'creators': [{"name": "Creator goes here"}]  # Add this line to match the actual behavior
            }
        }

        mock_put.assert_called_once_with(
            f"{self.client._endpoint}/deposit/depositions/1234",
            auth=self.client._bearer_auth,
            data=json.dumps(expected_metadata),
            headers={'Content-Type': 'application/json'}
        )

        self.assertEqual(self.client.deposition_id, 1234)
        self.assertEqual(self.client.bucket, "https://sandbox.zenodo.org/api/files/1234-abcd")
        self.assertEqual(self.client.title, "Test Project")
        self.assertTrue(self.client.associated)
            

    @patch('requests.get')
    def test_set_project(self, mock_get):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "id": 1,
            "title": "Test Project",
            "conceptrecid": "1234",
            "links": {"bucket": "https://sandbox.zenodo.org/api/files/1234-abcd"}
        }
        mock_get.return_value = mock_response

        self.client.set_project(1)
        self.assertEqual(self.client.deposition_id, 1)
        self.assertEqual(self.client.title, "Test Project")
        self.assertEqual(self.client.concept_id, "1234")
        self.assertTrue(self.client.associated)

    @patch('requests.get')
    def test_is_published(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"submitted": True}
        mock_get.return_value = mock_response

        self.client.deposition_id = 1
        result = self.client.is_published
        self.assertTrue(result)

        mock_get.assert_called_once_with(
            "https://sandbox.zenodo.org/api/deposit/depositions/1",
            auth=self.client._bearer_auth
        )

    @patch('requests.get')
    def test_list_files(self, mock_get):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "files": [
                {"filename": "file1.txt"},
                {"filename": "file2.txt"}
            ]
        }
        mock_get.return_value = mock_response

        self.client.deposition_id = 1
        with patch('builtins.print') as mock_print:
            self.client.list_files
            mock_print.assert_any_call("file1.txt")
            mock_print.assert_any_call("file2.txt")

    # Add more tests for other methods as needed

if __name__ == '__main__':
    unittest.main()