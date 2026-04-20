import json
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from metadata_converter.sample_handling import get_metadata, extract_sample


class TestSampleHandling(unittest.TestCase):

    @patch('metadata_converter.sample_handling.requests.get')
    def test_extract_sample_samea112489011(self, mock_get):
        # Load expected output
        with open('tests/sample_handling/BiosamplesMappingProduct.jsonld', 'r') as f:
            expected = json.load(f)

        # Load mock data from BiosamplesOriginal.jsonld
        with open('tests/sample_handling/BiosamplesOriginal.jsonld', 'r') as f:
            mock_data = json.load(f)

        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = mock_data
        mock_get.return_value = mock_response

        # Call the function
        sample_id = "SAMEA112489011"
        data = get_metadata(sample_id)
        result = extract_sample(data, sample_id)

        # Assert the result matches expected
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
