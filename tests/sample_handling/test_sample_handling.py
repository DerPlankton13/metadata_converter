import json
import unittest
from unittest.mock import MagicMock, patch

from metadata_converter.biosamples_handling import extract_sample, get_metadata


class TestSampleHandling(unittest.TestCase):
    @patch("metadata_converter.sample_handling.requests.get")
    def test_extract_sample_samea112489011(self, mock_get):
        # Load expected output
        with open("tests/sample_handling/SAMEA112489011_Product.jsonld", "r") as f:
            expected = json.load(f)

        # Load mock data from SAMEA112489011_original.jsonld
        with open("tests/sample_handling/SAMEA112489011_original.jsonld", "r") as f:
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


if __name__ == "__main__":
    unittest.main()
