import json
import unittest
from unittest.mock import MagicMock, patch

from deepdiff import DeepDiff

from metadata_converter.linked_data.biosamples_handling import (
    extract_sample,
    extract_sampling_action,
    fuse_metadata,
    get_metadata,
)


class TestSampleHandling(unittest.TestCase):
    @patch("metadata_converter.biosamples_handling.requests.get")
    def test_extract_sample_samea112489011(self, mock_get):
        # Load expected output
        with open("tests/sample_handling/Product_SAMEA112489011.jsonld", "r") as f:
            expected = json.load(f)

        # Load mock data from SAMEA112489011_with_units.jsonld (fused data)
        with open("tests/sample_handling/SAMEA112489011_with_units.jsonld", "r") as f:
            mock_data = json.load(f)

        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = mock_data
        mock_get.return_value = mock_response

        # Call the function
        sample_id = "SAMEA112489011"
        data = get_metadata(sample_id)
        result = extract_sample(data, sample_id)

        diff = DeepDiff(expected, result, ignore_order=False)
        if diff:
            self.fail(f"Mismatch:\n{diff.pretty()}")

    def test_fuse_metadata_samea112489011(self):
        # Load expected output
        with open("tests/sample_handling/SAMEA112489011_with_units.jsonld", "r") as f:
            expected = json.load(f)

        # Load input data
        with open("tests/sample_handling/SAMEA112489011_original.jsonld", "r") as f:
            structured = json.load(f)

        with open("tests/sample_handling/SAMEA112489011_original.json", "r") as f:
            unstructured = json.load(f)

        # Call fuse_metadata
        result = fuse_metadata(structured, unstructured)

        diff = DeepDiff(expected, result, ignore_order=False)
        if diff:
            self.fail(f"Mismatch:\n{diff.pretty()}")

    @patch("metadata_converter.biosamples_handling.requests.get")
    def test_extract_action_samea112489011(self, mock_get):
        # Load expected output
        with open("tests/sample_handling/Action_SAMEA112489011.jsonld", "r") as f:
            expected = json.load(f)

        # Load mock data from SAMEA112489011_with_units.jsonld (fused data)
        with open("tests/sample_handling/SAMEA112489011_with_units.jsonld", "r") as f:
            mock_data = json.load(f)

        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = mock_data
        mock_get.return_value = mock_response

        # Call the function
        sample_id = "SAMEA112489011"
        data = get_metadata(sample_id)
        result = extract_sampling_action(data, sample_id)

        diff = DeepDiff(expected, result, ignore_order=False)
        if diff:
            self.fail(f"Mismatch:\n{diff.pretty()}")


if __name__ == "__main__":
    unittest.main()
