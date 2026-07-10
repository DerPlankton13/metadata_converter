"""Tests for to_lookup_key, the canonical-string normalisation shared by the
flat_data broadcast @id pipeline and the uplift LinkApplier."""

import pytest

from metadata_converter.utils.lookup_key import to_lookup_key


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(1, "1", id="int"),
        pytest.param(" Hello ", "hello", id="strip-and-lowercase"),
        pytest.param(True, "true", id="bool-true"),
        pytest.param(False, "false", id="bool-false"),
        pytest.param(None, None, id="none"),
        pytest.param(1.0, "1", id="integer-valued-float-collapses"),
        pytest.param(2.5, "2.5", id="non-integer-float"),
    ],
)
def test_to_lookup_key_normalises_to_canonical_string(value, expected):
    assert to_lookup_key(value) == expected
