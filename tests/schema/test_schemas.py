"""Tests that real schema.org example documents are faithfully represented by the models.

Each example is parsed and then ``validate_strict``-checked — so a field not declared
on the models (at any depth) is rejected rather than silently kept by ``extra="allow"`` —
and must round-trip exactly, proving no content is dropped or coerced.
"""
import json
from pathlib import Path

import pytest

from metadata_converter import get_schema
from metadata_converter.schema_org_models.schemaorg_models import validate_strict

DATA_DIR = Path(__file__).parent / "data"

# Each example is a real schema.org JSON-LD document. Two are out of scope:
# Example3 uses the Role pattern (a Role carrying an arbitrary role-qualified property),
# which the models don't represent, and Example7 is a multi-node @graph document with no
# single top-level @type. Both xfail (strict, so they flag us if support ever lands).
EXAMPLE_FILES = [
    "Example1.jsonld",
    "Example2.jsonld",
    pytest.param(
        "Example3.jsonld",
        marks=pytest.mark.xfail(reason="Role pattern not modelled", strict=True),
    ),
    "Example4.jsonld",
    "Example5.jsonld",
    "Example6.jsonld",
    pytest.param(
        "Example7.jsonld",
        marks=pytest.mark.xfail(
            reason="@graph document — no single top-level @type", strict=True
        ),
    ),
    "Example8.jsonld",
    "Example9.jsonld",
]


@pytest.mark.parametrize("filename", EXAMPLE_FILES)
def test_example_is_strictly_modelled_and_round_trips(filename):
    data = json.loads((DATA_DIR / filename).read_text())
    data.pop("@context", None)

    model = get_schema(data["@type"])(**data)

    validate_strict(model)
    assert model.model_dump(by_alias=True, exclude_none=True) == data
