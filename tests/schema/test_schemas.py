"""Tests that real schema.org example documents are faithfully represented by the models.

Each example is parsed and then ``validate_strict``-checked — so a field not declared
on the models (at any depth) is rejected rather than silently kept by ``extra="allow"`` —
and must round-trip exactly, proving no content is dropped or coerced.
"""

import json
from pathlib import Path

import pytest

from metadata_converter import get_schema
from metadata_converter.schema_org_models.schemaorg_models import (
    Person,
    validate_strict,
)

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


@pytest.mark.parametrize(
    "extra_input, expected_result",
    [
        pytest.param(
            {"additional_prop": "who knows what this is?"},
            "who knows what this is?",
            id="untyped_scalar",
        ),
        pytest.param(
            {"additional_prop": {"@type": "Person", "name": "Peter Lustig"}},
            Person(name="Peter Lustig"),
            id="typed_nested_dict",
        ),
        pytest.param(
            {"additional_prop": {"prop1": "hi", "prop2": 123}},
            {"prop1": "hi", "prop2": 123},
            id="untyped_nested_dict",
        ),
        pytest.param(
            {
                "additional_prop": {
                    "prop1": "hi",
                    "nested_Person": {"@type": "Person", "name": "Peter Lustig"},
                }
            },
            {"prop1": "hi", "nested_Person": Person(name="Peter Lustig")},
            id="deep_nested_typed_dict",
        ),
        pytest.param(
            {"additional_prop": ["prop1", {"@type": "Person", "name": "Peter Lustig"}]},
            ["prop1", Person(name="Peter Lustig")],
            id="list_mixed_types",
        ),
    ],
)
def test_extra_property_validation(extra_input, expected_result):
    data = {
        "@type": "Person",
        "name": "Jane Doe",
    }
    data.update(extra_input)

    model = Person(**data)

    assert model.name == "Jane Doe"
    assert model.additional_prop == expected_result


def test_extra_property_with_unregistered_type_raises():
    with pytest.raises(ValueError, match="NotARealType"):
        Person(name="Jane Doe", additional_prop={"@type": "NotARealType", "name": "x"})
