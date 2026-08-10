"""Tests that real schema.org example documents are faithfully represented by the models.

Each example is parsed and then ``validate_strict``-checked — so a field not declared
on the models (at any depth) is rejected rather than silently kept by ``extra="allow"`` —
and must round-trip exactly, proving no content is dropped or coerced.
"""

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pytest
from pydantic import AnyUrl

from metadata_converter import get_schema
from metadata_converter.schema_org_models.schemaorg_models import (
    DefinedTerm,
    Person,
    PropertyValue,
    QuantitativeValue,
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
            PropertyValue(name="additional_prop", value="who knows what this is?"),
            id="additional_prop_is_string",
        ),
        pytest.param(
            {"additional_prop": 1},
            PropertyValue(name="additional_prop", value=1),
            id="additional_prop_is_int",
        ),
        pytest.param(
            {"additional_prop": 3.14},
            PropertyValue(name="additional_prop", value=3.14),
            id="additional_prop_is_float",
        ),
        pytest.param(
            {"additional_prop": AnyUrl("https://example.com/")},
            PropertyValue(name="additional_prop", value="https://example.com/"),
            id="additional_prop_is_url",
        ),
        pytest.param(
            {"additional_prop": True},
            PropertyValue(name="additional_prop", value=True),
            id="additional_prop_is_bool",
        ),
        pytest.param(
            {"additional_prop": date(2024, 1, 1)},
            PropertyValue(name="additional_prop", value="2024-01-01"),
            id="additional_prop_is_date",
        ),
        pytest.param(
            {"additional_prop": datetime(2024, 1, 1, 12, 30, 0)},
            PropertyValue(name="additional_prop", value="2024-01-01 12:30:00"),
            id="additional_prop_is_datetime",
        ),
        pytest.param(
            {"additional_prop": time(12, 30, 0)},
            PropertyValue(name="additional_prop", value="12:30:00"),
            id="additional_prop_is_time",
        ),
        pytest.param(
            {"additional_prop": timedelta(days=1, hours=2)},
            PropertyValue(name="additional_prop", value="1 day, 2:00:00"),
            id="additional_prop_is_timedelta",
        ),
        pytest.param(
            {"additional_prop": [timedelta(days=1, hours=2), 1, "I am there as well"]},
            [
                PropertyValue(name="additional_prop", value="1 day, 2:00:00"),
                PropertyValue(name="additional_prop", value=1),
                PropertyValue(name="additional_prop", value="I am there as well"),
            ],
            id="additional_prop_is_mixed_list",
        ),
        pytest.param(
            {"additional_prop": ["only one"]},
            PropertyValue(name="additional_prop", value="only one"),
            id="additional_prop_is_single_item_list",
        ),
        pytest.param(
            {"additional_prop": {"@type": "QuantitativeValue", "value": 5}},
            PropertyValue(name="additional_prop", value=QuantitativeValue(value=5)),
            id="additional_prop_is_structuredvalue_subtype",
        ),
        pytest.param(
            {"additional_prop": {"@type": "DefinedTerm", "name": "x"}},
            PropertyValue.model_construct(
                name="additional_prop", value=DefinedTerm(name="x")
            ),
            id="additional_prop_is_allowlisted_type",
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
    assert model.additionalProperty == expected_result


@pytest.mark.parametrize(
    "extra_input, expected_log_fragment",
    [
        pytest.param(
            {"additional_prop": {"@type": "NotARealType", "name": "x"}},
            "NotARealType",
            id="unregistered_type",
        ),
        pytest.param(
            {"additional_prop": {"@type": "Person", "name": "Someone Else"}},
            "Someone Else",
            id="registered_type_outside_allowlist",
        ),
        pytest.param(
            {"additional_prop": {"@type": "DefinedTerm", "termCode": {"bad": "shape"}}},
            "DefinedTerm",
            id="allowlisted_type_invalid",
        ),
    ],
)
def test_extra_property_unbuildable_dict_is_dropped(
    extra_input, expected_log_fragment, caplog
):
    data = {
        "@type": "Person",
        "name": "Jane Doe",
    }
    data.update(extra_input)

    with caplog.at_level("ERROR"):
        model = Person(**data)

    assert model.additionalProperty is None
    assert "additional_prop" in caplog.text
    assert expected_log_fragment in caplog.text


def test_extra_property_merges_with_existing_additional_property():
    data = {
        "@type": "Person",
        "name": "Jane Doe",
        "additionalProperty": PropertyValue(name="existing", value=1),
        "additional_prop": "new value",
    }

    model = Person(**data)

    assert model.additionalProperty == [
        PropertyValue(name="existing", value=1),
        PropertyValue(name="additional_prop", value="new value"),
    ]
