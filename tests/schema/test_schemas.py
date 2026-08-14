"""Tests that real schema.org example documents are faithfully represented by the models.

Each example is parsed and must round-trip exactly, proving no content is dropped or
coerced.
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
    QualitativeValue,
    QuantitativeValue,
    StructuredValue,
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
            {"additional_prop": {"@type": "DefinedTerm", "name": "x", "termCode": "y"}},
            PropertyValue(
                name="additional_prop",
                value="x",
                valueReference=DefinedTerm(name="x", termCode="y"),
            ),
            id="additional_prop_is_defined_term",
        ),
        pytest.param(
            {"additional_prop": {"@type": "QualitativeValue", "name": "high"}},
            PropertyValue(
                name="additional_prop",
                value="high",
                valueReference=QualitativeValue(name="high"),
            ),
            id="additional_prop_is_enumeration_subtype",
        ),
        pytest.param(
            {"additional_prop": {"@type": "DefinedTerm", "termCode": "DS06"}},
            PropertyValue(
                name="additional_prop",
                valueReference=DefinedTerm(termCode="DS06"),
            ),
            id="additional_prop_is_defined_term_without_name",
        ),
        pytest.param(
            {"additional_prop": {"@type": "DefinedTerm", "name": ["x", "y"]}},
            PropertyValue(
                name="additional_prop",
                valueReference=DefinedTerm(name=["x", "y"]),
            ),
            id="additional_prop_is_defined_term_with_list_name",
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
            id="registered_type_not_representable",
        ),
        pytest.param(
            {"additional_prop": {"@type": "DefinedTerm", "termCode": {"bad": "shape"}}},
            "DefinedTerm",
            id="defined_term_invalid",
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


def test_extra_property_needing_widened_value_raises():
    """A type representable in neither ``value`` nor ``valueReference`` is surfaced.

    Carrying one would mean widening ``PropertyValue.value`` past the schema.org
    definition, producing a file that can be written but not read back. Whether to
    support reading it or to drop it is undecided, so it must not pass silently.
    """
    data = {
        "@type": "Person",
        "name": "Jane Doe",
        "additional_prop": {
            "@type": "PropertyValueSpecification",
            "valueName": "query",
        },
    }

    with pytest.raises(NotImplementedError, match="PropertyValueSpecification"):
        Person(**data)


def test_extra_property_untyped_dict_is_kept():
    """A dict asserting no ``@type`` is still kept, rather than dropped.

    Nothing identifies which class it describes, so it is modelled as the one model
    type ``value`` admits — preserving the data beats discarding it. Keys that type
    does not declare are themselves kept, one level further down.
    """
    data = {
        "@type": "Person",
        "name": "Jane Doe",
        "additional_prop": {"name": "x", "termCode": "y"},
    }

    model = Person(**data)

    assert model.additionalProperty == PropertyValue(
        name="additional_prop",
        value=StructuredValue(
            name="x", additionalProperty=PropertyValue(name="termCode", value="y")
        ),
    )


def test_defined_term_extra_property_round_trips():
    """An extra property carrying a term survives a write/read cycle unchanged.

    Regression test for a load/uplift asymmetry: the term used to be written into
    ``PropertyValue.value`` with ``model_construct``, bypassing validation, so the
    file it produced was rejected on read and the whole entity was skipped.
    """
    data = {
        "@type": "Dataset",
        "@id": "https://example.org/dataset/1",
        "name": "A dataset",
        "disciplines": {
            "@type": "DefinedTerm",
            "name": "Cross-discipline",
            "termCode": "DS06",
            "inDefinedTermSet": "P08 (SEADATANET PARAMETER DISCIPLINES)",
        },
    }

    model = get_schema("Dataset")(**data)
    dumped = model.model_dump(by_alias=True, exclude_none=True)
    reloaded = get_schema("Dataset")(**dumped)

    assert reloaded == model


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


def test_scalar_extra_property_round_trips():
    """An extra property holding a plain scalar survives a write/read cycle unchanged.

    This is the shape the loaders produce most often, so a file written by one stage
    has to be readable by the next.
    """
    model = Person(**{"name": "Jane Doe", "dateReleased": "2023-01-01"})

    reloaded = Person(**model.model_dump(by_alias=True, exclude_none=True))

    assert reloaded == model
    assert reloaded.additionalProperty == PropertyValue(
        name="dateReleased", value="2023-01-01"
    )


def test_merged_additional_property_round_trips():
    """A merge leaves a list, which has to read back as a list of the same values.

    The single-value and list forms are both legal, so the collapse in
    ``_merge_additional_property`` must not change shape across a write/read cycle.
    """
    model = Person(
        **{
            "name": "Jane Doe",
            "additionalProperty": PropertyValue(name="existing", value=1),
            "additional_prop": "new value",
        }
    )

    reloaded = Person(**model.model_dump(by_alias=True, exclude_none=True))

    assert reloaded == model


def test_extra_property_assigned_after_construction_round_trips():
    """Assigning an extra gives the same readable file as passing it to the constructor.

    ``__setattr__`` routes the assignment into ``additionalProperty`` itself, so this
    covers a code path the constructor tests never reach.
    """
    model = Person(name="Jane Doe")
    model.dateReleased = "2023-01-01"

    reloaded = Person(**model.model_dump(by_alias=True, exclude_none=True))

    assert reloaded == model
    assert reloaded == Person(**{"name": "Jane Doe", "dateReleased": "2023-01-01"})
