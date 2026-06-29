"""Tests for ``validate_strict``: recursive rejection of fields not in the schema.

Models parse with ``extra="allow"`` (unknown fields are kept, not rejected).
``validate_strict`` walks a parsed model and raises if any instance, at any depth,
carries such an extra field — including inside nested models and lists.
"""

import pytest

from metadata_converter.schema_org_models.schemaorg_models import (
    Organization,
    Person,
    PropertyValue,
    validate_strict,
)


def test_validate_strict_passes_clean_model_with_nested_and_list_fields():
    person = Person(
        id="Person_alice.jsonld",
        name="Alice",
        affiliation=Organization(name="ACME"),
        additionalProperty=[PropertyValue(name="ok"), PropertyValue(name="also-ok")],
    )

    validate_strict(person)


def test_validate_strict_raises_on_top_level_extra_field():
    person = Person(id="Person_alice.jsonld", name="Alice", bogus="x")

    with pytest.raises(ValueError, match="bogus"):
        validate_strict(person)


def test_validate_strict_raises_on_nested_extra_field():
    person = Person(
        id="Person_alice.jsonld",
        affiliation=Organization(name="ACME", bogus="x"),
    )

    with pytest.raises(ValueError, match="bogus"):
        validate_strict(person)


def test_validate_strict_raises_on_extra_in_list_item():
    person = Person(
        id="Person_alice.jsonld",
        additionalProperty=[
            PropertyValue(name="ok"),
            PropertyValue(name="bad", bogus="x"),
        ],
    )

    with pytest.raises(ValueError, match="bogus"):
        validate_strict(person)
