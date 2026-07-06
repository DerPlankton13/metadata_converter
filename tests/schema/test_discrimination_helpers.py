"""Unit tests for the subtype-discrimination helpers, in isolation.

These test ``_is_union``, ``_referenced_subtypes``, ``_discriminate_value``, and the
``__init_subclass__`` registry side effect directly, via small local ``SchemaOrgBase``
subclasses — they don't need the real generated ``schemaorg_models.py`` classes, since
the helpers are project-agnostic. ``tests/schema/test_generated_discrimination.py``
covers the same mechanism wired into real generated fields.
"""
from typing import Union, get_origin

import pytest
from pydantic import Field

from metadata_converter.schema_org_models import schema_org_model_generator as generator_module
from metadata_converter.schema_org_models.schema_org_model_generator import (
    _SCHEMA_TYPE_REGISTRY,
    SchemaOrgBase,
    _discriminate_value,
    _is_union,
    _referenced_subtypes,
)

# SchemaOrgBase.additionalProperty forward-references `PropertyValue`, which is only
# defined in the real generated schemaorg_models.py. Building any subclass defined here
# instead needs a stand-in resolvable from this module's own globals.
generator_module.PropertyValue = SchemaOrgBase
SchemaOrgBase.model_rebuild(force=True)


class Foo(SchemaOrgBase):
    type: str = Field(default="Foo", alias="@type")
    name: str | None = None


class Bar(Foo):
    type: str = Field(default="Bar", alias="@type")


class Other(SchemaOrgBase):
    type: str = Field(default="Other", alias="@type")


@pytest.mark.parametrize(
    "origin, expected",
    [
        (get_origin(Union[str, int]), True),
        (get_origin(str | int), True),
        (get_origin(list[str]), False),
        (get_origin(str), False),
    ],
)
def test_is_union_true_only_for_union_origins(origin, expected):
    assert _is_union(origin) == expected


@pytest.mark.parametrize(
    "annotation, expected",
    [
        (Foo, {Foo}),
        (str, set()),
        (Foo | str, {Foo}),
        (list[Foo], {Foo}),
        (list[Foo | Bar], {Foo, Bar}),
    ],
)
def test_referenced_subtypes_extracts_referenced_classes(annotation, expected):
    assert _referenced_subtypes(annotation) == expected


@pytest.mark.parametrize(
    "value",
    [
        {"@type": "Bar", "name": "x"},
        {"type": "Bar", "name": "x"},
    ],
)
def test_discriminate_value_dict_resolves_to_registered_subtype(value):
    result = _discriminate_value(value, {Foo}, _SCHEMA_TYPE_REGISTRY)

    assert isinstance(result, Bar)
    assert result.name == "x"


@pytest.mark.parametrize(
    "value, match",
    [
        ({"@type": "Ghost"}, r"'Ghost' is not a known subtype of Foo"),
        ({"@type": "Other"}, r"'Other' is not a known subtype of Foo"),
    ],
)
def test_discriminate_value_unknown_or_unrelated_type_raises(value, match):
    with pytest.raises(ValueError, match=match):
        _discriminate_value(value, {Foo}, _SCHEMA_TYPE_REGISTRY)


@pytest.mark.parametrize(
    "value, base_classes",
    [
        ({"name": "x"}, {Foo}),
        ("a plain string", {Foo}),
        ({"@type": "Bar"}, set()),
    ],
)
def test_discriminate_value_unresolvable_input_passes_through(value, base_classes):
    result = _discriminate_value(value, base_classes, _SCHEMA_TYPE_REGISTRY)

    assert result == value


def test_discriminate_value_list_resolves_each_item_independently():
    value = [{"@type": "Bar", "name": "a"}, {"@type": "Bar", "name": "b"}]

    result = _discriminate_value(value, {Foo}, _SCHEMA_TYPE_REGISTRY)

    assert isinstance(result, list)
    assert isinstance(result[0], Bar)
    assert result[0].name == "a"
    assert isinstance(result[1], Bar)
    assert result[1].name == "b"


def test_new_subclass_registers_in_type_registry():
    class Baz(SchemaOrgBase):
        type: str = Field(default="Baz", alias="@type")

    assert _SCHEMA_TYPE_REGISTRY["Baz"] is Baz
