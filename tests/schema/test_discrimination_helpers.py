"""Unit tests for the subtype-discrimination helpers, in isolation.

These test ``_is_wrapped``, ``_collect_annotated_schema_types``, ``_discriminate_value``,
and the ``__init_subclass__`` registry side effect directly, via small local
``SchemaOrgBase`` subclasses — the helpers are project-agnostic, so the subclasses under
test don't need to be real schema.org types. They are still built against the real
generated ``schemaorg_models.py``, not ``schema_org_model_generator``'s copy, so the
helpers are exercised as actually shipped. ``tests/schema/test_generated_discrimination.py``
covers the same mechanism wired into real generated fields.
"""

from typing import Union

import pytest
from pydantic import Field

from metadata_converter.schema_org_models.schemaorg_models import (
    _SCHEMA_TYPE_REGISTRY,
    SchemaOrgBase,
    _collect_annotated_schema_types,
    _discriminate_value,
    _is_wrapped,
)

# Forces the deferred `PropertyValue` forward reference to resolve now, against
# schemaorg_models.py's namespace, before a subclass defined here needs it.
SchemaOrgBase.model_rebuild(force=True)


class Foo(SchemaOrgBase):
    type: str = Field(default="Foo", alias="@type")
    name: str | None = None


class Bar(Foo):
    type: str = Field(default="Bar", alias="@type")


class Other(SchemaOrgBase):
    type: str = Field(default="Other", alias="@type")


@pytest.mark.parametrize(
    "annotation, expected",
    [
        (Union[str, int], True),
        (str | int, True),
        (list[str], True),
        (set[str], True),
        (tuple[str, int], True),
        (str, False),
    ],
)
def test_is_wrapped_true_only_for_union_and_container_origins(annotation, expected):
    assert _is_wrapped(annotation) == expected


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
def test_collect_annotated_schema_types_extracts_referenced_classes(annotation, expected):
    assert _collect_annotated_schema_types(annotation) == expected


@pytest.mark.parametrize(
    "raw_input",
    [
        {"@type": "Bar", "name": "x"},
        {"type": "Bar", "name": "x"},
    ],
)
def test_discriminate_value_dict_resolves_to_registered_subtype(raw_input):
    result = _discriminate_value(raw_input, Foo)

    assert isinstance(result, Bar)
    assert result.name == "x"


@pytest.mark.parametrize(
    "raw_input, match",
    [
        ({"@type": "Ghost"}, r"'Ghost' is not a known subtype of Foo"),
        ({"@type": "Other"}, r"'Other' is not a known subtype of Foo"),
    ],
)
def test_discriminate_value_unknown_or_unrelated_type_raises(raw_input, match):
    with pytest.raises(ValueError, match=match):
        _discriminate_value(raw_input, Foo)


@pytest.mark.parametrize(
    "raw_input, annotation",
    [
        ({"name": "x"}, Foo),
        ("a plain string", Foo),
        ({"@type": "Bar"}, str),
    ],
)
def test_discriminate_value_unresolvable_input_passes_through(raw_input, annotation):
    result = _discriminate_value(raw_input, annotation)

    assert result == raw_input


def test_discriminate_value_list_resolves_each_item_independently():
    raw_input = [{"@type": "Bar", "name": "a"}, {"@type": "Bar", "name": "b"}]

    result = _discriminate_value(raw_input, Foo)

    assert isinstance(result, list)
    assert isinstance(result[0], Bar)
    assert result[0].name == "a"
    assert isinstance(result[1], Bar)
    assert result[1].name == "b"


def test_new_subclass_registers_in_type_registry():
    class Baz(SchemaOrgBase):
        type: str = Field(default="Baz", alias="@type")

    assert _SCHEMA_TYPE_REGISTRY["Baz"] is Baz
