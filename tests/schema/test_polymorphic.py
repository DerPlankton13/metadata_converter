"""Tests for ``polymorphic()``, the subtype-discrimination field wrapper.

A field typed as bare ``Thing`` (or any other schema.org class) currently accepts
any dict regardless of its ``@type`` — Pydantic's union resolution has no
discriminator to check "is this @type actually a subtype of what I declared?".
``polymorphic(base_cls, registry)`` produces an ``Annotated`` type that resolves
``@type``/``type`` against an explicit registry and only accepts objects whose
resolved class is a subtype of ``base_cls``; anything else (unrelated known type,
unregistered type) raises. Unmodeled *properties* on an otherwise-valid object are
unaffected by this — that is still handled by ``extra="allow"`` and is not this
mechanism's concern.
"""
import pytest
from pydantic import BaseModel, ValidationError

from metadata_converter.schema_org_models.schema_org_model_generator import polymorphic
from metadata_converter.schema_org_models.schemaorg_models import (
    CreativeWork,
    Dataset,
    Person,
    Product,
    Thing,
)


def test_subtype_resolves_to_declared_subclass():
    class Container(BaseModel):
        thing: polymorphic(Thing, {"Product": Product})

    container = Container(thing={"type": "Product", "name": "net", "category": "c"})

    assert isinstance(container.thing, Product)
    assert container.thing.name == "net"
    assert container.thing.category == "c"


def test_unrelated_registered_type_raises():
    class Container(BaseModel):
        person: polymorphic(Person, {"Product": Product})

    with pytest.raises(ValidationError):
        Container(person={"type": "Product", "name": "net"})


def test_unregistered_type_raises():
    class Container(BaseModel):
        thing: polymorphic(Thing, {"Product": Product})

    with pytest.raises(ValidationError):
        Container(thing={"type": "TotallyMadeUp", "name": "x"})


def test_extra_field_on_resolved_subtype_still_tolerated():
    class Container(BaseModel):
        work: polymorphic(CreativeWork, {"Dataset": Dataset})

    container = Container(
        work={"type": "Dataset", "name": "ds", "weirdField": "z"}
    )

    assert isinstance(container.work, Dataset)
    assert container.work.name == "ds"
    assert container.work.model_extra == {"weirdField": "z"}
    assert container.work.weirdField == "z"


def test_plain_string_passes_through_unresolved():
    class Container(BaseModel):
        thing: polymorphic(Thing, {"Product": Product}) | str

    container = Container(thing="a plain string")

    assert container.thing == "a plain string"
