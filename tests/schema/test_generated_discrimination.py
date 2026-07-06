"""Tests that the real, generated ``schemaorg_models.py`` actually discriminates subtypes.

``tests/schema/test_discrimination_helpers.py`` tests ``_referenced_subtypes`` and
``_discriminate_value`` in isolation, via hand-built classes and registries. These tests
instead exercise the already-generated production classes directly — proving
``SchemaOrgBase.discriminate_typed_fields`` actually fires on real fields
(``Action.instrument``, ``CreativeWork.character``), not just that the helpers work when
called by hand.
"""
import pytest
from pydantic import ValidationError

from metadata_converter.schema_org_models.schemaorg_models import Action, CreativeWork, Product


def test_action_instrument_resolves_to_registered_subtype():
    action = Action(instrument={"type": "Product", "name": "net", "category": "c"})

    assert isinstance(action.instrument, Product)
    assert action.instrument.name == "net"
    assert action.instrument.category == "c"


def test_creativework_character_unrelated_type_raises():
    with pytest.raises(ValidationError):
        CreativeWork(character={"type": "Product", "name": "net"})


def test_creativework_character_unregistered_type_raises():
    with pytest.raises(ValidationError):
        CreativeWork(character={"type": "TotallyMadeUp"})


def test_action_instrument_accepts_plain_string():
    action = Action(instrument="a plain string")

    assert action.instrument == "a plain string"


def test_action_instrument_list_resolves_each_item_independently():
    action = Action(
        instrument=[
            {"type": "Product", "name": "a"},
            {"type": "Product", "name": "b"},
        ]
    )

    assert isinstance(action.instrument, list)
    assert isinstance(action.instrument[0], Product)
    assert action.instrument[0].name == "a"
    assert isinstance(action.instrument[1], Product)
    assert action.instrument[1].name == "b"
