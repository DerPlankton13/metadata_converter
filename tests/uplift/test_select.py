"""Tests for selector helpers in ``uplift/select.py``: select_values, unwrap_value, render_ref_id."""
import pytest

from metadata_converter.uplift.select import (
    render_ref_id,
    select_values,
    unwrap_value,
)
from metadata_converter.schema_org_models.custom_models import Orcid
from metadata_converter.schema_org_models.schemaorg_models import (
    Action,
    Dataset,
    Organization,
    Person,
    Product,
    PropertyValue,
)

VALID_ORCID_A = "0000-0001-1111-1111"
VALID_ORCID_B = "0000-0002-2222-2222"


# ---------------------------------------------------------------------------
# select_values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "obj, path, expected",
    [
        pytest.param(Person(name="Ada"), "name", ["Ada"], id="simple-field"),
        pytest.param(Person(name="Ada"), "givenName", [], id="missing-field"),
        pytest.param(
            Dataset(about=Product(identifier="SAMEA001")),
            "about.identifier", ["SAMEA001"],
            id="nested-field",
        ),
        pytest.param(
            Person(identifier=Orcid(value=VALID_ORCID_A)),
            "identifier", [VALID_ORCID_A],
            id="auto-unwrap-property-value",
        ),
        pytest.param(
            Person(identifier=[Orcid(value=VALID_ORCID_A), Orcid(value=VALID_ORCID_B)]),
            "identifier", [VALID_ORCID_A, VALID_ORCID_B],
            id="list-of-property-values",
        ),
        pytest.param(
            Action(agent=[Person(name="A"), Person(name="B")]),
            "agent.name", ["A", "B"],
            id="descend-into-list-elements",
        ),
        pytest.param(None, "anything", [], id="none-input"),
        pytest.param("string", "foo", [], id="scalar-cannot-descend"),
    ],
)
def test_select_values_returns_matching_values_at_path(obj, path, expected):
    assert select_values(obj, path) == expected


def test_select_values_returns_model_without_value_field_as_is():
    org = Organization(name="ACME")
    assert select_values(Person(affiliation=org), "affiliation") == [org]


# ---------------------------------------------------------------------------
# unwrap_value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("x", ["x"], id="scalar"),
        pytest.param(PropertyValue(value="x"), ["x"], id="property-value-unwraps"),
        pytest.param(
            PropertyValue(value=PropertyValue(value="x")), ["x"],
            id="recursive-unwrap",
        ),
        pytest.param(
            [PropertyValue(value="x"), PropertyValue(value="y")], ["x", "y"],
            id="list-flattens-and-unwraps",
        ),
        pytest.param(None, [], id="none"),
    ],
)
def test_unwrap_value_returns_flat_value_list(value, expected):
    assert unwrap_value(value) == expected


def test_unwrap_value_returns_non_property_value_model_as_is():
    org = Organization(name="ACME")
    assert unwrap_value(org) == [org]


# ---------------------------------------------------------------------------
# render_ref_id
# ---------------------------------------------------------------------------


def test_render_ref_id_substitutes_placeholder():
    prod = Product(id="Product_XYZ.jsonld", identifier="SAMEA001")
    assert render_ref_id("Product_{identifier}.jsonld", prod) == "Product_SAMEA001.jsonld"


def test_render_ref_id_returns_none_when_placeholder_unresolvable():
    # identifier is None → select_values returns [] → cannot render
    assert render_ref_id("Product_{identifier}.jsonld", Product()) is None
