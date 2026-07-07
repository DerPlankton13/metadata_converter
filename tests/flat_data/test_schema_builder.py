"""Tests for the flat_data schema_builder: AST parsing, column resolution, evaluation."""
import pytest

from metadata_converter.flat_data.transform.schema_builder import (
    ColumnRef,
    Literal,
    Nested,
    NestedList,
    build_nested,
    build_root,
    instantiate,
    parse_mapping,
    resolve_column,
)
from metadata_converter.schema_org_models.schemaorg_models import (
    Organization,
    Person,
    PropertyValue,
)

# ---------------------------------------------------------------------------
# parse_mapping: TOML mapping → typed AST
# ---------------------------------------------------------------------------


def test_parse_literal_prefix_becomes_literal():
    assert parse_mapping("Literal:foo") == Literal(value="foo")


def test_parse_plain_string_becomes_column_ref():
    assert parse_mapping("author:pid") == ColumnRef(name="author:pid")


def test_parse_dict_becomes_nested():
    raw = {"type": "Orcid", "value": "author:pid"}
    assert parse_mapping(raw) == Nested(
        type="Orcid", fields={"value": ColumnRef(name="author:pid")}
    )


def test_parse_list_becomes_nested_list():
    raw = [{"type": "PropertyValue", "name": "Literal:flag"}]
    assert parse_mapping(raw) == NestedList(
        items=[Nested(type="PropertyValue", fields={"name": Literal(value="flag")})]
    )


def test_parse_dict_without_type_raises():
    with pytest.raises(ValueError, match="Missing 'type'"):
        parse_mapping({"name": "author:pid"})


def test_parse_list_with_non_schema_element_raises():
    with pytest.raises(TypeError, match="must be schemas"):
        parse_mapping(["Literal:foo"])


def test_parse_unsupported_node_raises():
    with pytest.raises(TypeError, match="Unsupported mapping node"):
        parse_mapping(123)


# ---------------------------------------------------------------------------
# resolve_column: pure column lookup with NaN stripping
# ---------------------------------------------------------------------------


def test_resolve_column_returns_list_of_values():
    row = {"author:pid": ["0000-0001-2345-6789"]}
    assert resolve_column("author:pid", row) == ["0000-0001-2345-6789"]


def test_resolve_column_drops_nan():
    row = {"author:pid": ["pid1", None, "pid2"]}
    assert resolve_column("author:pid", row) == ["pid1", "pid2"]


def test_resolve_column_returns_empty_when_all_values_are_missing():
    row = {"author:pid": [None]}
    assert resolve_column("author:pid", row) == []


def test_resolve_column_missing_header_raises():
    with pytest.raises(KeyError, match="not found"):
        resolve_column("author:pid", {})


# ---------------------------------------------------------------------------
# build_nested: inner evaluation — fan-out, drop-on-empty, mismatched lengths
# ---------------------------------------------------------------------------


def test_build_nested_literal_decorates_column_data():
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "col"}
    )
    row = {"col": [1]}
    [pv] = build_nested(schema, row)
    assert isinstance(pv, PropertyValue)
    assert pv.name == "flag"
    assert pv.value == 1


def test_build_nested_with_literal_alongside_empty_column_is_dropped():
    """Marker pattern: a literal label whose accompanying column came back empty for this row is dropped."""
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "col"}
    )
    row = {"col": [None]}
    assert build_nested(schema, row) == []


def test_build_nested_with_only_literals_emits_constant():
    """A Nested whose mapping declares no column refs anywhere is a constant; emit one instance regardless of row."""
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "Literal:fixed-value"}
    )
    [pv] = build_nested(schema, {})
    assert isinstance(pv, PropertyValue)
    assert pv.name == "flag"
    assert pv.value == "fixed-value"


def test_build_nested_literal_only_subobject_broadcasts_across_parent_fan_out():
    """A literal-only sub-object is emitted on every instance produced by parent fan-out."""
    schema = parse_mapping(
        {
            "type": "Person",
            "name": "col",
            "affiliation": {"type": "Organization", "name": "Literal:Acme"},
        }
    )
    row = {"col": ["alice", "bob"]}
    [alice, bob] = build_nested(schema, row)
    assert isinstance(alice, Person)
    assert isinstance(alice.affiliation, Organization)
    assert alice.name == "alice"
    assert alice.affiliation.name == "Acme"
    assert isinstance(bob, Person)
    assert isinstance(bob.affiliation, Organization)
    assert bob.name == "bob"
    assert bob.affiliation.name == "Acme"


def test_build_nested_empty_mapping_returns_nothing():
    """A Nested that declares no fields at all produces no instances (no data of any kind)."""
    schema = parse_mapping({"type": "PropertyValue"})
    assert build_nested(schema, {}) == []


def test_build_nested_fans_out_on_multi_value_leaf():
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "col"}
    )
    row = {"col": ["a", "b"]}
    [pv1, pv2] = build_nested(schema, row)
    assert isinstance(pv1, PropertyValue)
    assert (pv1.name, pv1.value) == ("flag", "a")
    assert isinstance(pv2, PropertyValue)
    assert (pv2.name, pv2.value) == ("flag", "b")


def test_build_nested_subobject_with_empty_column_is_dropped():
    # The only row-dependency is a column ref inside the nested affiliation; with
    # that column empty the whole object is dropped (not emitted as a bare literal).
    schema = parse_mapping(
        {
            "type": "Person",
            "name": "Literal:fixed",
            "affiliation": {"type": "Organization", "name": "org_col"},
        }
    )
    assert build_nested(schema, {"org_col": [None]}) == []


def test_build_nested_repeated_with_empty_column_is_dropped():
    # Same as above but the row-dependency lives inside a repeated block.
    schema = parse_mapping(
        {
            "type": "Person",
            "name": "Literal:fixed",
            "affiliation": [{"type": "Organization", "name": "org_col"}],
        }
    )
    assert build_nested(schema, {"org_col": [None]}) == []


def test_build_nested_literal_only_subobject_emits_constant():
    # A literal-only nested sub-object has no row-dependency, so the parent is
    # emitted as a constant even against an empty row.
    schema = parse_mapping(
        {
            "type": "Person",
            "affiliation": {"type": "Organization", "name": "Literal:Acme"},
        }
    )
    [person] = build_nested(schema, {})
    assert isinstance(person, Person)
    assert isinstance(person.affiliation, Organization)
    assert person.affiliation.name == "Acme"


def test_build_nested_literal_only_repeated_emits_constant():
    schema = parse_mapping(
        {
            "type": "Person",
            "affiliation": [{"type": "Organization", "name": "Literal:Acme"}],
        }
    )
    [person] = build_nested(schema, {})
    assert isinstance(person, Person)
    assert isinstance(person.affiliation[0], Organization)
    assert person.affiliation[0].name == "Acme"


def test_build_nested_warns_and_skips_on_mismatched_leaf_lengths(caplog):
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "col_a", "value": "col_b"}
    )
    row = {"col_a": ["a", "b"], "col_b": ["x", "y", "z"]}
    with caplog.at_level("WARNING"):
        result = build_nested(schema, row)
    assert result == []
    assert "PropertyValue" in caplog.text


# ---------------------------------------------------------------------------
# build_root: top-level evaluation — single instance, no fan-out
# ---------------------------------------------------------------------------


def test_build_root_with_only_literal_is_emitted():
    schema = parse_mapping({"type": "PropertyValue", "name": "Literal:static-label"})
    [pv] = build_root(schema, {})
    assert isinstance(pv, PropertyValue)
    assert pv.name == "static-label"


def test_build_root_keeps_multi_value_leaf_as_list_property():
    # No fan-out at top level — multi-values become list-valued properties on
    # the single entity rather than duplicate-@id siblings.
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "col"}
    )
    row = {"col": ["a", "b"]}
    [pv] = build_root(schema, row)
    assert isinstance(pv, PropertyValue)
    assert pv.name == "flag"
    assert pv.value == ["a", "b"]


# ---------------------------------------------------------------------------
# instantiate: validation failures are logged and skipped, not raised
# ---------------------------------------------------------------------------


def test_instantiate_logs_and_skips_on_validation_error(caplog):
    with caplog.at_level("WARNING"):
        result = instantiate(Person, {"@id": ["not", "a", "string"]})

    assert result == []
    assert "Could not create Person" in caplog.text
