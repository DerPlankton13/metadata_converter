
"""Tests for the flat_data schema_builder: AST parsing, column resolution, evaluation."""
import pytest

from metadata_converter.flat_data.transform.schema_builder import (
    ColumnRef,
    Literal,
    Nested,
    Repeated,
    build_nested,
    build_root,
    parse_mapping,
    resolve_column,
)

# ---------------------------------------------------------------------------
# parse_mapping: TOML mapping → typed AST
# ---------------------------------------------------------------------------


def test_parse_literal_strips_prefix():
    assert parse_mapping("Literal:foo") == Literal(value="foo")


def test_parse_column_is_default_for_strings():
    assert parse_mapping("author:pid") == ColumnRef(name="author:pid")


def test_parse_nested_dict_becomes_schema():
    raw = {"type": "Orcid", "value": "author:pid"}
    assert parse_mapping(raw) == Nested(
        type="Orcid", fields={"value": ColumnRef(name="author:pid")}
    )


def test_parse_repeated_list_becomes_repeated():
    raw = [{"type": "PropertyValue", "name": "Literal:flag"}]
    assert parse_mapping(raw) == Repeated(
        items=[Nested(type="PropertyValue", fields={"name": Literal(value="flag")})]
    )


def test_parse_dict_without_type_raises():
    with pytest.raises(ValueError, match="Missing 'type'"):
        parse_mapping({"name": "author:pid"})


def test_parse_list_with_non_schema_element_raises():
    with pytest.raises(TypeError, match="must be schemas"):
        parse_mapping(["Literal:foo"])


# ---------------------------------------------------------------------------
# resolve_column: pure column lookup with NaN stripping
# ---------------------------------------------------------------------------


def test_resolve_column_returns_list_of_values():
    entity = {"author:pid": ["0000-0001-2345-6789"]}
    assert resolve_column("author:pid", entity) == ["0000-0001-2345-6789"]


def test_resolve_column_drops_nan():
    entity = {"author:pid": ["pid1", None, "pid2"]}
    assert resolve_column("author:pid", entity) == ["pid1", "pid2"]


def test_resolve_column_returns_empty_when_all_values_are_missing():
    entity = {"author:pid": [None]}
    assert resolve_column("author:pid", entity) == []


# ---------------------------------------------------------------------------
# build_nested: inner evaluation — fan-out, drop-on-empty, mismatched lengths
# ---------------------------------------------------------------------------


def test_build_nested_literal_decorates_column_data():
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "col"}
    )
    entity = {"col": [1]}
    [pv] = build_nested(schema, entity)
    assert pv.name == "flag"
    assert pv.value == 1


def test_build_nested_with_literal_alongside_empty_column_is_dropped():
    """Marker pattern: a literal label whose accompanying column came back empty for this row is dropped."""
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "col"}
    )
    entity = {"col": [None]}
    assert build_nested(schema, entity) == []


def test_build_nested_with_only_literals_emits_constant():
    """A Nested whose mapping declares no column refs anywhere is a constant; emit one instance regardless of row."""
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "Literal:fixed-value"}
    )
    [pv] = build_nested(schema, {})
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
    entity = {"col": ["alice", "bob"]}
    [alice, bob] = build_nested(schema, entity)
    assert alice.name == "alice"
    assert alice.affiliation.name == "Acme"
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
    entity = {"col": ["a", "b"]}
    [pv1, pv2] = build_nested(schema, entity)
    assert (pv1.name, pv1.value) == ("flag", "a")
    assert (pv2.name, pv2.value) == ("flag", "b")


def test_build_nested_warns_and_skips_on_mismatched_leaf_lengths(caplog):
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "col_a", "value": "col_b"}
    )
    entity = {"col_a": ["a", "b"], "col_b": ["x", "y", "z"]}
    with caplog.at_level("WARNING"):
        result = build_nested(schema, entity)
    assert result == []
    assert "PropertyValue" in caplog.text


# ---------------------------------------------------------------------------
# build_root: top-level evaluation — single instance, no fan-out
# ---------------------------------------------------------------------------


def test_build_root_with_only_literal_is_emitted():
    schema = parse_mapping({"type": "PropertyValue", "name": "Literal:static-label"})
    [pv] = build_root(schema, {})
    assert pv.name == "static-label"


def test_build_root_keeps_multi_value_leaf_as_list_property():
    # No fan-out at top level — multi-values become list-valued properties on
    # the single entity rather than duplicate-@id siblings.
    schema = parse_mapping(
        {"type": "PropertyValue", "name": "Literal:flag", "value": "col"}
    )
    entity = {"col": ["a", "b"]}
    [pv] = build_root(schema, entity)
    assert pv.name == "flag"
    assert pv.value == ["a", "b"]
