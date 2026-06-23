"""Tests for the flat_data broadcast @id reference pipeline.

Covers to_lookup_key (canonical-string normalisation), extract_inline_id_ref_broadcasts
(facade that lifts inline mapping entries into config.broadcast_id_refs),
prepare_id_ref_broadcast (wide-format DataFrame access), and broadcast_id_refs
(schema object manipulation) in isolation so the full Excel-file pipeline is not
needed.
"""

import pandas as pd
import pytest

from metadata_converter.config import (
    CleaningConfig,
    BroadcastIdRef,
    ExcelExtractorConfig,
    FlatDataConfig,
    OutputConfig,
)
from metadata_converter.flat_data.transform.id_refs_broadcasting import (
    prepare_id_ref_broadcast,
    extract_inline_id_ref_broadcasts,
    broadcast_id_refs,
    to_lookup_key,
)
from metadata_converter.schema_org_models.schemaorg_models import DataCatalog, Person

# ---------------------------------------------------------------------------
# to_lookup_key
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(1, "1", id="int"),
        pytest.param(" Hello ", "hello", id="strip-and-lowercase"),
        pytest.param(True, "true", id="bool-true"),
        pytest.param(False, "false", id="bool-false"),
        pytest.param(None, None, id="none"),
        pytest.param(1.0, "1", id="integer-valued-float-collapses"),
        pytest.param(2.5, "2.5", id="non-integer-float"),
    ],
)
def test_to_lookup_key_normalises_to_canonical_string(value, expected):
    assert to_lookup_key(value) == expected


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_config(tmp_path, refs: list[BroadcastIdRef]) -> FlatDataConfig:
    """Build a FlatDataConfig where everything except broadcast_id_refs is boilerplate."""
    return FlatDataConfig(
        extractor=ExcelExtractorConfig(
            input=tmp_path / "dummy.xlsx",
            sheet_name=["author", "dataset"],
        ),
        cleaning=CleaningConfig(),
        output=OutputConfig(loaded_base=tmp_path / "out"),
        mapping={
            "author": {"type": "Person"},
            "dataset": {"type": "DataCatalog"},
        },
        broadcast_id_refs=refs,
    )


def author_df(is_dataset_author: tuple[int, int] = (1, 0)) -> pd.DataFrame:
    """Wide-format author DataFrame; the flag tuple sets the is-dataset-author column per row."""
    return pd.DataFrame(
        {
            "@id": ["Person_alice.jsonld", "Person_bob.jsonld"],
            "author:first-name": ["Alice", "Bob"],
            "author:is-dataset-author": list(is_dataset_author),
        }
    )


# ---------------------------------------------------------------------------
# prepare_id_ref_broadcast
# ---------------------------------------------------------------------------


def test_collect_filter_returns_ref_type_and_matching_ids(tmp_path):
    ref = BroadcastIdRef(
        on_sheet="dataset",
        property="creator",
        from_sheet="author",
        filter_column="author:is-dataset-author",
        filter_value=1,
    )
    config = make_config(tmp_path, [ref])
    data_dict = {"author": author_df(), "dataset": pd.DataFrame()}

    [(returned_ref, ref_type, ids)] = prepare_id_ref_broadcast(data_dict, config)

    assert returned_ref is ref
    assert ref_type == "Person"
    assert ids == ["Person_alice.jsonld"]


def test_collect_no_filter_returns_all_ids(tmp_path):
    ref = BroadcastIdRef(on_sheet="dataset", property="creator", from_sheet="author")
    config = make_config(tmp_path, [ref])
    data_dict = {"author": author_df(), "dataset": pd.DataFrame()}

    _, _, ids = prepare_id_ref_broadcast(data_dict, config)[0]

    assert set(ids) == {"Person_alice.jsonld", "Person_bob.jsonld"}


def test_collect_returns_empty_ids_when_filter_matches_nothing(tmp_path):
    ref = BroadcastIdRef(
        on_sheet="dataset",
        property="creator",
        from_sheet="author",
        filter_column="author:is-dataset-author",
        filter_value=1,
    )
    config = make_config(tmp_path, [ref])
    data_dict = {
        "author": author_df(is_dataset_author=(0, 0)),
        "dataset": pd.DataFrame(),
    }

    _, _, ids = prepare_id_ref_broadcast(data_dict, config)[0]

    assert ids == []


# ---------------------------------------------------------------------------
# broadcast_id_refs
# ---------------------------------------------------------------------------


def test_inject_single_ref_sets_scalar_property():
    catalog = DataCatalog(id="DataCatalog_main.jsonld")
    ref = BroadcastIdRef(on_sheet="dataset", property="creator", from_sheet="author")

    results = broadcast_id_refs(
        {"dataset": [catalog]}, [(ref, "Person", ["Person_alice.jsonld"])]
    )

    creator = results["dataset"][0].creator
    assert isinstance(creator, Person)
    assert creator.id == "Person_alice.jsonld"


def test_inject_multiple_refs_sets_list():
    catalog = DataCatalog(id="DataCatalog_main.jsonld")
    ref = BroadcastIdRef(on_sheet="dataset", property="creator", from_sheet="author")
    ids = ["Person_alice.jsonld", "Person_bob.jsonld"]

    results = broadcast_id_refs({"dataset": [catalog]}, [(ref, "Person", ids)])

    creator = results["dataset"][0].creator
    assert isinstance(creator, list)
    assert {c.id for c in creator} == set(ids)


def test_inject_empty_ids_leaves_property_unchanged_and_warns(caplog):
    catalog = DataCatalog(id="DataCatalog_main.jsonld")
    ref = BroadcastIdRef(on_sheet="dataset", property="creator", from_sheet="author")

    with caplog.at_level("WARNING"):
        results = broadcast_id_refs({"dataset": [catalog]}, [(ref, "Person", [])])

    assert results["dataset"][0].creator is None
    assert "no sources for dataset.creator" in caplog.text


# ---------------------------------------------------------------------------
# Integration: collect then inject
# ---------------------------------------------------------------------------


def test_integration_collect_then_inject_applies_filter(tmp_path):
    ref = BroadcastIdRef(
        on_sheet="dataset",
        property="creator",
        from_sheet="author",
        filter_column="author:is-dataset-author",
        filter_value=1,
    )
    config = make_config(tmp_path, [ref])
    data_dict = {"author": author_df(), "dataset": pd.DataFrame()}
    catalog = DataCatalog(id="DataCatalog_main.jsonld")

    collected = prepare_id_ref_broadcast(data_dict, config)
    results = broadcast_id_refs({"dataset": [catalog]}, collected)

    creator = results["dataset"][0].creator
    assert isinstance(creator, Person)
    assert creator.id == "Person_alice.jsonld"
