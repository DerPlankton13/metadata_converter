"""Tests for the flat_data cross-sheet reference pipeline.

Covers _collect_cross_ref_ids (wide-format DataFrame access) and _inject_cross_refs
(schema object manipulation) in isolation so the full Excel-file pipeline is not needed.
"""

import pandas as pd

from metadata_converter.config import (
    CleaningConfig,
    CrossSheetRef,
    ExcelExtractorConfig,
    FlatDataConfig,
    OutputConfig,
)
from metadata_converter.flat_data.run import _collect_cross_ref_ids, _inject_cross_refs
from metadata_converter.schema_org_models.schemaorg_models import DataCatalog, Person

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(tmp_path, cross_sheet_refs: list[CrossSheetRef]) -> FlatDataConfig:
    """Build a FlatDataConfig where everything except cross_sheet_refs is boilerplate."""
    return FlatDataConfig(
        extractor=ExcelExtractorConfig(
            type="excel",
            file_path=tmp_path / "dummy.xlsx",
            sheet_name=["author", "dataset"],
        ),
        cleaning=CleaningConfig(),
        output=OutputConfig(ingested=tmp_path / "out"),
        mapping={
            "author": {"type": "Person"},
            "dataset": {"type": "DataCatalog"},
        },
        cross_sheet_refs=cross_sheet_refs,
    )


def _author_df(is_dataset_author: tuple[int, int] = (1, 0)) -> pd.DataFrame:
    """Wide-format author DataFrame; the flag tuple sets the is-dataset-author column per row."""
    return pd.DataFrame(
        {
            "@id": ["Person_alice.jsonld", "Person_bob.jsonld"],
            "author:first-name": ["Alice", "Bob"],
            "author:is-dataset-author": list(is_dataset_author),
        }
    )


# ---------------------------------------------------------------------------
# _collect_cross_ref_ids
# ---------------------------------------------------------------------------


def test_collect_filter_returns_ref_type_and_matching_ids(tmp_path):
    ref = CrossSheetRef(
        on_sheet="dataset",
        property="creator",
        from_sheet="author",
        filter_column="author:is-dataset-author",
        filter_value=1,
    )
    config = _make_config(tmp_path, [ref])
    data_dict = {"author": _author_df(), "dataset": pd.DataFrame()}

    [(returned_ref, ref_type, ids)] = _collect_cross_ref_ids(data_dict, config)

    assert returned_ref is ref
    assert ref_type == "Person"
    assert ids == ["Person_alice.jsonld"]


def test_collect_no_filter_returns_all_ids(tmp_path):
    ref = CrossSheetRef(on_sheet="dataset", property="creator", from_sheet="author")
    config = _make_config(tmp_path, [ref])
    data_dict = {"author": _author_df(), "dataset": pd.DataFrame()}

    _, _, ids = _collect_cross_ref_ids(data_dict, config)[0]

    assert set(ids) == {"Person_alice.jsonld", "Person_bob.jsonld"}


def test_collect_returns_empty_ids_when_filter_matches_nothing(tmp_path):
    ref = CrossSheetRef(
        on_sheet="dataset",
        property="creator",
        from_sheet="author",
        filter_column="author:is-dataset-author",
        filter_value=1,
    )
    config = _make_config(tmp_path, [ref])
    data_dict = {
        "author": _author_df(is_dataset_author=(0, 0)),
        "dataset": pd.DataFrame(),
    }

    _, _, ids = _collect_cross_ref_ids(data_dict, config)[0]

    assert ids == []


# ---------------------------------------------------------------------------
# _inject_cross_refs
# ---------------------------------------------------------------------------


def test_inject_single_ref_sets_scalar_property():
    catalog = DataCatalog(id="DataCatalog_main.jsonld")
    ref = CrossSheetRef(on_sheet="dataset", property="creator", from_sheet="author")

    results = _inject_cross_refs(
        {"dataset": [catalog]}, [(ref, "Person", ["Person_alice.jsonld"])]
    )

    creator = results["dataset"][0].creator
    assert isinstance(creator, Person)
    assert creator.id == "Person_alice.jsonld"


def test_inject_multiple_refs_sets_list():
    catalog = DataCatalog(id="DataCatalog_main.jsonld")
    ref = CrossSheetRef(on_sheet="dataset", property="creator", from_sheet="author")
    ids = ["Person_alice.jsonld", "Person_bob.jsonld"]

    results = _inject_cross_refs({"dataset": [catalog]}, [(ref, "Person", ids)])

    creator = results["dataset"][0].creator
    assert isinstance(creator, list)
    assert {c.id for c in creator} == set(ids)


def test_inject_empty_ids_leaves_property_unchanged_and_warns(caplog):
    catalog = DataCatalog(id="DataCatalog_main.jsonld")
    ref = CrossSheetRef(on_sheet="dataset", property="creator", from_sheet="author")

    with caplog.at_level("WARNING"):
        results = _inject_cross_refs({"dataset": [catalog]}, [(ref, "Person", [])])

    assert results["dataset"][0].creator is None
    assert "no sources for dataset.creator" in caplog.text


# ---------------------------------------------------------------------------
# Integration: collect then inject
# ---------------------------------------------------------------------------


def test_integration_collect_then_inject_applies_filter(tmp_path):
    ref = CrossSheetRef(
        on_sheet="dataset",
        property="creator",
        from_sheet="author",
        filter_column="author:is-dataset-author",
        filter_value=1,
    )
    config = _make_config(tmp_path, [ref])
    data_dict = {"author": _author_df(), "dataset": pd.DataFrame()}
    catalog = DataCatalog(id="DataCatalog_main.jsonld")

    collected = _collect_cross_ref_ids(data_dict, config)
    results = _inject_cross_refs({"dataset": [catalog]}, collected)

    creator = results["dataset"][0].creator
    assert isinstance(creator, Person)
    assert creator.id == "Person_alice.jsonld"
