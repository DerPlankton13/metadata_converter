"""Tests for the flat_data broadcast @id reference pipeline.

Covers to_lookup_key (canonical-string normalisation), extract_inline_id_ref_broadcasts
(facade that lifts inline mapping entries into config.broadcast_id_refs),
prepare_id_ref_broadcast (wide-format DataFrame access), and broadcast_id_refs
(schema object manipulation) in isolation so the full Excel-file pipeline is not
needed.
"""

import copy

import pandas as pd
import pytest

from metadata_converter.config import (
    CleaningConfig,
    BroadcastIdRef,
    ExcelExtractorConfig,
    FlatDataConfig,
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
        output_dir=tmp_path / "out",
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


def test_prepare_filter_returns_ref_type_and_matching_ids(tmp_path):
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


def test_prepare_no_filter_returns_all_ids(tmp_path):
    ref = BroadcastIdRef(on_sheet="dataset", property="creator", from_sheet="author")
    config = make_config(tmp_path, [ref])
    data_dict = {"author": author_df(), "dataset": pd.DataFrame()}

    _, _, ids = prepare_id_ref_broadcast(data_dict, config)[0]

    assert ids == ["Person_alice.jsonld", "Person_bob.jsonld"]


def test_prepare_returns_empty_ids_when_filter_matches_nothing(tmp_path):
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
    alice, bob = creator
    assert alice.id == "Person_alice.jsonld"
    assert bob.id == "Person_bob.jsonld"


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


# ---------------------------------------------------------------------------
# extract_inline_id_ref_broadcasts
# ---------------------------------------------------------------------------


def make_config_with_mapping(tmp_path, mapping: dict) -> FlatDataConfig:
    """Build a FlatDataConfig with an arbitrary mapping; no broadcast_id_refs to start."""
    return FlatDataConfig(
        extractor=ExcelExtractorConfig(
            input=tmp_path / "dummy.xlsx",
            sheet_name=list(mapping.keys()),
        ),
        cleaning=CleaningConfig(),
        output_dir=tmp_path / "out",
        mapping=mapping,
    )


def test_inline_broadcast_id_ref_with_filter_extracted_correctly(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {
                    "type": "Person",
                    "id": {
                        "from_sheet": "author",
                        "filter_column": "author:is-dataset-author",
                        "filter_value": 1,
                    },
                },
            },
        },
    )

    extract_inline_id_ref_broadcasts(cfg)

    assert len(cfg.broadcast_id_refs) == 1
    ref = cfg.broadcast_id_refs[0]
    assert ref.on_sheet == "dataset"
    assert ref.property == "creator"
    assert ref.from_sheet == "author"
    assert ref.filter_column == "author:is-dataset-author"
    assert ref.filter_value == 1


def test_inline_broadcast_id_ref_without_filter_extracted_correctly(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "file": {"type": "Dataset"},
            "dataset": {
                "type": "DataCatalog",
                "dataset": {"type": "Dataset", "id": {"from_sheet": "file"}},
            },
        },
    )

    extract_inline_id_ref_broadcasts(cfg)

    [ref] = cfg.broadcast_id_refs
    assert ref.from_sheet == "file"
    assert ref.filter_column is None
    assert ref.filter_value is None


def test_inline_broadcast_id_ref_removed_from_mapping(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {
                    "type": "Person",
                    "id": {"from_sheet": "author"},
                },
            },
        },
    )

    extract_inline_id_ref_broadcasts(cfg)

    assert cfg.mapping["dataset"] == {"type": "DataCatalog"}


def test_multiple_inline_broadcast_id_refs_all_extracted(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "file": {"type": "Dataset"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {"type": "Person", "id": {"from_sheet": "author"}},
                "dataset": {"type": "Dataset", "id": {"from_sheet": "file"}},
            },
        },
    )

    extract_inline_id_ref_broadcasts(cfg)

    creator_ref, dataset_ref = cfg.broadcast_id_refs
    assert creator_ref.property == "creator"
    assert dataset_ref.property == "dataset"
    assert "creator" not in cfg.mapping["dataset"]
    assert "dataset" not in cfg.mapping["dataset"]


def test_inline_broadcast_id_refs_appended_to_existing_broadcast_id_refs(tmp_path):
    existing = BroadcastIdRef(
        on_sheet="dataset", property="dataset", from_sheet="file"
    )
    cfg = FlatDataConfig(
        extractor=ExcelExtractorConfig(
            input=tmp_path / "dummy.xlsx", sheet_name=["author", "file", "dataset"]
        ),
        cleaning=CleaningConfig(),
        output_dir=tmp_path / "out",
        mapping={
            "author": {"type": "Person"},
            "file": {"type": "Dataset"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {"type": "Person", "id": {"from_sheet": "author"}},
            },
        },
        broadcast_id_refs=[existing],
    )

    extract_inline_id_ref_broadcasts(cfg)

    assert len(cfg.broadcast_id_refs) == 2
    assert cfg.broadcast_id_refs[0] is existing
    assert cfg.broadcast_id_refs[1].property == "creator"


def test_extract_is_idempotent_on_second_call(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {"type": "Person", "id": {"from_sheet": "author"}},
            },
        },
    )

    extract_inline_id_ref_broadcasts(cfg)
    mapping_after_first = copy.deepcopy(cfg.mapping)
    refs_after_first = list(cfg.broadcast_id_refs)
    extract_inline_id_ref_broadcasts(cfg)

    assert cfg.broadcast_id_refs == refs_after_first
    assert cfg.mapping == mapping_after_first


def test_dict_with_id_as_string_passes_through_unchanged(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "dataset": {
                "type": "DataCatalog",
                "creator": {"type": "Person", "id": "author:pid"},
            },
        },
    )

    extract_inline_id_ref_broadcasts(cfg)

    assert cfg.broadcast_id_refs == []
    assert cfg.mapping["dataset"]["creator"] == {
        "type": "Person",
        "id": "author:pid",
    }


def test_dict_with_id_dict_without_from_sheet_passes_through_unchanged(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "dataset": {
                "type": "DataCatalog",
                "creator": {
                    "type": "Person",
                    "id": {"something_else": "foo"},
                },
            },
        },
    )

    extract_inline_id_ref_broadcasts(cfg)

    assert cfg.broadcast_id_refs == []
    assert cfg.mapping["dataset"]["creator"] == {
        "type": "Person",
        "id": {"something_else": "foo"},
    }


def test_inline_broadcast_id_ref_missing_type_raises(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {"id": {"from_sheet": "author"}},
            },
        },
    )

    with pytest.raises(ValueError, match="missing `type`"):
        extract_inline_id_ref_broadcasts(cfg)


def test_inline_broadcast_id_ref_with_extra_outer_key_raises(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {
                    "type": "Person",
                    "id": {"from_sheet": "author"},
                    "name": "stray",
                },
            },
        },
    )

    with pytest.raises(ValueError, match="unexpected keys"):
        extract_inline_id_ref_broadcasts(cfg)


def test_inline_broadcast_id_ref_with_extra_inner_key_raises(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {
                    "type": "Person",
                    "id": {"from_sheet": "author", "garbage": 1},
                },
            },
        },
    )

    with pytest.raises(ValueError, match="garbage"):
        extract_inline_id_ref_broadcasts(cfg)


@pytest.mark.parametrize(
    "id_dict",
    [
        pytest.param({"from_sheet": "author", "filter_column": "x"}, id="column-only"),
        pytest.param({"from_sheet": "author", "filter_value": 1}, id="value-only"),
    ],
)
def test_inline_broadcast_id_ref_with_half_filter_raises(tmp_path, id_dict):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {"type": "Person", "id": id_dict},
            },
        },
    )

    with pytest.raises(ValueError, match="filter_column and filter_value"):
        extract_inline_id_ref_broadcasts(cfg)


def test_inline_broadcast_id_ref_with_unknown_from_sheet_raises(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "dataset": {
                "type": "DataCatalog",
                "creator": {
                    "type": "Person",
                    "id": {"from_sheet": "missing"},
                },
            },
        },
    )

    with pytest.raises(ValueError, match="unknown sheet 'missing'"):
        extract_inline_id_ref_broadcasts(cfg)


def test_inline_broadcast_id_ref_with_type_mismatch_raises(tmp_path):
    cfg = make_config_with_mapping(
        tmp_path,
        {
            "author": {"type": "Person"},
            "dataset": {
                "type": "DataCatalog",
                "creator": {
                    "type": "Organization",
                    "id": {"from_sheet": "author"},
                },
            },
        },
    )

    with pytest.raises(ValueError, match="does not match"):
        extract_inline_id_ref_broadcasts(cfg)
