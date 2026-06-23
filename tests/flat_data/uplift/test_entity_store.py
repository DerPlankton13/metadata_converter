"""Tests for ``EntityStore`` in ``uplift/entity_store.py``: load → write round-trip."""


def test_uplifted_corpus_matches_expected_files(uplifted):
    assert set(uplifted) == {
        "Person_alice.jsonld",
        "Person_bob.jsonld",
        "DataCatalog_main.jsonld",
        "Action_analysis1.jsonld",
        "Dataset_file1.jsonld",
        "Product_SAMEA0001.jsonld",
    }
