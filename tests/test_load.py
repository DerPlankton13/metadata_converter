import json

import pytest

from metadata_converter.load import load_to_jsonld, standardise_context
from metadata_converter.schema_org_models.schemaorg_models import Person


def test_load_to_jsonld_writes_file_with_id_verbatim(tmp_path):
    schema = Person(id="Person_orig-id.jsonld", name="Ada Lovelace")

    load_to_jsonld(schema, tmp_path)

    written = tmp_path / "Person_orig-id.jsonld"
    assert written.exists()
    content = json.loads(written.read_text())
    assert content["@context"] == {"@vocab": "https://schema.org/"}
    assert content["@id"] == "Person_orig-id.jsonld"
    assert content["name"] == "Ada Lovelace"


def test_load_to_jsonld_string_output_dir_writes_file(tmp_path):
    schema = Person(id="Person_orig-id.jsonld", name="Ada Lovelace")

    load_to_jsonld(schema, str(tmp_path))

    assert (tmp_path / "Person_orig-id.jsonld").exists()


def test_standardise_context_missing_sets_default_vocab():
    jsonld = {}

    result = standardise_context(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


def test_standardise_context_already_canonical_is_noop():
    jsonld = {"@context": {"@vocab": "https://schema.org/"}}

    result = standardise_context(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


@pytest.mark.parametrize(
    "context_value",
    [
        pytest.param("schema.org", id="bare-host"),
        pytest.param("http://schema.org", id="http-no-slash"),
        pytest.param("https://schema.org/", id="https-with-slash"),
        pytest.param("HTTPS://SCHEMA.ORG", id="uppercase"),
    ],
)
def test_standardise_context_schema_org_string_variants_normalised(context_value):
    jsonld = {"@context": context_value}

    result = standardise_context(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


def test_standardise_context_schema_org_per_term_string_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": "http://schema.org/name"}

    result = standardise_context(jsonld)

    assert result["@context"] == "http://schema.org/name"
    assert "http://schema.org/name" in caplog.text


def test_standardise_context_unrelated_string_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": "https://example.org/context.jsonld"}

    result = standardise_context(jsonld)

    assert result["@context"] == "https://example.org/context.jsonld"
    assert "https://example.org/context.jsonld" in caplog.text


def test_standardise_context_list_string_and_dict_merged():
    jsonld = {
        "@context": [
            "http://schema.org",
            {
                "OBI": "http://purl.obolibrary.org/obo/OBI_",
                "biosample": "http://identifiers.org/biosample/",
            },
        ]
    }

    result = standardise_context(jsonld)

    assert result["@context"] == {
        "@vocab": "https://schema.org/",
        "OBI": "http://purl.obolibrary.org/obo/OBI_",
        "biosample": "http://identifiers.org/biosample/",
    }


def test_standardise_context_list_extra_dicts_all_merged():
    jsonld = {"@context": ["http://schema.org", {"foo": "bar"}, {"baz": "qux"}]}

    result = standardise_context(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/", "foo": "bar", "baz": "qux"}


def test_standardise_context_list_wrong_order_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": [{"foo": "bar"}, "http://schema.org"]}

    result = standardise_context(jsonld)

    assert result["@context"] == [{"foo": "bar"}, "http://schema.org"]
    assert "{'foo': 'bar'}" in caplog.text


def test_standardise_context_list_second_element_not_dict_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": ["http://schema.org", "http://example.org"]}

    result = standardise_context(jsonld)

    assert result["@context"] == ["http://schema.org", "http://example.org"]
    assert "http://example.org" in caplog.text


def test_standardise_context_list_first_element_not_schema_org_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": ["http://example.org", {"foo": "bar"}]}

    result = standardise_context(jsonld)

    assert result["@context"] == ["http://example.org", {"foo": "bar"}]
    assert "http://example.org" in caplog.text


def test_standardise_context_unrelated_dict_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": {"foo": "bar"}}

    result = standardise_context(jsonld)

    assert result["@context"] == {"foo": "bar"}
    assert "{'foo': 'bar'}" in caplog.text


def test_standardise_context_dict_with_schema_org_key_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": {"schema": "http://schema.org"}}

    result = standardise_context(jsonld)

    assert result["@context"] == {"schema": "http://schema.org"}
    assert "http://schema.org" in caplog.text
