import json

import pytest

from metadata_converter.load import load_to_jsonld, standardise_context, standardise_id
from metadata_converter.schema_org_models.schemaorg_models import Person


def test_standardise_id_hashed_id_unchanged():
    jsonld = {"@type": "Person", "@id": "Person_ABCDEFGHIJKLMNOPQRSTUV.jsonld", "name": "Ada"}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_ABCDEFGHIJKLMNOPQRSTUV.jsonld"
    assert "identifier" not in result
    assert result["name"] == "Ada"


def test_standardise_id_hashed_id_with_path_unchanged():
    jsonld = {
        "@type": "Person",
        "@id": "https://example.org/Person_ABCDEFGHIJKLMNOPQRSTUV.jsonld",
    }

    result = standardise_id(jsonld)

    assert result["@id"] == "https://example.org/Person_ABCDEFGHIJKLMNOPQRSTUV.jsonld"
    assert "identifier" not in result


def test_standardise_id_hashed_id_without_suffix_unchanged():
    jsonld = {"@type": "Person", "@id": "Person_ABCDEFGHIJKLMNOPQRSTUV"}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_ABCDEFGHIJKLMNOPQRSTUV"
    assert "identifier" not in result


def test_standardise_id_wrong_type_prefix_replaced():
    jsonld = {"@type": "Person", "@id": "Product_ABCDEFGHIJKLMNOPQRSTUV.jsonld"}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_RmE8cW95SDbeLiJs_xdhfT.jsonld"
    assert result["identifier"] == "Product_ABCDEFGHIJKLMNOPQRSTUV.jsonld"


def test_standardise_id_short_hash_replaced():
    jsonld = {"@type": "Person", "@id": "Person_abc.jsonld"}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_RmE8cW95SDbeLiJs_xdhfT.jsonld"
    assert result["identifier"] == "Person_abc.jsonld"


def test_standardise_id_arbitrary_id_replaced_and_kept_as_identifier():
    jsonld = {"@type": "Person", "@id": "orig-id", "name": "Ada"}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_cm1t_5rn_rz3CMLT2Mo0Ro.jsonld"
    assert result["identifier"] == "orig-id"
    assert result["name"] == "Ada"


def test_standardise_id_id_already_in_identifier_not_duplicated():
    jsonld = {"@type": "Person", "@id": "orig-id", "identifier": ["orig-id", "other"]}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_eVzZhKqCN9BXGy0mhryR78.jsonld"
    assert result["identifier"] == ["orig-id", "other"]


def test_standardise_id_id_already_in_url_not_duplicated():
    jsonld = {"@type": "Person", "@id": "orig-id", "url": "orig-id"}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_xnGa3-16Aw0UBA6S2DKRJW.jsonld"
    assert "identifier" not in result


def test_standardise_id_missing_id_hashed_without_identifier():
    jsonld = {"@type": "Person", "name": "Ada"}

    result = standardise_id(jsonld)

    assert result["@id"] == "Person_cm1t_5rn_rz3CMLT2Mo0Ro.jsonld"
    assert "identifier" not in result


def test_load_to_jsonld_writes_standardised_file(tmp_path):
    schema = Person(id="orig-id", name="Ada Lovelace")

    load_to_jsonld(schema, tmp_path)

    written = tmp_path / "Person_wucYhXNELrQ5sfY_WAt3wA.jsonld"
    assert written.exists()
    content = json.loads(written.read_text())
    assert content["@context"] == {"@vocab": "https://schema.org/"}
    assert content["@id"] == "Person_wucYhXNELrQ5sfY_WAt3wA.jsonld"
    assert content["identifier"] == "orig-id"
    assert content["name"] == "Ada Lovelace"


def test_load_to_jsonld_string_output_dir_writes_file(tmp_path):
    schema = Person(id="orig-id", name="Ada Lovelace")

    load_to_jsonld(schema, str(tmp_path))

    assert (tmp_path / "Person_wucYhXNELrQ5sfY_WAt3wA.jsonld").exists()


def test_standardise_context_missing_sets_default_vocab():
    jsonld = {}

    result = standardise_context(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


def test_standardise_context_already_normalised_unchanged():
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


def test_standardise_context_non_schema_string_becomes_list_with_vocab():
    jsonld = {"@context": "https://example.org/context.jsonld"}

    result = standardise_context(jsonld)

    assert result["@context"] == [
        "https://example.org/context.jsonld",
        {"@vocab": "https://schema.org/"},
    ]


def test_standardise_context_dict_schema_org_key_replaced_others_kept():
    jsonld = {"@context": {"schema": "http://schema.org", "foo": "bar"}}

    result = standardise_context(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/", "foo": "bar"}


def test_standardise_context_dict_no_schema_org_adds_vocab():
    jsonld = {"@context": {"foo": "bar"}}

    result = standardise_context(jsonld)

    assert result["@context"] == {"foo": "bar", "@vocab": "https://schema.org/"}


def test_standardise_context_per_term_iris_unchanged():
    jsonld = {
        "@context": {
            "name": "http://schema.org/name",
            "image": {"@id": "http://schema.org/image", "@type": "@id"},
        }
    }

    result = standardise_context(jsonld)

    assert result["@context"] == {
        "name": "http://schema.org/name",
        "image": {"@id": "http://schema.org/image", "@type": "@id"},
    }


def test_standardise_context_list_schema_org_string_and_dict_merged():
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
