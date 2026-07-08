import json
import re

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


def test_standardise_context_schema_org_per_term_string_raises():
    jsonld = {"@context": "http://schema.org/name"}

    with pytest.raises(ValueError, match="http://schema.org/name"):
        standardise_context(jsonld)


def test_standardise_context_unrelated_string_raises():
    jsonld = {"@context": "https://example.org/context.jsonld"}

    with pytest.raises(ValueError, match="https://example.org/context.jsonld"):
        standardise_context(jsonld)


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


def test_standardise_context_list_wrong_order_raises():
    jsonld = {"@context": [{"foo": "bar"}, "http://schema.org"]}

    with pytest.raises(ValueError, match=re.escape("[{'foo': 'bar'}, 'http://schema.org']")):
        standardise_context(jsonld)


def test_standardise_context_list_second_element_not_dict_raises():
    jsonld = {"@context": ["http://schema.org", "http://example.org"]}

    with pytest.raises(ValueError, match=re.escape("['http://schema.org', 'http://example.org']")):
        standardise_context(jsonld)


def test_standardise_context_list_first_element_not_schema_org_raises():
    jsonld = {"@context": ["http://example.org", {"foo": "bar"}]}

    with pytest.raises(ValueError, match=re.escape("['http://example.org', {'foo': 'bar'}]")):
        standardise_context(jsonld)


def test_standardise_context_unrelated_dict_raises():
    jsonld = {"@context": {"foo": "bar"}}

    with pytest.raises(ValueError, match=re.escape("{'foo': 'bar'}")):
        standardise_context(jsonld)


def test_standardise_context_dict_with_schema_org_key_raises():
    jsonld = {"@context": {"schema": "http://schema.org"}}

    with pytest.raises(ValueError, match=re.escape("{'schema': 'http://schema.org'}")):
        standardise_context(jsonld)
