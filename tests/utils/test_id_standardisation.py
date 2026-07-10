from metadata_converter.utils.jsonld import standardise_id


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
