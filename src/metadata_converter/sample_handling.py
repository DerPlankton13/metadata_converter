from typing import Any

import requests

from metadata_converter.schema_org_models.custom_models import SRA
from metadata_converter.schema_org_models.schemaorg_models import PropertyValue


def get_metadata(sample_id: str) -> dict:

    url = f"https://www.ebi.ac.uk/biosamples/samples/{sample_id}.ldjson"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # raises an exception for 4xx/5xx status codes
        data = response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
    except requests.exceptions.ConnectionError:
        print("Could not connect")
    except requests.exceptions.Timeout:
        print("Request timed out")

    return data


def find_property_value(data: dict, query: str) -> Any:

    props = data["mainEntity"]["additionalProperty"]

    results = [p for p in props if p.get("name") == query]

    if len(results) != 1:
        raise Exception()  # f"No unique match found for {query} within {data}")

    return results[0]


def extract_sample(data: dict, sample_id: str) -> dict:
    sample_dict = {
        "@context": {"@vocab": "http://schema.org"},
        "@type": "Product",
        "additionalType": [
            "sample",
            "http://purl.obolibrary.org/obo/OBI_0000747",
        ],
        "@id": "",
        "identifier": [
            SRA(value=find_property_value(data, "SRA accession")["value"]).model_dump(by_alias=True, exclude_none=True),
            PropertyValue(
                name="BioSamples Accession",
                propertyID="https://registry.identifiers.org/registry/biosample",
                value=sample_id,
                url=f"https://www.ebi.ac.uk/ena/browser/view/{sample_id}?dataType=BIOSAMPLE"
            ).model_dump(by_alias=True, exclude_none=True),
            PropertyValue(
                name="sampling design label",
                propertyID="sampling design label",
                value=find_property_value(data, "sampling design label")["value"]
            ).model_dump(by_alias=True, exclude_none=True),
        ],
        "name": data["mainEntity"]["name"],
        "description": find_property_value(data, "sample description")["value"],
        "subjectOf": data["mainEntity"]["sameAs"],
        "url": data["mainEntity"]["url"],
        "productionDate": find_property_value(data, "collection date")["value"],
        "material": find_property_value(data, "environmental medium")["value"],
        "countryOfOrigin": find_property_value(data, "geographic location (country and/or sea)")["value"],
        "funding": "....B5D object",
        "manufacturer": {
            "@type": "ResearchProject",
            "name": find_property_value(data, "project name")["value"]
        },
        "keywords": [
            find_property_value(data, "organism")["value"],
            find_property_value(data, "target analysis type")["value"],
            find_property_value(data, "local environmental context")["value"]
        ],
        "additionalProperty": {
            "@type": "PropertyValue",
            "name": "checklist",
            "value": find_property_value(data, "checklist")["value"]
        }
    }
    return sample_dict


if __name__ == "__main__":
    sample_id = "SAMEA112489011"
    data = get_metadata(sample_id)
    extract_sample(data, sample_id)
