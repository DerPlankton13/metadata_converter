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


def safe_extract(data: dict, query: str, default=None) -> Any:
    """
    Safely extract a property value from the data without raising exceptions.
    Returns the default value if the property is not found.
    """
    try:
        return find_property_value(data, query)["value"]
    except (Exception, KeyError, IndexError):
        return default


def extract_sample(data: dict, sample_id: str) -> dict:
    identifier_list = [
        PropertyValue(
            name="BioSamples Accession",
            propertyID="https://registry.identifiers.org/registry/biosample",
            value=sample_id,
            url=f"https://www.ebi.ac.uk/ena/browser/view/{sample_id}?dataType=BIOSAMPLE"
        ).model_dump(by_alias=True, exclude_none=True),
    ]

    sra_accession = safe_extract(data, "SRA accession")
    if sra_accession:
        identifier_list.insert(0,
            SRA(value=sra_accession).model_dump(by_alias=True, exclude_none=True)
        )

    sampling_design = safe_extract(data, "sampling design label")
    if sampling_design:
        identifier_list.append(
            PropertyValue(
                name="sampling design label",
                propertyID="sampling design label",
                value=sampling_design
            ).model_dump(by_alias=True, exclude_none=True)
        )

    sample_dict = {
        "@context": {"@vocab": "http://schema.org"},
        "@type": "Product",
        "additionalType": [
            "sample",
            "http://purl.obolibrary.org/obo/OBI_0000747",
        ],
        "@id": f"Product_{sample_id}.jsonld",
        "identifier": identifier_list,
    }

    # Add optional fields only if data exists
    if data["mainEntity"].get("name"):
        sample_dict["name"] = data["mainEntity"]["name"]

    description = safe_extract(data, "sample description")
    if description:
        sample_dict["description"] = description

    if data["mainEntity"].get("sameAs"):
        sample_dict["subjectOf"] = data["mainEntity"]["sameAs"]

    if data["mainEntity"].get("url"):
        sample_dict["url"] = data["mainEntity"]["url"]

    production_date = safe_extract(data, "collection date")
    if production_date:
        sample_dict["productionDate"] = production_date

    material = safe_extract(data, "environmental medium")
    if material:
        sample_dict["material"] = material

    country = safe_extract(data, "geographic location (country and/or sea)")
    if country:
        sample_dict["countryOfOrigin"] = country

    sample_dict["funding"] = "....B5D object"

    project_name = safe_extract(data, "project name")
    if project_name:
        sample_dict["manufacturer"] = {
            "@type": "ResearchProject",
            "name": project_name
        }

    keywords = [
        k for k in [
            safe_extract(data, "organism"),
            safe_extract(data, "target analysis type"),
            safe_extract(data, "local environmental context")
        ] if k is not None
    ]
    if keywords:
        sample_dict["keywords"] = keywords

    checklist = safe_extract(data, "checklist")
    if checklist:
        sample_dict["additionalProperty"] = {
            "@type": "PropertyValue",
            "name": "checklist",
            "value": checklist
        }

    return sample_dict


if __name__ == "__main__":
    sample_id = "SAMEA112489011"
    data = get_metadata(sample_id)
    extract_sample(data, sample_id)
