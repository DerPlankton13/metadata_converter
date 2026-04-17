from typing import Any

import requests

from metadata_converter.schema_org_models.custom_models import SRA


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
    sample_dict = {}
    sample_dict["type"] = "Product"
    sample_dict["additionalType"] = [
        "sample",
        "http://purl.obolibrary.org/obo/OBI_0000747",
    ]
    sample_dict["id"] = f"Product_{sample_id}.jsonld"
    value = find_property_value(data, "SRA accession")["value"]
    print(value)
    sample_dict["identifier"] = [
        SRA(value=value).model_dump(by_alias=True, exclude_none=True)
    ]
    sample_dict["identifier"].append(data["@id"].split(":")[1])
    sample_dict["identifier"].append(find_property_value(data, "sampling design label"))

    print(sample_dict)


if __name__ == "__main__":
    sample_id = "SAMEA112489011"
    data = get_metadata(sample_id)
    extract_sample(data, sample_id)
