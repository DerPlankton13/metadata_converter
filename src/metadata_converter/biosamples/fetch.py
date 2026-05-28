import time

import requests


def fetch_metadata(url: str, retries: int = 2) -> dict:
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(attempt + 1)


def fuse_metadata(structured_metadata: dict, unstructured_metadata: dict) -> dict:
    characteristics = unstructured_metadata.get("characteristics", {})

    for prop in structured_metadata["mainEntity"]["additionalProperty"]:
        name = prop["name"]
        if name in characteristics and "unit" in characteristics[name][0]:
            if len(characteristics[name]) > 1:
                raise RuntimeError(
                    "Could not fuse the data, several entries were found for {name} in the unstructured metadata."
                )
            prop["unitText"] = characteristics[name][0]["unit"]

    return structured_metadata


def get_metadata(sample_id: str) -> dict:
    base_url = f"https://www.ebi.ac.uk/biosamples/samples/{sample_id}"
    structured_metadata = fetch_metadata(base_url + ".ldjson")
    unstructured_metadata = fetch_metadata(base_url + ".json")
    return fuse_metadata(structured_metadata, unstructured_metadata)
