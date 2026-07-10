import copy
import os
import time
from pathlib import Path

import requests

from metadata_converter.utils.io import write_json
from metadata_converter.utils.jsonld import expand_curie
from metadata_converter.utils.provenance_writer import write_provenance_file


def fetch_metadata(url: str, session: requests.Session) -> dict:
    for attempt in range(4):
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == 3:
                raise
            time.sleep(2**attempt)


def fuse_metadata(structured_metadata: dict, unstructured_metadata: dict) -> dict:
    fused = copy.deepcopy(structured_metadata)
    characteristics = unstructured_metadata.get("characteristics", {})

    for prop in fused["mainEntity"]["additionalProperty"]:
        name = prop["name"]
        if name in characteristics and "unit" in characteristics[name][0]:
            if len(characteristics[name]) > 1:
                raise ValueError(
                    f"Could not fuse the data, several entries were found for {name} in the unstructured metadata."
                )
            prop["unitText"] = characteristics[name][0]["unit"]

    return fused


def sample_source_urls(sample_id: str) -> list[str]:
    """The two source URLs a sample is fused from: structured (.ldjson) and unstructured (.json)."""
    base = f"https://www.ebi.ac.uk/biosamples/samples/{sample_id}"
    return [f"{base}.ldjson", f"{base}.json"]


def get_metadata(
    sample_id: str,
    session: requests.Session,
    fetched_path: Path,
    provenance_dir: Path | None = None,
) -> None:
    ldjson_url, json_url = sample_source_urls(sample_id)
    structured = fetch_metadata(ldjson_url, session)
    unstructured = fetch_metadata(json_url, session)
    write_json(structured, fetched_path / f"{sample_id}.ldjson")
    write_json(unstructured, fetched_path / f"{sample_id}.json")
    if provenance_dir is not None:
        write_provenance_file(
            os.path.relpath(fetched_path / f"{sample_id}.json"),
            provenance_dir,
            json_url,
            "fetch",
        )
        # we need to expand the @id as we are not keeping the context in the
        # provenance file and the CURIE becomes unresolvable otherwise
        structured_id = expand_curie(structured["@id"], structured["@context"])
        write_provenance_file(structured_id, provenance_dir, ldjson_url, "fetch")
