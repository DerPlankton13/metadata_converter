"""
Fetch JSON-LD metadata for all records in the BIOcean5D Zenodo community.

Usage:
    python fetch_biocean5d_jsonld.py [--output-dir OUTPUT_DIR] [--token ACCESS_TOKEN]

Output:
    One .jsonld file per record saved to the output directory,
    named by record ID (e.g. 12345678.jsonld).
"""

import argparse
import json
import time
from pathlib import Path

import requests

COMMUNITY_ID = "horizoneurope_biocean5d"
ZENODO_API = "https://zenodo.org/api"
RECORDS_SEARCH_URL = f"{ZENODO_API}/records"
JSONLD_EXPORT_URL = "https://zenodo.org/records/{record_id}/export/json-ld"

PAGE_SIZE = 25  # 25 is the maximum allowed without authentication; use authenticated requests to increase the limit to 100
REQUEST_DELAY = 0.5  # seconds between requests (be polite to the API)


def get_community_record_ids(token: str | None) -> list[str]:
    """Return all record IDs belonging to the BIOcean5D community."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {
        "communities": COMMUNITY_ID,
        "size": PAGE_SIZE,
        "page": 1,
        "sort": "newest",
    }

    # First request to get total and first page of results
    response = requests.get(
        RECORDS_SEARCH_URL, params=params, headers=headers, timeout=30
    )
    response.raise_for_status()
    data = response.json()

    total = data["hits"]["total"]
    print(f"Found {total} records in the BIOcean5D community.")

    record_ids = [str(hit["id"]) for hit in data["hits"]["hits"]]
    print(f"  Fetched page 1 ({len(record_ids)}/{total} records)")

    while len(record_ids) < total:
        params["page"] += 1
        time.sleep(REQUEST_DELAY)

        response = requests.get(
            RECORDS_SEARCH_URL, params=params, headers=headers, timeout=30
        )
        response.raise_for_status()
        data = response.json()

        record_ids.extend(str(hit["id"]) for hit in data["hits"]["hits"])
        print(f"  Fetched page {params['page']} ({len(record_ids)}/{total} records)")

    return record_ids


def fetch_jsonld(record_id: str, token: str | None) -> dict:
    """Fetch the JSON-LD export for a single record."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = JSONLD_EXPORT_URL.format(record_id=record_id)
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(
        description="Fetch BIOcean5D Zenodo records as JSON-LD"
    )
    parser.add_argument(
        "--output-dir",
        default="biocean5d_jsonld",
        help="Directory to save .jsonld files (default: biocean5d_jsonld/)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Zenodo personal access token (optional, but increases rate limits)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir.resolve()}\n")

    # Step 1: collect all record IDs
    record_ids = get_community_record_ids(args.token)

    # Step 2: fetch JSON-LD for each record
    success, failed = 0, []
    for i, record_id in enumerate(record_ids, start=1):
        out_path = output_dir / f"{record_id}.jsonld"

        if out_path.exists():
            print(f"[{i}/{len(record_ids)}] Skipping {record_id} (already downloaded)")
            success += 1
            continue

        try:
            jsonld = fetch_jsonld(record_id, args.token)
            out_path.write_text(
                json.dumps(jsonld, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            print(f"[{i}/{len(record_ids)}] Saved {record_id}.jsonld")
            success += 1
        except requests.HTTPError as e:
            print(f"[{i}/{len(record_ids)}] ERROR {record_id}: {e}")
            failed.append(record_id)
        except Exception as e:
            print(f"[{i}/{len(record_ids)}] UNEXPECTED ERROR {record_id}: {e}")
            failed.append(record_id)

        time.sleep(REQUEST_DELAY)

    # Summary
    print(f"\nDone. {success} records saved to '{output_dir}'.")
    if failed:
        print(f"Failed ({len(failed)}): {', '.join(failed)}")


if __name__ == "__main__":
    main()
