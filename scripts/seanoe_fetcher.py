"""
Fetch JSON-LD metadata for BIOcean5D-related records from SEANOE.

SEANOE has no reliable programmatic way to filter by project, so records are
hardcoded below. The JSON-LD is embedded in each record's HTML page as a
<script type="application/ld+json"> block (schema.org format).

To add new records, append (record_id, url) tuples to BIOCEAN5D_RECORDS.

Usage:
    python fetch_seanoe_biocean5d_jsonld.py [--output-dir OUTPUT_DIR]

Requirements:
    pip install requests beautifulsoup4

Output:
    One .jsonld file per record saved to the output directory,
    named by SEANOE record ID (e.g. 110692.jsonld).
"""

import argparse
import json
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

REQUEST_DELAY = 1.0  # seconds between requests

# Hardcoded BIOcean5D-affiliated records on SEANOE.
# Update this list as new records are identified.
BIOCEAN5D_RECORDS = [
    ("110692", "https://www.seanoe.org/data/00995/110692/"),
    ("102697", "https://www.seanoe.org/data/00915/102697/"),
    ("102694", "https://www.seanoe.org/data/00915/102694/"),
    ("102336", "https://www.seanoe.org/data/00911/102336/"),
    ("102537", "https://www.seanoe.org/data/00913/102537/"),
]


def extract_jsonld(html: str) -> dict | None:
    """
    Extract the first application/ld+json block from an HTML page.
    Returns the parsed dict, or None if not found.
    """
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"type": "application/ld+json"})
    if script_tag and script_tag.string:
        return json.loads(script_tag.string)
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Fetch BIOcean5D SEANOE records as JSON-LD"
    )
    parser.add_argument(
        "--output-dir",
        default="seanoe_biocean5d_jsonld",
        help="Directory to save .jsonld files (default: seanoe_biocean5d_jsonld/)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir.resolve()}")
    print(f"Processing {len(BIOCEAN5D_RECORDS)} hardcoded record(s).\n")

    session = requests.Session()
    session.headers.update({"User-Agent": "BIOcean5D-metadata-harvester/1.0"})

    success, failed = 0, []
    for i, (record_id, url) in enumerate(BIOCEAN5D_RECORDS, start=1):
        out_path = output_dir / f"{record_id}.jsonld"

        if out_path.exists():
            print(
                f"[{i}/{len(BIOCEAN5D_RECORDS)}] Skipping {record_id} (already downloaded)"
            )
            success += 1
            continue

        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()

            jsonld = extract_jsonld(response.text)
            if jsonld is None:
                print(
                    f"[{i}/{len(BIOCEAN5D_RECORDS)}] WARNING: no JSON-LD found in {url}"
                )
                failed.append(record_id)
            else:
                out_path.write_text(
                    json.dumps(jsonld, indent=2, ensure_ascii=False), encoding="utf-8"
                )
                title = jsonld.get("name", "(no title)")
                print(
                    f"[{i}/{len(BIOCEAN5D_RECORDS)}] Saved {record_id}.jsonld — {title}"
                )
                success += 1

        except requests.HTTPError as e:
            print(f"[{i}/{len(BIOCEAN5D_RECORDS)}] HTTP ERROR {record_id}: {e}")
            failed.append(record_id)
        except Exception as e:
            print(f"[{i}/{len(BIOCEAN5D_RECORDS)}] UNEXPECTED ERROR {record_id}: {e}")
            failed.append(record_id)

        time.sleep(REQUEST_DELAY)

    print(f"\nDone. {success} record(s) saved to '{output_dir}'.")
    if failed:
        print(f"Failed ({len(failed)}): {', '.join(failed)}")


if __name__ == "__main__":
    main()
