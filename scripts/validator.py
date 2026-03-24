"""
Bulk JSON-LD validator against the schema.org vocabulary using SHACL.

Validates one or more JSON-LD files against the official schema.org SHACL
shapes, which are downloaded once and cached locally. Results are printed
to stdout and optionally written to a JSON summary file.

Usage
-----
Download shapes and validate a directory::

    python validate_jsonld.py --input ./jsonld_files

Validate a single file, using a previously cached shapes file::

    python validate_jsonld.py --input ./file.jsonld --shapes schema.shacl.ttl

Validate and write a JSON report::

    python validate_jsonld.py --input ./jsonld_files --output results.json

Dependencies
------------
Install required packages::

    pip install pyshacl rdflib requests
"""

import argparse
import json
import os
import sys
from pathlib import Path

import requests
from pyshacl import validate
from rdflib import Graph

# Official schema.org vocabulary in Turtle format.
# This file acts as both the ontology and SHACL shapes source.
SCHEMA_ORG_URL = "https://schema.org/version/latest/schemaorg-current-https.ttl"
DEFAULT_SHAPES_CACHE = "schema.shacl.ttl"


# ---------------------------------------------------------------------------
# Shapes management
# ---------------------------------------------------------------------------


def download_shapes(url: str, destination: str) -> None:
    """
    Download the schema.org SHACL shapes file from the official URL.

    Parameters
    ----------
    url : str
        URL of the schema.org Turtle file to download.
    destination : str
        Local file path where the downloaded content will be saved.

    Raises
    ------
    requests.HTTPError
        If the HTTP request returns an unsuccessful status code.
    OSError
        If the file cannot be written to `destination`.
    """
    print(f"Downloading schema.org shapes from {url} ...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    with open(destination, "wb") as f:
        f.write(response.content)
    print(f"Shapes saved to '{destination}'.")


def load_shapes(shapes_path: str) -> Graph:
    """
    Parse the SHACL shapes Turtle file into an RDFLib graph.

    Downloads the shapes file first if it does not already exist locally.

    Parameters
    ----------
    shapes_path : str
        Path to the local Turtle file containing the schema.org shapes.
        If the file does not exist, it will be downloaded automatically.

    Returns
    -------
    rdflib.Graph
        An RDFLib graph populated with the parsed SHACL shapes.

    Raises
    ------
    rdflib.exceptions.ParserError
        If the Turtle file cannot be parsed.
    """
    if not os.path.exists(shapes_path):
        download_shapes(SCHEMA_ORG_URL, shapes_path)
    else:
        print(f"Using cached shapes file '{shapes_path}'.")

    shapes_graph = Graph()
    shapes_graph.parse(shapes_path, format="turtle")
    return shapes_graph


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_file(file_path: str, shapes_graph: Graph) -> dict:
    """
    Validate a single JSON-LD file against the schema.org SHACL shapes.

    Parameters
    ----------
    file_path : str
        Path to the JSON-LD file to validate.
    shapes_graph : rdflib.Graph
        Pre-loaded RDFLib graph containing the SHACL shapes. Passing this
        in avoids re-parsing the shapes for every file.

    Returns
    -------
    dict
        A result dictionary with the following keys:

        file : str
            The path of the validated file.
        valid : bool
            ``True`` if the file conforms to the schema.org shapes.
        error : str or None
            A human-readable error message if the file could not be parsed
            or validated at all (distinct from SHACL constraint violations);
            ``None`` otherwise.
        report : str or None
            The full SHACL validation report text when `valid` is ``False``;
            ``None`` when the file is valid or when a hard error occurred.
    """
    result = {"file": file_path, "valid": False, "error": None, "report": None}

    try:
        data_graph = Graph()
        data_graph.parse(file_path, format="json-ld")
    except Exception as exc:
        result["error"] = f"Failed to parse JSON-LD: {exc}"
        return result

    try:
        conforms, _, report_text = validate(
            data_graph,
            shacl_graph=shapes_graph,
            inference="rdfs",  # Apply RDFS inference before validation
            abort_on_first=False,  # Collect all violations, not just the first
        )
    except Exception as exc:
        result["error"] = f"SHACL validation engine error: {exc}"
        return result

    result["valid"] = conforms
    if not conforms:
        result["report"] = report_text

    return result


def validate_all(input_path: str, shapes_graph: Graph) -> list[dict]:
    """
    Validate one or more JSON-LD files and return all results.

    If `input_path` points to a directory, every file with a ``.jsonld``
    or ``.json`` extension inside that directory is validated (non-recursive).
    If it points to a single file, only that file is validated.

    Parameters
    ----------
    input_path : str
        Path to a JSON-LD file or a directory containing JSON-LD files.
    shapes_graph : rdflib.Graph
        Pre-loaded RDFLib graph containing the SHACL shapes.

    Returns
    -------
    list of dict
        A list of result dictionaries as returned by :func:`validate_file`,
        one entry per processed file.

    Raises
    ------
    FileNotFoundError
        If `input_path` does not exist.
    """
    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    if path.is_file():
        files = [path]
    else:
        # glob collects all matching files in one step; sorted() gives a
        # stable, alphabetical order so the [index/total] progress output
        # is predictable and easy to scan or compare across runs.
        files = sorted(path.glob("*.jsonld")) + sorted(path.glob("*.json"))

    if not files:
        print("No .jsonld or .json files found in the specified directory.")
        return []

    results = []
    total = len(files)

    for index, file_path in enumerate(files, start=1):
        result = validate_file(str(file_path), shapes_graph)
        results.append(result)

        status = "✅" if result["valid"] else ("⚠️ " if result["error"] else "❌")
        print(f"[{index}/{total}] {status} {file_path.name}")
        if result["error"]:
            print(f"         Error: {result['error']}")

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_summary(results: list[dict]) -> None:
    """
    Print a human-readable validation summary to stdout.

    Parameters
    ----------
    results : list of dict
        List of result dictionaries as returned by :func:`validate_all`.
    """
    total = len(results)
    valid = sum(1 for r in results if r["valid"])
    errors = sum(1 for r in results if r["error"])
    invalid = total - valid - errors

    print("\n" + "=" * 50)
    print("Validation Summary")
    print("=" * 50)
    print(f"  Total files processed : {total}")
    print(f"  ✅ Valid              : {valid}")
    print(f"  ❌ Invalid (SHACL)   : {invalid}")
    print(f"  ⚠️  Parse/engine errors: {errors}")
    print("=" * 50)

    failed = [r for r in results if not r["valid"]]
    if failed:
        print("\nFiles with issues:")
        for r in failed:
            print(f"\n  📄 {r['file']}")
            if r["error"]:
                print(f"     Error   : {r['error']}")
            if r["report"]:
                # Indent each line of the SHACL report for readability
                indented = "\n".join(
                    "     " + line for line in r["report"].splitlines()
                )
                print(f"     Report  :\n{indented}")


def write_json_report(results: list[dict], output_path: str) -> None:
    """
    Write the full validation results to a JSON file.

    Parameters
    ----------
    results : list of dict
        List of result dictionaries as returned by :func:`validate_all`.
    output_path : str
        Destination path for the JSON report file.

    Raises
    ------
    OSError
        If the file cannot be written to `output_path`.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nFull report written to '{output_path}'.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build the command-line argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured argument parser for this script.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Validate JSON-LD files against the schema.org vocabulary "
            "using SHACL shapes."
        )
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to a single JSON-LD file or a directory of JSON-LD files.",
    )
    parser.add_argument(
        "--shapes",
        "-s",
        default=DEFAULT_SHAPES_CACHE,
        help=(
            f"Path to the local schema.org SHACL Turtle file "
            f"(default: '{DEFAULT_SHAPES_CACHE}'). "
            "Downloaded automatically if not present."
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Optional path to write a JSON report of all results.",
    )
    return parser


def main() -> None:
    """
    Entry point for the bulk JSON-LD validator.

    Parses CLI arguments, loads the SHACL shapes, validates all specified
    files, prints a summary, and optionally writes a JSON report.
    """
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        shapes_graph = load_shapes(args.shapes)
    except Exception as exc:
        print(f"Fatal: could not load SHACL shapes — {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        results = validate_all(args.input, shapes_graph)
    except FileNotFoundError as exc:
        print(f"Fatal: {exc}", file=sys.stderr)
        sys.exit(1)

    print_summary(results)

    if args.output:
        write_json_report(results, args.output)


if __name__ == "__main__":
    main()
