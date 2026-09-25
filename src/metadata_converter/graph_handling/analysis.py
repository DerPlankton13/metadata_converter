"""SPARQL query functions and table formatting for the BIOcean5D metadata graph.

Each analysis has a ``rows_*`` function that runs the SPARQL query and
returns raw rows as plain Python types. A single ``format_table`` helper
renders any of those row sets via ``tabulate``. The notebook computes each
set of rows once, then passes them to both ``format_table`` and
``write_stats_csv``, so every SPARQL query runs exactly once per analyse run.

The ``tablefmt`` parameter exists so the notebook can request ``"pipe"``
(Markdown) for Jupyter/Quarto rendering via ``IPython.display.Markdown``.

**Future direction:** if pandas is added as a dependency, replace the
``rows_*`` functions with ones that return DataFrames. DataFrames
auto-render as HTML tables in both Jupyter and Quarto with no wrapper
needed, making ``format_table``, ``tablefmt``, and the ``Markdown`` import
in the notebook redundant. See ``docs/setup_notes.md`` — *Notebook display*
section.
"""

import csv
from pathlib import Path

import rdflib
from rdflib import RDF, SDO, Graph
from rdflib.namespace import split_uri
from tabulate import tabulate

_INIT_NS = {"schema": str(SDO), "rdf": str(RDF)}


def strip_namespace(iri) -> str:
    """Strips the namespace and raises an error for malformed HTTP(S) namespaces.

    For non-HTTP URIs (CURIEs etc.) the full string is returned as-is.
    """
    if not str(iri).startswith(("http://", "https://")):
        # necessary for valid CURIE
        if ":" in str(iri):
            return str(iri)
        else:
            raise ValueError(
                f"The provided IRI is neither expanded nor a valid CURIE: {iri}"
            )

    ns, name = split_uri(iri)
    # a valid HTTP(S) namespace needs to end with # or /, but never // as in http(s)://
    if not (ns.endswith("#") or ns.endswith("/")) or ns.endswith("//"):
        raise ValueError(
            f"Namespace splitting failed for {iri}. Extracted {ns}, {name}."
        )
    return name


def load_graph(path: str) -> Graph:
    """Parse a turtle file into a rdflib Graph backed by the Oxigraph store."""
    g = Graph(store="Oxigraph")
    g.parse(path, format="ox-ttl")
    return g


# ---------------------------------------------------------------------------
# Row builders — run SPARQL, return raw Python data
# ---------------------------------------------------------------------------


def count_base_nodes(g: Graph) -> int:
    """Return the count of distinct non-blank subjects."""
    (row,) = g.query(
        """
        SELECT (COUNT(DISTINCT ?s) AS ?cnt)
        WHERE { ?s ?p ?o . FILTER(!isBlank(?s)) }
        """,
        initNs=_INIT_NS,
    )
    return int(row.cnt)


def rows_type_counts(g: Graph) -> list[tuple[str, int]]:
    """Return [(type_label, count), …] ordered by count descending."""
    res = g.query(
        """
        SELECT ?type (COUNT(*) AS ?cnt)
        WHERE { ?s rdf:type ?type . }
        GROUP BY ?type
        ORDER BY DESC(?cnt)
        """,
        initNs=_INIT_NS,
    )
    return [(strip_namespace(row.type), int(row.cnt)) for row in res]


def rows_base_nodes_type_counts(g: Graph) -> list[tuple[str, int]]:
    """Return [(type_label, count), …] ordered by count descending."""
    res = g.query(
        """
        SELECT ?type (COUNT(*) AS ?cnt)
        WHERE { ?s rdf:type ?type . FILTER(!isBlank(?s))}
        GROUP BY ?type
        ORDER BY DESC(?cnt)
        """,
        initNs=_INIT_NS,
    )
    return [(strip_namespace(row.type), int(row.cnt)) for row in res]


def rows_blank_nodes_type_counts(g: Graph) -> list[tuple[str, int]]:
    """Return [(type_label, count), …] ordered by count descending."""
    res = g.query(
        """
        SELECT ?type (COUNT(*) AS ?cnt)
        WHERE { ?s rdf:type ?type . FILTER(isBlank(?s))}
        GROUP BY ?type
        ORDER BY DESC(?cnt)
        """,
        initNs=_INIT_NS,
    )
    return [(strip_namespace(row.type), int(row.cnt)) for row in res]


def rows_empty_strings(g: Graph) -> list[tuple[str, int]]:
    """Return [(property_label, count), …] for empty-string objects, ordered by count."""
    res = g.query(
        """
        SELECT ?p (COUNT(*) AS ?cnt)
        WHERE { ?s ?p '' . }
        GROUP BY ?p
        ORDER BY DESC(?cnt)
        """,
        initNs=_INIT_NS,
    )
    return [(strip_namespace(row.p), int(row.cnt)) for row in res]


def rows_invalid_markers(g: Graph) -> list[tuple[str, str, str, int]]:
    """Return [(type, property, object, count), …] for empty or 'unknown' objects."""
    res = g.query(
        """
        SELECT ?type ?p ?o (COUNT(*) AS ?cnt)
        WHERE {
            ?s ?p ?o .
            ?s rdf:type ?type .
            FILTER( ?o = "" || REGEX(STR(?o), "unknown", "i") )
        }
        GROUP BY ?type ?p ?o
        ORDER BY DESC(?cnt)
        """,
        initNs=_INIT_NS,
    )
    return [
        (
            strip_namespace(row.type),
            strip_namespace(row.p),
            '""' if row.o == rdflib.term.Literal("") else str(row.o),
            int(row.cnt),
        )
        for row in res
    ]


# ---------------------------------------------------------------------------
# Table formatting
# ---------------------------------------------------------------------------


def format_table(rows, headers: list[str], tablefmt: str = "simple") -> str:
    """Format rows as a table with thousands-separated integers."""
    return tabulate(rows, headers=headers, tablefmt=tablefmt, intfmt=",")


# ---------------------------------------------------------------------------
# CSV export — takes precomputed rows, runs no SPARQL
# ---------------------------------------------------------------------------


def write_stats_csv(
    path: str | Path,
    total_triples: int,
    base_nodes: int,
    type_rows: list[tuple[str, int]],
    base_node_type_rows: list[tuple[str, int]],
    empty_string_rows: list[tuple[str, int]],
) -> None:
    """Write summary, type-count, and empty-string stats to a structured CSV."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = [
        {"category": "summary", "metric": "total_triples", "count": str(total_triples)},
        {"category": "summary", "metric": "base_nodes", "count": str(base_nodes)},
    ]
    for type_name, count in type_rows:
        rows.append(
            {"category": "type_counts", "metric": type_name, "count": str(count)}
        )
    for type_name, count in base_node_type_rows:
        rows.append(
            {
                "category": "base_nodes_type_counts",
                "metric": type_name,
                "count": str(count),
            }
        )
    for prop_name, count in empty_string_rows:
        rows.append(
            {"category": "empty_strings", "metric": prop_name, "count": str(count)}
        )

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["category", "metric", "count"])
        writer.writeheader()
        writer.writerows(rows)
