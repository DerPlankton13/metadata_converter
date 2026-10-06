import logging
from collections.abc import Iterable
from pathlib import Path

import networkx as nx
import pandas as pd
from rdflib import RDF, SDO, Graph, URIRef

from metadata_converter.utils.graph_helpers import (
    convert_result_to_pd,
    load_graph,
)
from metadata_converter.utils.provenance_writer import write_provenance_file

_INIT_NS = {"schema": str(SDO), "rdf": str(RDF)}

logger = logging.getLogger(__name__)


def merge_entities_by_identifier(
    graph_input_path: Path, graph_output_path: Path, provenance_dir: Path
) -> None:
    """Merge entities sharing the same identifier into golden nodes.

    This is the final deduplication step. All entities carrying an identifier are
    clustered by identifier and type, where the type prevents false positives such
    as a person and a book sharing an identifier. Each cluster is merged into a single
    golden node (see `merge_into_golden_node`), all links to the other nodes of the
    cluster are redirected to the golden node, and the other nodes are deleted.

    Parameters
    ----------
    graph_input_path : Path
        Turtle file of the graph to deduplicate.
    graph_output_path : Path
        Turtle file the deduplicated graph is written to.
    provenance_dir : Path
        Directory receiving one provenance file per golden node.
    """
    g = load_graph(graph_input_path)
    ids = get_identifiers(g)

    clusters = build_clusters(ids, g)
    # log some statistics
    node_count = sum(len(nodes) for nodes in clusters)
    logger.info(
        "Nodes sharing an identifier and type with at least one other: %d",
        node_count,
    )
    logger.info("Number of clusters: %d", len(clusters))
    logger.info(
        "Number of duplicates to remove from the graph: %d", node_count - len(clusters)
    )

    for cluster in clusters:
        merge_into_golden_node(cluster, provenance_dir, g)

    g.serialize(graph_output_path, format="ox-ttl")


def get_identifiers(g: Graph) -> pd.DataFrame:
    """Extract all nodes carrying an identifier.

    The node IRI together with its predicates and objects as well as the identifier is
    returned. The identifier value is either extracted from the top level identifier
    object or, if the identifier is of type PropertyValue from the value property of
    the PropertyValue.

    Parameters
    ----------
    g : Graph
        Graph to query.

    Returns
    -------
    pd.DataFrame
        One row per identifier with columns ``s`` (the node), ``p``
        (``schema:identifier``), ``o`` (the raw identifier object) and ``identifier``
        (the value of ``o``, or of its ``schema:value`` if ``o`` is a PropertyValue).
    """
    identifiers = g.query(
        """
            SELECT ?s ?p ?o ?identifier
            WHERE { BIND(schema:identifier AS ?p) .                                                                                             ?s ?p ?o .
                    OPTIONAL { ?o a schema:PropertyValue. ?o schema:value ?value }
                    BIND(COALESCE(?value, ?o) AS ?identifier)
            }
            """,
        initNs=_INIT_NS,
    )
    return convert_result_to_pd(identifiers)


def build_clusters(ids: pd.DataFrame, g: Graph) -> list[set[URIRef]]:
    """Cluster nodes connected through shared identifiers within the same type.

    Nodes are clustered by identifier and type; a node may carry several types, so
    its full set of types is compared. If nodes contain more than one identifier, there
    can be a transitive link between clusters. These links are resolved by combining
    clusters sharing a node. Identifiers occurring on nodes of different types are
    logged as a warning.

    Parameters
    ----------
    ids : pd.DataFrame
        Identifier table as returned by `get_identifiers`.
    g : Graph
        Graph the nodes' types are read from.

    Returns
    -------
    list[set[URIRef]]
        Clusters of at least two nodes each; no node occurs in more than one cluster.
    """

    # remove any possible duplicate identifier entries
    ids = ids.drop_duplicates(["s", "identifier"])
    # add the types to the ids
    ids = ids.assign(types=ids.s.map(lambda node: get_types(node, g)))

    # log, if any identifier belongs to more than one type
    types_per_identifier = ids.groupby("identifier")["types"].agg(set)
    mixed = types_per_identifier[types_per_identifier.map(len) > 1]
    if not mixed.empty:
        logger.warning(
            "Identifiers occurring on nodes of different types: %s", mixed.to_dict()
        )

    # cluster by identifier and type ensuring that nodes with the same identifier
    # but different type are not merged.
    clusters = ids.groupby(["identifier", "types"])["s"].agg(list)
    # transient relations between clusters can occur if nodes have more than one
    # identifier: [a, b], [c, d, e, a]
    # resolve them via connected components here
    graph = nx.Graph()
    for cluster in clusters:
        nx.add_path(graph, cluster)
    return [cluster for cluster in nx.connected_components(graph) if len(cluster) > 1]


def get_types(node: URIRef, g: Graph) -> frozenset[URIRef]:
    """Return all types of a node."""
    return frozenset(g.objects(node, RDF.type))


def merge_into_golden_node(
    node_cluster: Iterable[URIRef], provenance_dir: Path, g: Graph
) -> None:
    """Merge all nodes of a cluster into a single golden node, modifying `g` in place.

    The richest node (most triples, ties broken by IRI) becomes the golden node. The
    other nodes donate, in the same order (most triples, ties broken by IRI), every
    property the golden node does not yet have, with all of its values.
    References to the donors are redirected to the golden node, the donors are deleted,
    and a provenance file is written.

    Parameters
    ----------
    node_cluster : Iterable[URIRef]
        Nodes to merge; duplicates are ignored.
    provenance_dir : Path
        Directory the provenance file of the golden node is written to.
    g : Graph
        Graph containing the nodes.
    """
    # dedup, or a repeated golden node would be removed as its own donor
    ordered_nodes = sorted(
        set(node_cluster), key=lambda n: (-calculate_node_richness(n, g), str(n))
    )
    golden_node, *donors = ordered_nodes
    golden_props = set(g.predicates(golden_node))
    for donor in donors:
        donor_props = list(g.predicate_objects(donor))
        for p, o in donor_props:
            if p not in golden_props:
                g.add((golden_node, p, o))
        # we update after adding to ensure that if multiple objects for a property
        # exist all are donated; the set immediately removes any duplicates
        golden_props |= {p for p, _ in donor_props}

    for old_node in donors:
        for s, p in list(g.subject_predicates(old_node)):
            g.remove((s, p, old_node))
            g.add((s, p, golden_node))
        g.remove((old_node, None, None))

    write_provenance_file(
        str(golden_node),
        provenance_dir,
        [str(golden_node), *[str(donor) for donor in donors]],
    )


def calculate_node_richness(node: URIRef, g: Graph) -> int:
    """Count the triples with the node as subject."""
    return len(list(g.predicate_objects(node)))
