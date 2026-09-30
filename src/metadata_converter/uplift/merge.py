import logging
from collections import defaultdict
from pathlib import Path

import networkx as nx
import pandas as pd
from rdflib import RDF, SDO, Graph, Literal, URIRef

from metadata_converter.graph_handling.helpers import (
    convert_result_to_pd,
    load_graph,
)
from metadata_converter.utils.provenance_writer import write_provenance_file

_INIT_NS = {"schema": str(SDO), "rdf": str(RDF)}

logger = logging.getLogger(__name__)


def merge_entities_by_identifier(
    graph_path: Path, output_path: Path, provenance_dir: Path
) -> None:
    """Merges entities sharing the same identifier.

    This is the final deduplication step. The graph is loaded, all entities that have
    an identifier are extracted together with the identifier. Then the nodes are
    clustered by identifier and node type. The type is taken into account to prevent
    false positives, i.e. that the identifier could denote a person and a book.

    The nodes of these clusters are merged into a single, golden node and all links to
    the other nodes of the cluster are replaced by links to the golden node. Afterwards,
    the old nodes are deleted.
    The merging into the golden node is done as follows:
    1.

    Finally, the updated graph is serialised as turtle into the output_path.

    """
    g = load_graph(graph_path)
    ids = get_identifiers(g)

    clusters = build_clusters(ids, g)
    # log some statistics
    node_count = sum(len(nodes) for nodes in clusters.values())
    logger.info(
        "Nodes sharing an identifier and type set with at least one other: %d",
        node_count,
    )
    logger.info("Number of clusters: %d", len(clusters))
    logger.info(
        "Number of duplicates to remove from the graph: %d", node_count - len(clusters)
    )

    for cluster in clusters.values():
        merge_into_golden_node(cluster, provenance_dir, g)

    g.serialize(output_path, format="ox-ttl")


def get_identifiers(g: Graph):
    """Extracts all nodes with an identifier.

    The node IRI together with its predicates and objects as well as the identifier is
    returned. The identifier value is either extracted from the top level identifier
    object or, if the identifier is of type PropertyValue from the value property of
    the PropertyValue.

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


def build_clusters(ids: pd.DataFrame, g: Graph) -> dict:
    """Clusters all nodes sharing the same identifier and being of the same type.

    Logs if an identifier occurs on nodes of different types and takes into account
    that a node could be of more than one type, by using a set of unique types as type.
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

    # cluster by type and identifier and collect all nodes as list
    clusters = ids.groupby(["identifier", "types"])["s"].agg(list).to_dict()
    return {key: nodes for key, nodes in clusters.items() if len(nodes) > 1}


def get_types(node: URIRef, g: Graph) -> frozenset[str]:
    """Returns the types of a node, can be multiple."""
    return frozenset(g.objects(node, RDF.type))


def merge_into_golden_node(node_cluster: list[URIRef], provenance_dir: Path, g: Graph):
    """Merge all nodes of a cluster into a single golden node."""
    ordered_nodes = sorted(
        node_cluster, key=lambda n: (-calculate_node_richness(n, g), str(n))
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


def calculate_node_richness(node, g: Graph) -> int:
    """Count the number of properties of a node."""
    return len(list(g.predicate_objects(node)))
